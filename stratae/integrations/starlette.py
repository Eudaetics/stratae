"""
Starlette integration for lifecycle management.

{py:func}`scoped_route` builds a `Route` subclass that activates an
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` around each request. Use it in
place of `Route`.

The request scope closes once the response is fully sent, on success and on error alike. It
closes with the error the endpoint raised, including one Starlette turns into a response, such
as `HTTPException`. So its cleanup runs after the response, and exception handlers can use it.

Work that must finish before the response, like committing a transaction, belongs in a
nested `handler` scope. It ends when the endpoint returns, so a failed commit becomes a 500.

````{example} Committing a per-request transaction before the response
```{code-block} python
import sqlite3
from contextlib import asynccontextmanager
from typing import Annotated
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.testclient import TestClient
from stratae.depends import Depends, inject
from stratae.integrations.starlette import scoped_route
from stratae.lifecycle import AsyncScope

application = AsyncScope("application", isolation="shared")
request = AsyncScope("request", requires=application)
transaction = AsyncScope("transaction", requires=request)

@application.cache()
async def get_connection():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE products (id TEXT PRIMARY KEY, name TEXT)")
    conn.commit()
    try:
        yield conn
    finally:
        conn.close()

@request.cache()
async def get_cursor():
    cur = (await get_connection()).cursor()
    try:
        yield cur
    finally:
        cur.close()

@transaction.cache()
async def get_transaction():
    conn = await get_connection()
    try:
        yield await get_cursor()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise

type CursorDep = Annotated[sqlite3.Cursor, Depends(get_transaction)]

# inject hides cur from the route's signature.
@inject
async def create_product(request: Request, cur: CursorDep) -> JSONResponse:
    product_id = request.path_params["product_id"]
    cur.execute(
        "INSERT INTO products VALUES (?, ?)",
        (product_id, request.query_params["name"]),
    )
    return JSONResponse({"status": "created"})

@inject
async def read_product(request: Request, cur: CursorDep) -> JSONResponse:
    product_id = request.path_params["product_id"]
    row = cur.execute(
        "SELECT name FROM products WHERE id = ?", (product_id,)
    ).fetchone()
    return JSONResponse({"name": row[0]})

@asynccontextmanager
async def lifespan(app: Starlette):
    async with application:
        yield

ScopedRoute = scoped_route(request, handler=transaction)
app = Starlette(
    lifespan=lifespan,
    routes=[
        ScopedRoute("/products/{product_id}", create_product, methods=["POST"]),
        ScopedRoute("/products/{product_id}", read_product, methods=["GET"]),
    ],
)

with TestClient(app) as client:
    client.post("/products/keyboard-1", params={"name": "Mechanical Keyboard"})
    response = client.get("/products/keyboard-1")
    print(response.json())
```
```{output}
{'name': 'Mechanical Keyboard'}
```
````

{py:func}`scoped_websocket_route` builds a `WebSocketRoute` subclass that activates a scope
for each connection's lifetime. The scope closes with the exception the connection ended
with, so cleanup sees a client leaving (`WebSocketDisconnect`) or a deliberate close
(`WebSocketException`) as it happened. Keep connection-lifetime resources in it, not
transactions: give each message's work its own nested scope inside the receive loop, so a
failed message rolls back without ending the connection.

````{example} Committing a transaction per WebSocket message
```{code-block} python
import sqlite3
from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.testclient import TestClient
from starlette.websockets import WebSocket, WebSocketDisconnect
from stratae.integrations.starlette import scoped_websocket_route
from stratae.lifecycle import AsyncScope

application = AsyncScope("application", isolation="shared")
connection = AsyncScope("connection", requires=application)
message = AsyncScope("message", requires=connection)

@application.cache()
async def get_database():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE notes (body TEXT)")
    try:
        yield conn
    finally:
        conn.close()

@connection.cache()
async def get_cursor():
    cur = (await get_database()).cursor()
    try:
        yield cur
    finally:
        cur.close()

@message.cache()
async def get_transaction():
    conn = await get_database()
    try:
        yield await get_cursor()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise

async def notes(websocket: WebSocket) -> None:
    await websocket.accept()
    while True:
        try:
            body = await websocket.receive_text()
        except WebSocketDisconnect:
            return
        try:
            async with message:
                cur = await get_transaction()
                cur.execute("INSERT INTO notes VALUES (?)", (body,))
                if not body.strip():
                    raise ValueError("empty note")
        except ValueError as exc:
            await websocket.send_json({"error": str(exc)})
            continue
        total = (await get_cursor()).execute("SELECT COUNT(*) FROM notes").fetchone()[0]
        await websocket.send_json({"saved": body, "total": total})

@asynccontextmanager
async def lifespan(app: Starlette):
    async with application:
        yield

ScopedWebSocketRoute = scoped_websocket_route(connection)
app = Starlette(lifespan=lifespan, routes=[ScopedWebSocketRoute("/notes", notes)])

with TestClient(app) as client, client.websocket_connect("/notes") as websocket:
    for body in ["milk", " ", "eggs"]:
        websocket.send_text(body)
        print(websocket.receive_json())
```
```{output}
{'saved': 'milk', 'total': 1}
{'error': 'empty note'}
{'saved': 'eggs', 'total': 2}
```
````
"""

from collections.abc import Awaitable, Callable
from functools import wraps
from inspect import isfunction, ismethod
from typing import Any, cast

from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route, WebSocketRoute

from stratae.callables import is_async_callable, unwrap_partial
from stratae.integrations._activation import ActivatingRoute, recording_errors, within
from stratae.lifecycle import AsyncScope


class UnsupportedEndpointError(TypeError):
    """Raised when a scoped route is given an endpoint it can't scope."""


class ClassEndpointError(UnsupportedEndpointError):
    """Raised when {py:func}`scoped_route` is given a class-based endpoint or an ASGI app."""

    def __init__(self, endpoint: object) -> None:
        """:param endpoint: The endpoint."""
        super().__init__(
            f"scoped_route needs a function or method endpoint, got {endpoint!r}. "
            "Class-based endpoints such as HTTPEndpoint aren't supported."
        )


class SyncWebSocketEndpointError(UnsupportedEndpointError):
    """Raised when {py:func}`scoped_websocket_route` is given a sync function endpoint."""

    def __init__(self, endpoint: object) -> None:
        """:param endpoint: The endpoint."""
        super().__init__(f"scoped_websocket_route needs an async endpoint, got {endpoint!r}.")


def _refuse_class_endpoint(endpoint: Callable[..., Any]) -> None:
    """
    Refuse an endpoint that isn't a function or method, or a `functools.partial` of one.

    :raises ClassEndpointError: If it's an ASGI app or `HTTPEndpoint` class, which scoped
        routes don't support.
    """
    target = unwrap_partial(endpoint)
    if not (isfunction(target) or ismethod(target)):
        raise ClassEndpointError(endpoint)


def _as_async(
    endpoint: Callable[[Request], Awaitable[Response] | Response],
) -> Callable[[Request], Awaitable[Response]]:
    """
    Return `endpoint` if async, otherwise a coroutine running it in Starlette's threadpool.

    The threadpool copies the context, so scopes stay visible to the endpoint.
    """
    _refuse_class_endpoint(endpoint)
    if is_async_callable(endpoint):
        return cast(Callable[[Request], Awaitable[Response]], endpoint)

    async def run_sync(request: Request) -> Response:
        return await run_in_threadpool(cast(Callable[[Request], Response], endpoint), request)

    return run_sync


def scoped_route(scope: AsyncScope, *, handler: AsyncScope | None = None) -> type[Route]:
    """
    Build a Route subclass that activates `scope` around every request.

    :param scope: Active until the response is fully sent, including streaming and
        background tasks. It closes with the error the endpoint raised, if any, including one
        Starlette turns into a response, such as `HTTPException`.
    :param handler: An optional nested scope, active only while the endpoint runs. Use it
        for work like commits that must finish before the response.
    :returns: A Route subclass. Its endpoints must be functions or methods; others raise
        `TypeError`.
    """
    handler_scope = handler

    class ScopedRoute(ActivatingRoute, Route):
        activates = scope

        def __init__(
            self,
            path: str,
            endpoint: Callable[[Request], Awaitable[Response] | Response],
            **kwargs: Any,
        ) -> None:
            call = _as_async(endpoint)
            if handler_scope is not None:
                call = within(handler_scope, call)
            super().__init__(path, wraps(endpoint)(recording_errors(call)), **kwargs)

    return ScopedRoute


def scoped_websocket_route(scope: AsyncScope) -> type[WebSocketRoute]:
    """
    Build a WebSocketRoute subclass that activates `scope` for every connection.

    :param scope: Active for the connection's lifetime. It closes with the exception the
        connection ended with, if any, including one Starlette handles, such as
        `WebSocketException`.
    :returns: A WebSocketRoute subclass. Its endpoints may be async functions or methods, or
        classes such as `WebSocketEndpoint`; sync functions raise
        {py:exc}`SyncWebSocketEndpointError`.
    """

    class ScopedWebSocketRoute(ActivatingRoute, WebSocketRoute):
        activates = scope

        def __init__(self, path: str, endpoint: Callable[..., Any], **kwargs: Any) -> None:
            target = unwrap_partial(endpoint)
            if isfunction(target) or ismethod(target):
                if not is_async_callable(target):
                    raise SyncWebSocketEndpointError(endpoint)
                endpoint = wraps(endpoint)(recording_errors(endpoint))
            super().__init__(path, endpoint, **kwargs)

    return ScopedWebSocketRoute
