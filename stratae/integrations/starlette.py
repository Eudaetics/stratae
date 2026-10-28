"""
Starlette integration for lifecycle management.

{py:func}`scoped_route` builds a `Route` subclass that activates an
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` around each request. Use it in
place of `Route`.

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
from stratae.lifecycle import AsyncScope, async_resource

application = AsyncScope("application", isolation="shared")
request = AsyncScope("request", requires=application)
transaction = AsyncScope("transaction", requires=request)

@application.cache()
@async_resource
async def get_connection():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE products (id TEXT PRIMARY KEY, name TEXT)")
    conn.commit()
    try:
        yield conn
    finally:
        conn.close()

@request.cache()
@async_resource
async def get_cursor():
    cur = (await get_connection()).cursor()
    try:
        yield cur
    finally:
        cur.close()

@transaction.cache()
@async_resource
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
from stratae.lifecycle import AsyncScope, async_resource

application = AsyncScope("application", isolation="shared")
connection = AsyncScope("connection", requires=application)
message = AsyncScope("message", requires=connection)

@application.cache()
@async_resource
async def get_database():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE notes (body TEXT)")
    try:
        yield conn
    finally:
        conn.close()

@connection.cache()
@async_resource
async def get_cursor():
    cur = (await get_database()).cursor()
    try:
        yield cur
    finally:
        cur.close()

@message.cache()
@async_resource
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

from functools import partial, wraps
from inspect import iscoroutinefunction, isfunction, ismethod
from typing import Any, Awaitable, Callable, cast

from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route, WebSocketRoute
from starlette.types import Receive, Send
from starlette.types import Scope as ASGIScope
from starlette.websockets import WebSocket

from stratae.lifecycle import AsyncScope
from stratae.lifecycle.scope import AsyncActivation

_ACTIVATION_KEY = "stratae.activation"
"""ASGI scope key holding a request's or connection's open activation, until whichever side
closes it."""


async def _close_activation(asgi_scope: ASGIScope, exc: BaseException) -> None:
    """Close the request's or connection's activation with `exc`, unless already closed."""
    activation: AsyncActivation | None = asgi_scope.pop(_ACTIVATION_KEY, None)
    if activation is None:
        return
    await activation.scope.deactivate(activation, exc)


def _unwrap_partial(endpoint: Callable[..., Any]) -> Callable[..., Any]:
    """Return the callable behind `endpoint`, unwrapping `functools.partial`."""
    target = endpoint
    while isinstance(target, partial):
        target = target.func
    return target


def _endpoint_function(endpoint: Callable[..., Any]) -> Callable[..., Any]:
    """
    Return the function or method behind `endpoint`, unwrapping `functools.partial`.

    :raises TypeError: If it's an ASGI app or `HTTPEndpoint` class, which scoped routes
        don't support.
    """
    target = _unwrap_partial(endpoint)
    if not (isfunction(target) or ismethod(target)):
        raise TypeError(
            f"scoped_route needs a function or method endpoint, got {endpoint!r}. "
            "Class-based endpoints such as HTTPEndpoint aren't supported."
        )
    return target


def _as_async(
    endpoint: Callable[[Request], Awaitable[Response] | Response],
) -> Callable[[Request], Awaitable[Response]]:
    """
    Return `endpoint` if async, otherwise a coroutine running it in Starlette's threadpool.

    The threadpool copies the context, so scopes stay visible to the endpoint.
    """
    if iscoroutinefunction(_endpoint_function(endpoint)):
        return cast(Callable[[Request], Awaitable[Response]], endpoint)

    async def run_sync(request: Request) -> Response:
        return await run_in_threadpool(cast(Callable[[Request], Response], endpoint), request)

    return run_sync


def scoped_route(scope: AsyncScope, *, handler: AsyncScope | None = None) -> type[Route]:
    """
    Build a Route subclass that activates `scope` around every request.

    :param scope: Active until the response is fully sent, including streaming and
        background tasks.
    :param handler: An optional nested scope, active only while the endpoint runs. Use it
        for work like commits that must finish before the response.
    :returns: A Route subclass. Its endpoints must be functions or methods; others raise
        `TypeError`.
    """
    lifecycle_scope = scope
    handler_scope = handler

    class ScopedRoute(Route):
        def __init__(
            self,
            path: str,
            endpoint: Callable[[Request], Awaitable[Response] | Response],
            **kwargs: Any,
        ) -> None:
            call = _as_async(endpoint)

            @wraps(endpoint)
            async def scoped_endpoint(request: Request) -> Response:
                if handler_scope is not None:
                    async with handler_scope:
                        return await call(request)
                try:
                    return await call(request)
                except BaseException as exc:
                    await _close_activation(request.scope, exc)
                    raise

            super().__init__(path, scoped_endpoint, **kwargs)

        async def handle(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
            scope[_ACTIVATION_KEY] = lifecycle_scope.activate()
            try:
                await super().handle(scope, receive, send)
            except BaseException as exc:
                await _close_activation(scope, exc)
                raise
            activation: AsyncActivation | None = scope.pop(_ACTIVATION_KEY, None)
            if activation is not None:
                await lifecycle_scope.deactivate(activation)

    return ScopedRoute


def _scoped_websocket_endpoint(
    endpoint: Callable[[WebSocket], Awaitable[None]],
) -> Callable[[WebSocket], Awaitable[None]]:
    """
    Wrap a function endpoint so the exception it raises closes the connection's activation.

    Starlette handles exceptions like `WebSocketException` inside the route, so without this
    the activation would close as though the connection had ended cleanly.
    """

    @wraps(endpoint)
    async def scoped_endpoint(websocket: WebSocket) -> None:
        try:
            await endpoint(websocket)
        except BaseException as exc:
            await _close_activation(websocket.scope, exc)
            raise

    return scoped_endpoint


def scoped_websocket_route(scope: AsyncScope) -> type[WebSocketRoute]:
    """
    Build a WebSocketRoute subclass that activates `scope` for every connection.

    :param scope: Active for the connection's lifetime. It closes with the exception the
        connection ended with, if any, including one Starlette handles, such as
        `WebSocketException`.
    :returns: A WebSocketRoute subclass. Its endpoints may be async functions or methods, or
        classes such as `WebSocketEndpoint`; sync functions raise `TypeError`.
    """
    lifecycle_scope = scope

    class ScopedWebSocketRoute(WebSocketRoute):
        def __init__(self, path: str, endpoint: Callable[..., Any], **kwargs: Any) -> None:
            target = _unwrap_partial(endpoint)
            if isfunction(target) or ismethod(target):
                if not iscoroutinefunction(target):
                    raise TypeError(
                        f"scoped_websocket_route needs an async endpoint, got {endpoint!r}."
                    )
                endpoint = _scoped_websocket_endpoint(endpoint)
            super().__init__(path, endpoint, **kwargs)

        async def handle(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
            scope[_ACTIVATION_KEY] = lifecycle_scope.activate()
            try:
                await super().handle(scope, receive, send)
            except BaseException as exc:
                await _close_activation(scope, exc)
                raise
            activation: AsyncActivation | None = scope.pop(_ACTIVATION_KEY, None)
            if activation is not None:
                await lifecycle_scope.deactivate(activation)

    return ScopedWebSocketRoute
