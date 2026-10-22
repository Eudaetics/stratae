"""
Starlette integration for lifecycle management.

{py:func}`scoped_route` builds a `Route` subclass that activates an
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` around every
request it handles. Pass it in place of `Route` when constructing routes. A
cached resource then opens once per request and closes when the request
finishes, even if a handler calls it more than once.

Work that must finish before the response starts, such as committing a
transaction, belongs in a second scope nested inside the request scope and
passed as `handler`. It's active only while the endpoint runs, so a commit
happens - or fails, becoming a 500 - before any response is sent, while the
request scope's resources stay open for a streamed body and background tasks.

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

# inject strips cur from the wrapper's exposed signature, so the wrapped
# endpoint still matches Starlette's request-only calling convention.
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
"""

from functools import partial, wraps
from inspect import iscoroutinefunction, isfunction, ismethod
from typing import Any, Awaitable, Callable, cast

from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from starlette.types import Receive, Send
from starlette.types import Scope as ASGIScope

from stratae.lifecycle import AsyncScope
from stratae.lifecycle.scope import AsyncActivation

_ACTIVATION_KEY = "stratae.activation"
"""ASGI scope key holding a request's open activation, until whichever side closes it."""


async def _close_activation(asgi_scope: ASGIScope, exc: BaseException) -> None:
    """Close the request's activation with `exc`, unless the other side already closed it."""
    activation: AsyncActivation | None = asgi_scope.pop(_ACTIVATION_KEY, None)
    if activation is None:
        return
    await activation.scope.deactivate(activation, exc)


def _endpoint_function(endpoint: Callable[..., Any]) -> Callable[..., Any]:
    """
    Return the function or method behind `endpoint`, seeing through `functools.partial`.

    :raises TypeError: If it's neither. Starlette runs any other endpoint - a class such as
        an `HTTPEndpoint` subclass, or an ASGI app - as an ASGI app that sends its own
        response, which a scoped route doesn't support.
    """
    target = endpoint
    while isinstance(target, partial):
        target = target.func
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
    Return `endpoint` itself if async, or a coroutine running it in Starlette's threadpool.

    Starlette runs a sync endpoint in its threadpool, so the scoped wrapper, which is always
    async, does the same. The threadpool copies the calling context, so the request scope -
    and a handler scope - stay visible to the endpoint there.
    """
    if iscoroutinefunction(_endpoint_function(endpoint)):
        return cast(Callable[[Request], Awaitable[Response]], endpoint)

    async def run_sync(request: Request) -> Response:
        return await run_in_threadpool(cast(Callable[[Request], Response], endpoint), request)

    return run_sync


def scoped_route(scope: AsyncScope, *, handler: AsyncScope | None = None) -> type[Route]:
    """
    Build a Route subclass that activates `scope` around every request it handles.

    :param scope: Active until the response is fully sent, including a streamed body and
        background tasks. Its resources' cleanup runs after the response, so suits
        releasing resources rather than work whose failure should change the response.
    :param handler: An optional scope nested inside `scope`, active only while the endpoint
        runs. It ends before the response starts, so a transaction cached in it commits,
        or rolls back on an exception, in time for a failed commit to become a 500. Omit
        it when no work needs to finish before the response.
    :returns: The Route subclass, to use in place of `Route` when building routes. Its
        endpoints must be functions or methods, sync or async; building one for any other
        endpoint, such as an `HTTPEndpoint` class, raises TypeError.
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
            # Inlined rather than awaiting _close_activation: the success path runs on
            # every request, and this saves a coroutine per request.
            activation: AsyncActivation | None = scope.pop(_ACTIVATION_KEY, None)
            if activation is not None:
                await lifecycle_scope.deactivate(activation)

    return ScopedRoute
