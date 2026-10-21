"""
Starlette integration for lifecycle management.

{py:func}`scoped_route` builds a `Route` subclass that activates an
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` around every
request it handles. Pass it in place of `Route` when constructing routes. A
cached resource then opens once per request and closes when the request
finishes, even if a handler calls it more than once.

````{example} Caching a database connection and a per-request cursor
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
    conn = await get_connection()
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    finally:
        cur.close()

type CursorDep = Annotated[sqlite3.Cursor, Depends(get_cursor)]

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
    async with application.activate():
        yield

ScopedRoute = scoped_route(request)
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

from functools import wraps
from typing import Any, Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from starlette.types import Receive, Send
from starlette.types import Scope as ASGIScope

from stratae.lifecycle import AsyncScope
from stratae.lifecycle.scope import AsyncActivation

_ACTIVATION_KEY = "stratae.activation"
"""ASGI scope key holding a request's open activation, until whichever side closes it."""


async def _close_activation(asgi_scope: ASGIScope, exc: BaseException | None) -> None:
    """Close the request's activation with `exc`, unless the other side already closed it."""
    activation: AsyncActivation | None = asgi_scope.pop(_ACTIVATION_KEY, None)
    if activation is None:
        return
    if exc is None:
        await activation.__aexit__(None, None, None)
    else:
        await activation.__aexit__(type(exc), exc, exc.__traceback__)


def scoped_route(scope: AsyncScope) -> type[Route]:
    """Build a Route subclass that activates `scope` around every request it handles."""
    # Bound to a new name: handle()'s own `scope` parameter, the ASGI scope, shadows it.
    lifecycle_scope = scope

    class ScopedRoute(Route):
        def __init__(
            self,
            path: str,
            endpoint: Callable[[Request], Awaitable[Response]],
            **kwargs: Any,
        ) -> None:
            @wraps(endpoint)
            async def scoped_endpoint(request: Request) -> Response:
                try:
                    return await endpoint(request)
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
            await _close_activation(scope, None)

    return ScopedRoute
