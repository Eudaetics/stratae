"""
Starlette integration for lifecycle management.

{py:func}`scoped_route` builds a `Route` subclass that activates a
{py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>` scope
around every request it handles. Pass it in place of `Route` when
constructing routes. A cached resource then opens once per request and
closes when the request finishes, even if a handler calls it more than
once.

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
from stratae.lifecycle import AsyncLifecycle, Scope, async_resource

lifecycle = AsyncLifecycle([
    Scope("application", "shared"), Scope("request")
])

@lifecycle.cache("application")
@async_resource
async def get_connection():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE products (id TEXT PRIMARY KEY, name TEXT)")
    conn.commit()
    try:
        yield conn
    finally:
        conn.close()

@lifecycle.cache("request")
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
    async with lifecycle.start("application"):
        yield

ScopedRoute = scoped_route(lifecycle, "request")
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

from typing import Any, Awaitable, Callable

from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from stratae.lifecycle.lifecycle import AsyncLifecycle


def scoped_route(lifecycle: AsyncLifecycle, scope: str) -> type[Route]:
    """Build a Route subclass that activates `scope` around every request it handles."""

    class ScopedRoute(Route):
        def __init__(
            self,
            path: str,
            endpoint: Callable[[Request], Awaitable[Response]],
            **kwargs: Any,
        ) -> None:
            async def scoped_endpoint(request: Request) -> Response:
                async with lifecycle.start(scope):
                    return await endpoint(request)

            super().__init__(path, scoped_endpoint, **kwargs)

    return ScopedRoute
