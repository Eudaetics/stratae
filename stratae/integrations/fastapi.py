"""
FastAPI integration for lifecycle management.

{py:func}`scoped_route` builds an `APIRoute` subclass that activates an
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` around every
request it handles. Set it as a router's `route_class`. A cached resource
then opens once per request and closes when the request finishes, even if a
handler calls it more than once.

````{example} Caching a database connection and a per-request cursor
```{code-block} python
import sqlite3
from contextlib import asynccontextmanager
from typing import Annotated
from fastapi import FastAPI
from fastapi.testclient import TestClient
from stratae.depends import Depends, inject
from stratae.integrations.fastapi import scoped_route
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with application.activate():
        yield

app = FastAPI(lifespan=lifespan)
app.router.route_class = scoped_route(request)

# inject strips cur from the wrapper's exposed signature, so FastAPI only
# ever sees product_id/name when it inspects the route.
@app.post("/products/{product_id}")
@inject
async def create_product(
    product_id: str, name: str, cur: CursorDep
) -> dict[str, str]:
    cur.execute("INSERT INTO products VALUES (?, ?)", (product_id, name))
    return {"status": "created"}

@app.get("/products/{product_id}")
@inject
async def read_product(product_id: str, cur: CursorDep) -> dict[str, str]:
    row = cur.execute(
        "SELECT name FROM products WHERE id = ?", (product_id,)
    ).fetchone()
    return {"name": row[0]}

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

from typing import Any, Callable, Coroutine

from fastapi import Request, Response
from fastapi.routing import APIRoute

from stratae.lifecycle import AsyncScope


def scoped_route(scope: AsyncScope) -> type[APIRoute]:
    """Build an APIRoute subclass that activates `scope` around every request it handles."""

    class ScopedRoute(APIRoute):
        def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
            handler = super().get_route_handler()

            async def scoped_handler(request: Request) -> Response:
                async with scope.activate():
                    return await handler(request)

            return scoped_handler

    return ScopedRoute
