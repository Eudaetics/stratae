"""
FastAPI integration for lifecycle management.

{py:func}`scoped_route` builds an `APIRoute` subclass that activates an
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` around each request. Set it as
a router's `route_class`.

Work that must finish before the response, like committing a transaction, belongs in a
nested `handler` scope. It ends when the route handler returns, so a failed commit becomes a 500.

````{example} Committing a per-request transaction before the response
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with application:
        yield

app = FastAPI(lifespan=lifespan)
app.router.route_class = scoped_route(request, handler=transaction)

# inject hides cur from the route's signature.
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
from starlette.types import Receive, Send
from starlette.types import Scope as ASGIScope

from stratae.lifecycle import AsyncScope
from stratae.lifecycle.scope import AsyncActivation

_ACTIVATION_KEY = "stratae.activation"
"""ASGI scope key holding a request's open activation, until whichever side closes it."""


async def _close_activation(asgi_scope: ASGIScope, exc: BaseException) -> None:
    """Close the request's activation with `exc`, unless already closed."""
    activation: AsyncActivation | None = asgi_scope.pop(_ACTIVATION_KEY, None)
    if activation is None:
        return
    await activation.scope.deactivate(activation, exc)


def scoped_route(scope: AsyncScope, *, handler: AsyncScope | None = None) -> type[APIRoute]:
    """
    Build an APIRoute subclass that activates `scope` around every request.

    :param scope: Active until the response is fully sent, including streaming and
        background tasks.
    :param handler: An optional nested scope, active only while the route handler runs,
        including dependency solving and response validation. Use it for work like
        commits that must finish before the response.
    :returns: An APIRoute subclass, to set as a router's `route_class`.
    """
    lifecycle_scope = scope
    handler_scope = handler

    class ScopedRoute(APIRoute):
        async def handle(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
            scope[_ACTIVATION_KEY] = lifecycle_scope.activate()
            try:
                await super().handle(scope, receive, send)
            except BaseException as exc:
                await _close_activation(scope, exc)
                raise
            # Inlined _close_activation, saving a coroutine per request.
            activation: AsyncActivation | None = scope.pop(_ACTIVATION_KEY, None)
            if activation is not None:
                await lifecycle_scope.deactivate(activation)

        def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
            route_handler = super().get_route_handler()

            if handler_scope is not None:
                active_handler_scope = handler_scope

                async def handler_scoped(request: Request) -> Response:
                    async with active_handler_scope:
                        return await route_handler(request)

                return handler_scoped

            async def scoped_handler(request: Request) -> Response:
                try:
                    return await route_handler(request)
                except BaseException as exc:
                    await _close_activation(request.scope, exc)
                    raise

            return scoped_handler

    return ScopedRoute
