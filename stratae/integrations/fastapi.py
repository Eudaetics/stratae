"""
FastAPI integration for lifecycle management.

{py:func}`scoped_route` builds an `APIRoute` subclass that activates an
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` around every
request it handles. Set it as a router's `route_class`. A cached resource
then opens once per request and closes when the request finishes, even if a
handler calls it more than once.

Work that must finish before the response starts, such as committing a
transaction, belongs in a second scope nested inside the request scope and
passed as `handler`. It's active only while the route handler runs, so a commit
happens - or fails, becoming a 500 - before any response is sent, while the
request scope's resources stay open for a streamed body and background tasks.

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


def scoped_route(scope: AsyncScope, *, handler: AsyncScope | None = None) -> type[APIRoute]:
    """
    Build an APIRoute subclass that activates `scope` around every request it handles.

    :param scope: Active until the response is fully sent, including a streamed body and
        background tasks. Its resources' cleanup runs after the response, so suits
        releasing resources rather than work whose failure should change the response.
    :param handler: An optional scope nested inside `scope`, active only while the route
        handler runs - FastAPI's dependency solving, the endpoint, and response validation.
        It ends before the response starts, so a transaction cached in it commits, or
        rolls back on an exception, in time for a failed commit to become a 500. Omit it
        when no work needs to finish before the response.
    :returns: The APIRoute subclass, to set as a router's `route_class`.
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
            # Inlined rather than awaiting _close_activation: the success path runs on
            # every request, and this saves a coroutine per request.
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
