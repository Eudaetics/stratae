"""
Wrap ASGI requests in a lifecycle scope for the duration of each request.

{py:class}`RequestLifecycleMiddleware` is ASGI middleware that activates a named
{py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>` scope around
each incoming HTTP connection, then runs the wrapped app inside it. The scope exits
once the app returns or raises. Non-HTTP connections, such as `"websocket"` and
`"lifespan"`, pass straight through to the wrapped app with no scope active. This
middleware works with any ASGI framework, including FastAPI, Starlette, and Quart.

The module also defines the ASGI type aliases `Scope`, `ASGIReceiveCallable`,
`ASGISendCallable`, and `ASGI3Application`, used to type the middleware's
constructor and `__call__` signatures without a hard dependency on any particular
ASGI framework's own types.

```{rubric} Example:
```
<!--- skip: next -->
```{code-block} python
:caption: A request ID is generated once per request and shared by a dependency and its handler

from uuid import uuid4

from fastapi import Depends, FastAPI
from stratae.integrations.asgi import RequestLifecycleMiddleware
from stratae.lifecycle import AsyncLifecycle, Scope

lifecycle = AsyncLifecycle([Scope("request")])

@lifecycle.cache("request")
async def get_request_id() -> str:
    return str(uuid4())

async def log_request():
    request_id = await get_request_id()
    print(f"handling request {request_id}")

app = FastAPI()
app.add_middleware(RequestLifecycleMiddleware, lifecycle=lifecycle, scope="request")

@app.get("/orders", dependencies=[Depends(log_request)])
async def list_orders():
    request_id = await get_request_id()  # same ID the log_request dependency just logged
    return {"request_id": request_id}
```

See {py:class}`RequestLifecycleMiddleware` for additional examples.
"""

from typing import Awaitable, Callable

from stratae.lifecycle import AsyncLifecycle

# Redefine types for ASGI applications
type Scope = dict[str, object]
type ASGIReceiveCallable = Callable[[], Awaitable[dict[str, object]]]
type ASGISendCallable = Callable[[dict[str, object]], Awaitable[None]]
type ASGI3Application = Callable[[Scope, ASGIReceiveCallable, ASGISendCallable], Awaitable[None]]


class RequestLifecycleMiddleware:
    """
    ASGI middleware that activates a lifecycle scope around each HTTP request.

    Add it as the outermost layer of the ASGI stack, so the scope is active for
    everything downstream, including other middleware. Only `"http"` connections
    are wrapped; other connection types, such as `"websocket"` and `"lifespan"`,
    are passed through to the wrapped app unchanged, with no scope active.

    ```{rubric} Example:
    ```
    <!--- skip: next -->
    ```{code-block} python
    :caption: Wrapping a FastAPI app so its routes can use request-scoped dependencies

    from fastapi import FastAPI
    from stratae.integrations.asgi import RequestLifecycleMiddleware
    from stratae.lifecycle import AsyncLifecycle, Scope

    lifecycle = AsyncLifecycle([Scope("request")])

    @lifecycle.cache("request")
    async def get_db_connection() -> object:
        return object()  # a pooled connection, opened once per request

    app = FastAPI()
    app.add_middleware(RequestLifecycleMiddleware, lifecycle=lifecycle, scope="request")

    @app.get("/orders")
    async def list_orders():
        conn = await get_db_connection()
        return {"connection": id(conn)}
    ```

    """

    __slots__ = ("app", "_lifecycle", "_scope")

    def __init__(self, app: ASGI3Application, lifecycle: AsyncLifecycle, scope: str):
        """
        Store the wrapped app and the lifecycle scope to activate for each request.

        :param app: The downstream ASGI application to call for every connection.
        :param lifecycle: The
            {py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>` whose
            `scope` gets activated around each HTTP connection.
        :param scope: Name of the
            {py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>` scope
            to activate; must match one of the scopes `lifecycle` was constructed with.

        """
        self.app = app
        self._lifecycle = lifecycle
        self._scope = scope

    async def __call__(self, scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        """
        Run `app` inside the configured lifecycle scope for HTTP connections.

        Other connection types are passed straight through to `app` with no scope
        active. `scope` here is the ASGI connection scope dict passed by the server,
        not the {py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>`
        scope name given to the constructor.

        :param scope: The ASGI connection scope dict for this connection;
            `scope["type"]` determines whether the lifecycle scope is
            activated before calling `app`.
        :param receive: ASGI receive callable, passed to `app` unchanged.
        :param send: ASGI send callable, passed to `app` unchanged.
        :raises Exception: Any exception raised by `app`; propagates after the
            lifecycle scope exits, if one was activated for this connection.

        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async with self._lifecycle.start(self._scope):
            await self.app(scope, receive, send)
