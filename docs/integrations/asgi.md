# ASGI

`RequestLifecycleMiddleware` bridges `stratae.lifecycle` to any ASGI framework — FastAPI, Starlette, Quart, or a raw ASGI app. It activates a named lifecycle scope for the duration of each HTTP request, and closes it (running any resource cleanup registered during the request) when the app returns or raises.

```python
from stratae.integrations.asgi import RequestLifecycleMiddleware
from stratae.lifecycle import AsyncLifecycle, Scope

lifecycle = AsyncLifecycle([Scope("request")])

@lifecycle.cache("request")
async def get_request_id() -> str:
    return str(uuid4())

app.add_middleware(RequestLifecycleMiddleware, lifecycle=lifecycle, scope="request")

@app.get("/orders")
async def list_orders():
    request_id = await get_request_id()
    return {"request_id": request_id}
```

A couple of things worth being deliberate about:

- Install it as the **outermost** middleware layer, so the scope is active for everything downstream, including other middleware.
- Only `"http"` connections get the scope activated. WebSocket and lifespan connections pass straight through to the wrapped app with no scope active — a handler that tries to resolve a request-scoped dependency from a WebSocket route will get a `ScopeInactiveError`.
- The constructor's `scope` argument is a `stratae.lifecycle` scope *name* (a string, like `"request"` above) — not the ASGI connection `scope` dict that `__call__` receives. They share a name because both are called "scope" in their respective worlds, but they're unrelated.
- If the wrapped app raises, the exception propagates *after* the lifecycle scope exits, so cleanup still runs on error.

This composes the same way any `stratae.lifecycle` scope does — see the [Lifecycle guide](../lifecycle) for caching semantics, and the [Dependency Injection guide](../dependency-injection) for injecting request-scoped values into handlers alongside route logic.

Full signatures: {doc}`stratae.integrations.asgi API reference <../apidocs/stratae.integrations/stratae.integrations.asgi>`.
