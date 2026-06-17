"""ASGI integration for lifecycle management."""

from typing import Awaitable, Callable

from stratae.lifecycle import AsyncLifecycle

# Redefine types for ASGI applications
type Scope = dict[str, object]
type ASGIReceiveCallable = Callable[[], Awaitable[dict[str, object]]]
type ASGISendCallable = Callable[[dict[str, object]], Awaitable[None]]
type ASGI3Application = Callable[[Scope, ASGIReceiveCallable, ASGISendCallable], Awaitable[None]]


class RequestLifecycleMiddleware:
    """
    ASGI middleware to manage the REQUEST lifecycle scope.

    This middleware ensures that each incoming HTTP request is wrapped in a REQUEST scope
    lifecycle context. It should be added as the outermost middleware to ensure that the
    REQUEST scope is properly managed for all requests.

    Works with any ASGI framework (FastAPI, Starlette, Quart, etc.).
    """

    def __init__(self, app: ASGI3Application, lifecycle: AsyncLifecycle, scope: str):
        """Initialize the RequestLifecycleMiddleware."""
        self.app = app
        self._lifecycle = lifecycle
        self._scope = scope

    async def __call__(self, scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable):
        """Wrap HTTP requests in a REQUEST scope."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async with self._lifecycle.start(self._scope):
            await self.app(scope, receive, send)
