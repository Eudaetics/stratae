"""Starlette integration for lifecycle management."""

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
