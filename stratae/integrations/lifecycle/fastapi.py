"""FastAPI integration for lifecycle management."""

from typing import Any, Callable, Coroutine

from fastapi import Request, Response
from fastapi.routing import APIRoute

from stratae.lifecycle.lifecycle import AsyncLifecycle


def scoped_route(lifecycle: AsyncLifecycle, scope: str) -> type[APIRoute]:
    """Build an APIRoute subclass that activates `scope` around every request it handles."""

    class ScopedRoute(APIRoute):
        def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
            handler = super().get_route_handler()

            async def scoped_handler(request: Request) -> Response:
                async with lifecycle.start(scope):
                    return await handler(request)

            return scoped_handler

    return ScopedRoute
