"""
The scope activation behind the Starlette and FastAPI scoped routes.

A scoped route activates its scope around each request or connection, and closes it once the
response is fully sent, including streaming and background tasks. It closes it with the error
the request ended with: the one that escaped the route, or else the one the endpoint raised.
The framework turns some errors into a response inside the route, such as `HTTPException`, so
the endpoint's wrapper records what it raised, for the route to close with.
"""

from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, ClassVar

from starlette.requests import HTTPConnection
from starlette.routing import BaseRoute
from starlette.types import Receive, Send
from starlette.types import Scope as ASGIScope

from stratae.lifecycle import AsyncScope

_ERROR_KEY = "stratae.endpoint_error"
"""ASGI scope key holding the error a request's or connection's endpoint raised."""


def within[C: HTTPConnection, R](
    scope: AsyncScope, call: Callable[[C], Awaitable[R]]
) -> Callable[[C], Coroutine[Any, Any, R]]:
    """
    Return `call`, run with `scope` active around it.

    :param scope: Active only while `call` runs, such as a route's handler scope.
    :param call: The endpoint, or the framework's handler around it.
    """

    async def scoped(connection: C) -> R:
        async with scope:
            return await call(connection)

    return scoped


def recording_errors[C: HTTPConnection, R](
    call: Callable[[C], Awaitable[R]],
) -> Callable[[C], Coroutine[Any, Any, R]]:
    """
    Return `call`, recording what it raises for the route to close the activation with.

    :param call: The endpoint, or the framework's handler around it.
    """

    async def recorded(connection: C) -> R:
        try:
            return await call(connection)
        except BaseException as exc:
            connection.scope[_ERROR_KEY] = exc
            raise

    return recorded


class ActivatingRoute(BaseRoute):
    """
    Activate `activates` around each request or connection a route handles.

    Mix it in ahead of a Starlette route class, and set `activates` on the subclass. Wrap the
    endpoint in {py:func}`recording_errors`, so errors the framework handles reach the scope.
    """

    activates: ClassVar[AsyncScope]
    """The scope activated for each request or connection."""

    async def handle(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
        """Handle a request or connection with the scope active, closing it once at the end."""
        lifecycle_scope = self.activates
        activation = await lifecycle_scope.activate()
        try:
            await super().handle(scope, receive, send)
        except BaseException as exc:
            await lifecycle_scope.deactivate(activation, exc)
            raise
        await lifecycle_scope.deactivate(activation, scope.get(_ERROR_KEY))
