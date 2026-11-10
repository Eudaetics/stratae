"""
The activation handoff behind the Starlette and FastAPI scoped routes.

A scoped route activates its scope around each request or connection. Two sides can close
the activation. The endpoint closes it early, with the exception it raised, before the
framework turns that exception into a response, so resources see the real error. The route
closes it once the response is fully sent, including streaming and background tasks. The
activation waits in the ASGI scope until whichever side closes it first.
"""

from typing import ClassVar

from starlette.routing import BaseRoute
from starlette.types import Receive, Send
from starlette.types import Scope as ASGIScope

from stratae.lifecycle import AsyncScope
from stratae.lifecycle.scope import AsyncActivation

_ACTIVATION_KEY = "stratae.activation"
"""ASGI scope key holding a request's or connection's open activation, until a side closes it."""


async def close_activation(asgi_scope: ASGIScope, exc: BaseException) -> None:
    """
    Close the request's or connection's activation with `exc`, unless already closed.

    :param asgi_scope: The request's or connection's ASGI scope.
    :param exc: The exception the endpoint raised, passed to each resource's cleanup.
    """
    activation: AsyncActivation | None = asgi_scope.pop(_ACTIVATION_KEY, None)
    if activation is None:
        return
    await activation.scope.deactivate(activation, exc)


class ActivatingRoute(BaseRoute):
    """
    Activate `activates` around each request or connection a route handles.

    Mix it in ahead of a Starlette route class, and set `activates` on the subclass.
    """

    activates: ClassVar[AsyncScope]
    """The scope activated for each request or connection."""

    async def handle(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
        """Handle a request or connection with the scope active, closing it unless closed."""
        lifecycle_scope = self.activates
        scope[_ACTIVATION_KEY] = await lifecycle_scope.activate()
        try:
            await super().handle(scope, receive, send)
        except BaseException as exc:
            await close_activation(scope, exc)
            raise
        # Inlined close_activation, saving a coroutine per request.
        activation: AsyncActivation | None = scope.pop(_ACTIVATION_KEY, None)
        if activation is not None:
            await lifecycle_scope.deactivate(activation)
