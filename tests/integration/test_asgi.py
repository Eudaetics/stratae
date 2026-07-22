"""Protocol-level tests for RequestLifecycleMiddleware, using no ASGI framework."""

from typing import Sequence

import pytest

from stratae.integrations.asgi import (
    ASGIReceiveCallable,
    ASGISendCallable,
    RequestLifecycleMiddleware,
    Scope,
)
from stratae.lifecycle import AsyncLifecycle


async def _receive() -> dict[str, object]:
    """Fail the test if the middleware calls receive - it must stay transparent."""
    raise AssertionError("middleware must not call receive")


async def _send(message: dict[str, object]) -> None:
    """Fail the test if the middleware calls send - it must stay transparent."""
    raise AssertionError("middleware must not call send")


async def test_http_request_runs_app_inside_scope(async_lifecycle: AsyncLifecycle):
    """
    Test that HTTP requests are wrapped in the configured lifecycle scope.

    Given: A RequestLifecycleMiddleware wrapping a recording ASGI app
    When: The middleware handles an http-type scope
    Then: The app runs with the request scope active, receives the middleware's arguments
        unchanged, and the scope is exited before the middleware returns
    """
    # Arrange
    calls: list[tuple[Sequence[str], Scope, ASGIReceiveCallable, ASGISendCallable]] = []

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        """Record the active scopes and arguments seen by the downstream app."""
        calls.append((async_lifecycle.active_scopes(), scope, receive, send))

    middleware = RequestLifecycleMiddleware(app, async_lifecycle, "request")
    http_scope: Scope = {"type": "http"}

    # Act
    await middleware(http_scope, _receive, _send)

    # Assert
    assert len(calls) == 1
    active_scopes, seen_scope, seen_receive, seen_send = calls[0]
    assert active_scopes == ["request"], "App should run inside the request scope"
    assert seen_scope is http_scope
    assert seen_receive is _receive
    assert seen_send is _send
    assert async_lifecycle.is_empty(), "Scope should be exited before the middleware returns"


async def test_non_http_scope_passes_through_without_lifecycle(async_lifecycle: AsyncLifecycle):
    """
    Test that non-HTTP scope types bypass lifecycle management.

    Given: A RequestLifecycleMiddleware wrapping a recording ASGI app
    When: The middleware handles a lifespan-type scope
    Then: The app runs with no lifecycle scope active
    """
    # Arrange
    active: list[Sequence[str]] = []

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        """Record the active scopes seen by the downstream app."""
        active.append(async_lifecycle.active_scopes())

    middleware = RequestLifecycleMiddleware(app, async_lifecycle, "request")

    # Act
    await middleware({"type": "lifespan"}, _receive, _send)

    # Assert
    assert active == [[]], "App should run with no lifecycle scope active"


async def test_app_exception_propagates_and_scope_exits(async_lifecycle: AsyncLifecycle):
    """
    Test that app exceptions propagate while the scope still exits.

    Given: A RequestLifecycleMiddleware wrapping an ASGI app that raises
    When: The middleware handles an http-type scope
    Then: The exception propagates to the caller and the request scope is exited
    """

    # Arrange
    class BoomError(Exception):
        """Sentinel exception raised by the downstream app."""

    async def app(scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
        """Raise from within the request scope."""
        raise BoomError

    middleware = RequestLifecycleMiddleware(app, async_lifecycle, "request")

    # Act / Assert
    with pytest.raises(BoomError):
        await middleware({"type": "http"}, _receive, _send)

    assert async_lifecycle.is_empty(), "Scope should be exited even when the app raises"
