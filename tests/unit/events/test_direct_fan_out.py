"""
Unit tests for how both direct buses collect their sync PubSub handlers' failures.

Every test runs against DirectBus and AsyncDirectBus, so the two can't drift apart.

This test suite verifies the following behaviors:

DirectBus and AsyncDirectBus (sync handler failures):
- A handler raising an Exception doesn't stop the other handlers.
- Every handler's Exception is raised together in a HandlerErrors group.
- A handler's CancelledError doesn't stop the other handlers, and is raised with the other
  failures in a BaseHandlerErrors group that isn't a HandlerErrors.
- A handler's KeyboardInterrupt propagates at once, ungrouped.
- A request responder's exception propagates directly, not grouped.
"""

import asyncio
from collections.abc import Callable
from inspect import isawaitable
from typing import Any
from unittest.mock import Mock

import pytest

from stratae.events import AsyncDirectBus, DirectBus, PubSub, Request
from stratae.events.exceptions import BaseHandlerErrors, HandlerErrors


class _Placed:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


class _Lookup:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


_placed = PubSub(_Placed)
_lookup = Request(_Lookup, reply=str)

type _Bus = DirectBus | AsyncDirectBus


@pytest.fixture(params=[DirectBus, AsyncDirectBus], ids=["DirectBus", "AsyncDirectBus"])
def bus(request: pytest.FixtureRequest) -> _Bus:
    """Return a fresh bus of each kind."""
    kind: Callable[[], _Bus] = request.param
    return kind()


async def _emit(bus: _Bus, event: Any, payload: Any) -> Any:
    """Emit on either bus, awaiting the async bus's result."""
    result = bus.emit(event, payload)
    if isawaitable(result):
        return await result
    return result


def _raising(error: BaseException) -> Mock:
    """Return a sync handler raising `error`."""
    return Mock(side_effect=error)


async def test_raising_handler_does_not_prevent_other_handlers(bus: _Bus) -> None:
    """
    A handler raising an Exception should not stop the other handlers.

    Given: A sync handler raising ValueError and another sync handler
    When: The event is emitted
    Then: The other handler is still called
    """
    # Arrange
    other = Mock()
    bus.handle(_placed, _raising(ValueError("boom")))
    bus.handle(_placed, other)
    payload = _Placed(1)

    # Act
    with pytest.raises(HandlerErrors):
        await _emit(bus, _placed, payload)

    # Assert
    other.assert_called_once_with(payload)


async def test_handler_exceptions_collected_into_handler_errors(bus: _Bus) -> None:
    """
    Every handler's Exception should be raised together in a HandlerErrors group.

    Given: Two sync handlers, raising ValueError and RuntimeError
    When: The event is emitted
    Then: A HandlerErrors holding both is raised
    """
    # Arrange
    error_a, error_b = ValueError("first"), RuntimeError("second")
    bus.handle(_placed, _raising(error_a))
    bus.handle(_placed, _raising(error_b))

    # Act / Assert
    with pytest.raises(HandlerErrors) as exc_info:
        await _emit(bus, _placed, _Placed(2))

    assert set(exc_info.value.exceptions) == {error_a, error_b}


async def test_cancelled_error_is_collected_with_the_other_failures(bus: _Bus) -> None:
    """
    A sync handler's CancelledError should be collected, not stop the emit.

    A sync handler can't receive its task's cancellation, so its CancelledError is its own
    failure.

    Given: A sync handler raising CancelledError, one raising ValueError, and one that succeeds
    When: The event is emitted
    Then: Every handler runs, and a BaseHandlerErrors that isn't a HandlerErrors holds both
        failures
    """
    # Arrange
    cancelled, error = asyncio.CancelledError(), ValueError("failed")
    succeeding = Mock()
    bus.handle(_placed, _raising(cancelled))
    bus.handle(_placed, _raising(error))
    bus.handle(_placed, succeeding)
    payload = _Placed(3)

    # Act
    with pytest.raises(BaseHandlerErrors) as exc_info:
        await _emit(bus, _placed, payload)

    # Assert
    assert not isinstance(exc_info.value, HandlerErrors)
    assert set(exc_info.value.exceptions) == {cancelled, error}
    succeeding.assert_called_once_with(payload)


async def test_keyboard_interrupt_propagates_ungrouped(bus: _Bus) -> None:
    """
    A handler's KeyboardInterrupt should propagate at once, not be collected.

    Given: A sync handler raising KeyboardInterrupt, and one raising ValueError
    When: The event is emitted
    Then: The KeyboardInterrupt itself is raised, not a group
    """
    # Arrange
    interrupt = KeyboardInterrupt()
    bus.handle(_placed, _raising(interrupt))
    bus.handle(_placed, _raising(ValueError("failed")))

    # Act / Assert
    with pytest.raises(KeyboardInterrupt) as exc_info:
        await _emit(bus, _placed, _Placed(4))

    assert exc_info.value is interrupt


async def test_request_responder_exception_propagates_directly(bus: _Bus) -> None:
    """
    A sync responder's exception should propagate to the emitter unwrapped.

    Given: A sync responder raising CancelledError for a request event
    When: The request is emitted
    Then: The CancelledError itself is raised, not a group
    """
    # Arrange
    cancelled = asyncio.CancelledError()
    bus.handle(_lookup, _raising(cancelled))

    # Act / Assert
    with pytest.raises(asyncio.CancelledError) as exc_info:
        await _emit(bus, _lookup, _Lookup(5))

    assert exc_info.value is cancelled
