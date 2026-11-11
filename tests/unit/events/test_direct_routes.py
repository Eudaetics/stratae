"""
Unit tests for how both direct buses find an event's handlers.

Every test runs against DirectBus and AsyncDirectBus.

This test suite verifies the following behaviors:

DirectBus and AsyncDirectBus (routes):
- A bound callable emits without looking its event up: the event isn't hashed or compared.
- A handler registered after binding receives the bound callable's next emit.
- A handler removed after binding doesn't receive the bound callable's next emit.
- Two separately built equal events share their handlers, whichever is bound and whichever is
  handled, in either order.
- A serializer given to bind is ignored, as emit ignores it.
"""

from collections.abc import Callable
from inspect import isawaitable
from typing import Any, Literal
from unittest.mock import Mock

import pytest

from stratae.events import AsyncDirectBus, DirectBus, PubSub


class _Placed:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


_looked_up: list[str] = []
"""Records each time a _Counted event is hashed or compared."""


class _Counted(PubSub[_Placed, Literal[False]]):
    """A PubSub event recording each time it's hashed or compared, as a lookup would."""

    __slots__ = ()

    def __hash__(self) -> int:
        _looked_up.append("hash")
        return super().__hash__()

    def __eq__(self, other: object) -> bool:
        _looked_up.append("eq")
        return super().__eq__(other)


type _Bus = DirectBus | AsyncDirectBus


@pytest.fixture(params=[DirectBus, AsyncDirectBus], ids=["DirectBus", "AsyncDirectBus"])
def bus(request: pytest.FixtureRequest) -> _Bus:
    """Return a fresh bus of each kind."""
    kind: Callable[[], _Bus] = request.param
    return kind()


async def _call(bound: Callable[..., Any], payload: _Placed) -> None:
    """Call a bound callable from either bus, awaiting the async bus's result."""
    result = bound(payload)
    if isawaitable(result):
        await result


async def test_bound_callable_does_not_look_up_its_event(bus: _Bus) -> None:
    """
    A bound callable should reach its handlers without hashing or comparing its event.

    Given: A handler for an event that records each hash and comparison, and a bound callable
    When: The bound callable is called three times
    Then: The handler receives every call, and the event is never hashed or compared
    """
    # Arrange
    event = _Counted(_Placed)
    handler = Mock()
    bus.handle(event, handler)
    bound = bus.bind(event)
    _looked_up.clear()

    # Act
    for order_id in range(3):
        await _call(bound, _Placed(order_id))

    # Assert
    assert handler.call_count == 3
    assert _looked_up == []


async def test_handler_registered_after_binding_receives_next_emit(bus: _Bus) -> None:
    """
    A handler registered after binding should receive the bound callable's next emit.

    Given: A bound callable for an event with no handlers, called once
    When: A handler is registered, and the bound callable called again
    Then: The handler receives only the second call's payload
    """
    # Arrange
    event = PubSub(_Placed)
    bound = bus.bind(event)
    await _call(bound, _Placed(1))
    handler = Mock()
    bus.handle(event, handler)
    payload = _Placed(2)

    # Act
    await _call(bound, payload)

    # Assert
    handler.assert_called_once_with(payload)


async def test_handler_removed_after_binding_misses_next_emit(bus: _Bus) -> None:
    """
    A handler removed after binding shouldn't receive the bound callable's next emit.

    Given: Two handlers for an event, and a bound callable
    When: One handler is removed, and the bound callable called
    Then: Only the remaining handler receives the payload
    """
    # Arrange
    event = PubSub(_Placed)
    kept, removed = Mock(), Mock()
    bus.handle(event, kept)
    registered = bus.handle(event, removed)
    bound = bus.bind(event)

    # Act
    bus.remove(registered)
    await _call(bound, _Placed(3))

    # Assert
    kept.assert_called_once()
    removed.assert_not_called()


@pytest.mark.parametrize("bind_first", [True, False], ids=["bind-first", "handle-first"])
async def test_equal_events_share_handlers(bus: _Bus, bind_first: bool) -> None:
    """
    Separately built equal events should share handlers, whichever is bound and handled.

    Given: Two equal PubSub events built separately, one bound and one handled, in either order
    When: The bound callable is called
    Then: The handler registered through the other event receives the payload
    """
    # Arrange
    bound_event, handled_event = PubSub(_Placed), PubSub(_Placed)
    handler = Mock()
    if bind_first:
        bound = bus.bind(bound_event)
        bus.handle(handled_event, handler)
    else:
        bus.handle(handled_event, handler)
        bound = bus.bind(bound_event)
    payload = _Placed(4)

    # Act
    await _call(bound, payload)

    # Assert
    handler.assert_called_once_with(payload)


async def test_serializer_given_to_bind_is_ignored(bus: _Bus) -> None:
    """
    A serializer given to bind should be ignored, as emit ignores it: nothing is serialized.

    Given: A handler, and a callable bound with a serializer
    When: The bound callable is called
    Then: The handler receives the payload itself, and the serializer isn't called
    """
    # Arrange
    event = PubSub(_Placed)
    handler, serializer = Mock(), Mock()
    bus.handle(event, handler)
    bound = bus.bind(event, serializer=serializer)
    payload = _Placed(5)

    # Act
    await _call(bound, payload)

    # Assert
    handler.assert_called_once_with(payload)
    serializer.assert_not_called()
