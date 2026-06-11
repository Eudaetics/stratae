"""
Unit tests for the AsyncLocalBus adapter.

This test suite verifies the following behaviors:

AsyncLocalBus:
- publish returns an AsyncBoundEvent.
- Awaiting the AsyncBoundEvent dispatches the payload to a registered sync handler.
- Awaiting the AsyncBoundEvent dispatches the payload to a registered async handler.
- Mixed sync and async handlers on the same channel both receive the payload.
- All handlers on a channel receive the payload.
- Handlers on different channels are isolated from each other.
- subscribe returns a Handler.
- unsubscribe removes a handler; subsequent emits do not invoke it.
- The same callable may be registered multiple times independently.
- emit_publish directly dispatches to handle_subscribe.
- handle_subscribe invokes all handlers registered on the channel.
- A raising handler does not prevent other handlers from running.
- All handler exceptions are collected and re-raised as an ExceptionGroup.

AsyncLocalBus (with envelope):
- Handlers can access the Envelope during dispatch.
- Each top-level emission creates an independent envelope.
- A handler that emits an event receives a child envelope.
- The envelope is cleaned up after dispatch completes.
"""

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from stratae.events.adapters.local_async import AsyncLocalBus
from stratae.events.bound import AsyncBoundEvent
from stratae.events.envelope import Envelope
from stratae.events.event import EventSchema


class _TaskCreated(EventSchema):
    def __init__(self, task_id: int) -> None:
        self.task_id = task_id

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, _TaskCreated):
            return NotImplemented
        return self.task_id == other.task_id


@pytest.fixture
def bus() -> AsyncLocalBus:
    """Return a fresh AsyncLocalBus instance with no envelope."""
    return AsyncLocalBus()


@pytest.fixture
def bus_with_envelope() -> AsyncLocalBus:
    """Return a fresh AsyncLocalBus instance with envelope tracking enabled."""
    return AsyncLocalBus(use_envelope=True)


def test_publish_returns_async_bound_event(bus: AsyncLocalBus):
    """
    Publish should return an AsyncBoundEvent bound to emit_publish.

    Given: An AsyncLocalBus
    When: publish is called with a schema
    Then: An AsyncBoundEvent should be returned
    """
    assert isinstance(bus.publish(_TaskCreated), AsyncBoundEvent)


async def test_dispatches_to_sync_handler(bus: AsyncLocalBus):
    """
    Awaiting a BoundEvent should dispatch the payload to a registered sync handler.

    Given: A sync handler subscribed via a BoundEvent config
    When: The AsyncBoundEvent is awaited
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = Mock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler)

    # Act
    await emit(task_id=1)

    # Assert
    handler.assert_called_once_with(_TaskCreated(1))


async def test_dispatches_to_async_handler(bus: AsyncLocalBus):
    """
    Awaiting a BoundEvent should dispatch the payload to a registered async handler.

    Given: An async handler subscribed via a BoundEvent config
    When: The AsyncBoundEvent is awaited
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler)

    # Act
    await emit(task_id=2)

    # Assert
    handler.assert_called_once_with(_TaskCreated(2))


async def test_dispatches_to_mixed_handlers(bus: AsyncLocalBus):
    """
    Both sync and async handlers subscribed to the same BoundEvent should receive the payload.

    Given: A sync handler and an async handler subscribed to the same BoundEvent
    When: An event is emitted
    Then: Both handlers should be called with the payload
    """
    # Arrange
    sync_handler = Mock()
    async_handler = AsyncMock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, sync_handler)
    bus.subscribe(emit, async_handler)

    # Act
    await emit(task_id=3)

    # Assert
    sync_handler.assert_called_once_with(_TaskCreated(3))
    async_handler.assert_called_once_with(_TaskCreated(3))


async def test_dispatches_to_all_handlers_on_channel(bus: AsyncLocalBus):
    """
    All handlers registered on the same BoundEvent should receive the payload.

    Given: Two async handlers subscribed to the same BoundEvent
    When: An event is emitted
    Then: Both handlers should be called with the payload
    """
    # Arrange
    handler_a = AsyncMock()
    handler_b = AsyncMock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler_a)
    bus.subscribe(emit, handler_b)

    # Act
    await emit(task_id=4)

    # Assert
    handler_a.assert_called_once_with(_TaskCreated(4))
    handler_b.assert_called_once_with(_TaskCreated(4))


async def test_channel_isolation(bus: AsyncLocalBus):
    """
    Handlers subscribed to one BoundEvent should not receive events emitted on another.

    Given: Two handlers each subscribed to a different BoundEvent
    When: An event is emitted on one BoundEvent
    Then: Only the handler for that BoundEvent should be called
    """
    # Arrange
    emit_task = bus.publish(_TaskCreated)
    emit_order = bus.publish(_TaskCreated)
    task_handler = AsyncMock()
    order_handler = AsyncMock()
    bus.subscribe(emit_task, task_handler)
    bus.subscribe(emit_order, order_handler)

    # Act
    await emit_task(task_id=5)

    # Assert
    task_handler.assert_called_once_with(_TaskCreated(5))
    order_handler.assert_not_called()


def test_subscribe_returns_handler(bus: AsyncLocalBus):
    """
    Subscribe should return the Handler wrapping the registered callable.

    Given: An AsyncLocalBus
    When: subscribe is called with a callable
    Then: The returned Handler should wrap that callable
    """
    fn = Mock()
    emit = bus.publish(_TaskCreated)
    handle = bus.subscribe(emit, fn)

    assert handle.call is fn


async def test_unsubscribe_prevents_further_dispatch(bus: AsyncLocalBus):
    """
    Unsubscribed handlers should not receive subsequent emissions.

    Given: A handler subscribed and then unsubscribed
    When: An event is emitted
    Then: The handler should not be called
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.publish(_TaskCreated)
    handle = bus.subscribe(emit, handler)
    bus.unsubscribe(handle)

    # Act
    await emit(task_id=6)

    # Assert
    handler.assert_not_called()


async def test_same_callable_registered_twice_called_twice(bus: AsyncLocalBus):
    """
    Registering the same callable twice should produce two independent subscriptions.

    Given: The same callable subscribed to a BoundEvent twice
    When: An event is emitted
    Then: The callable should be invoked twice
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler)
    bus.subscribe(emit, handler)

    # Act
    await emit(task_id=7)

    # Assert
    assert handler.call_count == 2


async def test_emit_publish_dispatches_directly(bus: AsyncLocalBus):
    """
    emit_publish should dispatch the payload directly to all registered handlers.

    Given: A handler subscribed to a BoundEvent
    When: emit_publish is called directly with that BoundEvent
    Then: The handler should receive the payload
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler)
    payload = _TaskCreated(8)

    # Act
    await bus.emit_publish(payload, emit)

    # Assert
    handler.assert_called_once_with(payload)


async def test_handle_subscribe_invokes_all_handlers(bus: AsyncLocalBus):
    """
    handle_subscribe should invoke every handler registered for the given BoundEvent.

    Given: Two handlers subscribed to a BoundEvent
    When: handle_subscribe is called directly
    Then: Both handlers should receive the payload
    """
    # Arrange
    handler_a = AsyncMock()
    handler_b = AsyncMock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler_a)
    bus.subscribe(emit, handler_b)
    payload = _TaskCreated(9)

    # Act
    await bus.handle_subscribe(payload, config=emit)

    # Assert
    handler_a.assert_called_once_with(payload)
    handler_b.assert_called_once_with(payload)


async def test_raising_handler_does_not_prevent_other_handlers(bus: AsyncLocalBus):
    """
    A handler that raises should not prevent subsequent handlers from running.

    Given: Two handlers subscribed to a BoundEvent, the first of which raises
    When: An event is emitted
    Then: The second handler should still be called
    """
    # Arrange
    second_handler = AsyncMock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, AsyncMock(side_effect=ValueError("boom")))
    bus.subscribe(emit, second_handler)
    payload = _TaskCreated(10)

    # Act
    with pytest.raises(ExceptionGroup):
        await bus.handle_subscribe(payload, config=emit)

    # Assert
    second_handler.assert_called_once_with(payload)


async def test_handler_exceptions_collected_into_exception_group(bus: AsyncLocalBus):
    """
    All handler exceptions should be collected and raised together as an ExceptionGroup.

    Given: Two handlers subscribed to a BoundEvent, both of which raise
    When: an event is emitted
    Then: An ExceptionGroup containing both exceptions should be raised
    """
    # Arrange
    error_a = ValueError("first")
    error_b = RuntimeError("second")
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, AsyncMock(side_effect=error_a))
    bus.subscribe(emit, AsyncMock(side_effect=error_b))
    payload = _TaskCreated(11)

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exc_info:
        await bus.handle_subscribe(payload, config=emit)

    assert set(exc_info.value.exceptions) == {error_a, error_b}


async def test_handler_can_access_envelope_during_dispatch(bus_with_envelope: AsyncLocalBus):
    """
    Handlers should be able to access a valid Envelope during dispatch.

    Given: A handler that captures the current envelope
    When: An event is emitted
    Then: The captured value should be an Envelope instance
    """
    # Arrange
    emit = bus_with_envelope.publish(_TaskCreated)
    captured: list[Envelope] = []

    @bus_with_envelope.subscribe(emit)
    async def _(_: EventSchema) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        captured.append(envelope)

    # Act
    await emit(task_id=1)

    # Assert
    assert len(captured) == 1
    assert isinstance(captured[0], Envelope)


async def test_each_emission_creates_independent_envelope(bus_with_envelope: AsyncLocalBus):
    """
    Each top-level emission should produce an envelope with a unique correlation id.

    Given: A handler that captures the current envelope
    When: Two separate events are emitted
    Then: Each emission should have a distinct correlation id
    """
    # Arrange
    emit = bus_with_envelope.publish(_TaskCreated)
    captured: list[Envelope] = []

    @bus_with_envelope.subscribe(emit)
    async def _(_: EventSchema) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        captured.append(envelope)

    # Act
    await emit(task_id=1)
    await emit(task_id=2)

    # Assert
    assert captured[0].correlation_id != captured[1].correlation_id


async def test_nested_emission_produces_child_envelope(bus_with_envelope: AsyncLocalBus):
    """
    A handler that emits an event should receive a child envelope linked to the outer one.

    Given: An outer handler that emits on a second BoundEvent, and an inner handler on that event
    When: The outer event is emitted
    Then: The inner envelope should share the outer correlation id and
          have the outer message id as its causation id
    """
    # Arrange
    emit_outer = bus_with_envelope.publish(_TaskCreated)
    emit_inner = bus_with_envelope.publish(_TaskCreated)
    outer_envelopes: list[Envelope] = []
    inner_envelopes: list[Envelope] = []

    @bus_with_envelope.subscribe(emit_outer)
    async def _(_: EventSchema) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        outer_envelopes.append(envelope)
        await emit_inner(task_id=99)

    @bus_with_envelope.subscribe(emit_inner)
    async def _(_: EventSchema) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        inner_envelopes.append(envelope)

    # Act
    await emit_outer(task_id=1)

    # Assert
    assert inner_envelopes[0].correlation_id == outer_envelopes[0].correlation_id
    assert inner_envelopes[0].causation_id == outer_envelopes[0].message_id
    assert inner_envelopes[0].message_id != outer_envelopes[0].message_id


async def test_envelope_cleaned_up_after_dispatch(bus_with_envelope: AsyncLocalBus):
    """
    The Envelope should not be accessible after dispatch completes.

    Given: An AsyncLocalBus with a subscribed handler
    When: An event is emitted and dispatch completes
    Then: Accessing the current envelope should return None
    """
    # Arrange
    emit = bus_with_envelope.publish(_TaskCreated)
    bus_with_envelope.subscribe(emit, AsyncMock())

    # Act
    await emit(task_id=1)

    # Assert
    assert Envelope.current() is None
