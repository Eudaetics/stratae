"""
Unit tests for the LocalBus adapter.

This test suite verifies the following behaviors:

LocalBus:
- publish returns a BoundEvent.
- Calling the BoundEvent dispatches the payload to a registered handler.
- All handlers registered on a channel receive the payload.
- Handlers on different channels are isolated from each other.
- subscribe returns a Handler.
- unsubscribe removes a handler; subsequent emits do not invoke it.
- The same callable may be registered multiple times independently.
- emit_publish directly dispatches to handle_subscribe.
- handle_subscribe invokes all handlers registered on the channel.
- A raising handler does not prevent other handlers from running.
- All handler exceptions are collected and re-raised as an ExceptionGroup.

LocalBus (with envelope):
- Handlers can access the EventEnvelope during dispatch.
- Each top-level emission creates an independent envelope.
- A handler that emits an event receives a child envelope.
- The envelope is cleaned up after dispatch completes.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from stratae.events.adapters.local import LocalBus
from stratae.events.envelope import EventEnvelope
from stratae.events.event import BoundEvent, EventSchema


class _TaskCreated(EventSchema):
    def __init__(self, task_id: int) -> None:
        self.task_id = task_id

    def __eq__(self, other: Any):
        if not isinstance(other, _TaskCreated):
            return NotImplemented
        return self.task_id == other.task_id


@pytest.fixture
def bus() -> LocalBus:
    """Return a fresh LocalBus instance with no envelope."""
    return LocalBus()


@pytest.fixture
def bus_with_envelope() -> LocalBus:
    """Return a fresh LocalBus instance with envelope tracking enabled."""
    return LocalBus(use_envelope=True)


def test_publish_returns_bound_event(bus: LocalBus):
    """
    Publish should return a BoundEvent bound to emit_publish.

    Given: A LocalBus
    When: publish is called with a schema
    Then: A BoundEvent should be returned
    """
    assert isinstance(bus.publish(_TaskCreated), BoundEvent)


def test_calling_bound_event_dispatches_to_handler(bus: LocalBus):
    """
    Calling a BoundEvent should dispatch the constructed payload to registered handlers.

    Given: A handler subscribed via a BoundEvent config
    When: The BoundEvent is called
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = Mock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler)

    # Act
    emit(task_id=1)

    # Assert
    handler.assert_called_once_with(_TaskCreated(1))


def test_dispatches_to_all_handlers_on_channel(bus: LocalBus):
    """
    All handlers registered on the same BoundEvent should receive the payload.

    Given: Two handlers subscribed to the same BoundEvent
    When: An event is emitted
    Then: Both handlers should be called with the payload
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler_a)
    bus.subscribe(emit, handler_b)

    # Act
    emit(task_id=2)

    # Assert
    handler_a.assert_called_once_with(_TaskCreated(2))
    handler_b.assert_called_once_with(_TaskCreated(2))


def test_channel_isolation(bus: LocalBus):
    """
    Handlers subscribed to one BoundEvent should not receive events emitted on another.

    Given: Two handlers each subscribed to a different BoundEvent
    When: An event is emitted on one BoundEvent
    Then: Only the handler for that BoundEvent should be called
    """
    # Arrange
    emit_task = bus.publish(_TaskCreated)
    emit_order = bus.publish(_TaskCreated)
    task_handler = Mock()
    order_handler = Mock()
    bus.subscribe(emit_task, task_handler)
    bus.subscribe(emit_order, order_handler)

    # Act
    emit_task(task_id=3)

    # Assert
    task_handler.assert_called_once_with(_TaskCreated(3))
    order_handler.assert_not_called()


def test_subscribe_returns_handler(bus: LocalBus):
    """
    Subscribe should return the Handler wrapping the registered callable.

    Given: A LocalBus
    When: subscribe is called with a callable
    Then: The returned Handler should wrap that callable
    """
    fn = Mock()
    emit = bus.publish(_TaskCreated)
    handle = bus.subscribe(emit, fn)

    assert handle.call is fn


def test_unsubscribe_prevents_further_dispatch(bus: LocalBus):
    """
    Unsubscribed handlers should not receive subsequent emissions.

    Given: A handler subscribed and then unsubscribed
    When: An event is emitted
    Then: The handler should not be called
    """
    # Arrange
    handler = Mock()
    emit = bus.publish(_TaskCreated)
    handle = bus.subscribe(emit, handler)
    bus.unsubscribe(handle)

    # Act
    emit(task_id=4)

    # Assert
    handler.assert_not_called()


def test_same_callable_registered_twice_called_twice(bus: LocalBus):
    """
    Registering the same callable twice should produce two independent subscriptions.

    Given: The same callable subscribed to a BoundEvent twice
    When: An event is emitted
    Then: The callable should be invoked twice
    """
    # Arrange
    handler = Mock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler)
    bus.subscribe(emit, handler)

    # Act
    emit(task_id=5)

    # Assert
    assert handler.call_count == 2


def test_emit_publish_dispatches_directly(bus: LocalBus):
    """
    emit_publish should dispatch the payload directly to all registered handlers.

    Given: A handler subscribed to a BoundEvent
    When: emit_publish is called directly with that BoundEvent
    Then: The handler should receive the payload
    """
    # Arrange
    handler = Mock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler)
    payload = _TaskCreated(6)

    # Act
    bus.emit_publish(payload, emit)

    # Assert
    handler.assert_called_once_with(payload)


def test_handle_subscribe_invokes_all_handlers(bus: LocalBus):
    """
    handle_subscribe should invoke every handler registered for the given BoundEvent.

    Given: Two handlers subscribed to a BoundEvent
    When: handle_subscribe is called directly
    Then: Both handlers should receive the payload
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, handler_a)
    bus.subscribe(emit, handler_b)
    payload = _TaskCreated(7)

    # Act
    bus.handle_subscribe(payload, config=emit)

    # Assert
    handler_a.assert_called_once_with(payload)
    handler_b.assert_called_once_with(payload)


def test_raising_handler_does_not_prevent_other_handlers(bus: LocalBus):
    """
    A handler that raises should not prevent subsequent handlers from running.

    Given: Two handlers subscribed to a BoundEvent, the first of which raises
    When: an event is emitted
    Then: The second handler should still be called
    """
    # Arrange
    second_handler = Mock()
    emit = bus.publish(_TaskCreated)
    bus.subscribe(emit, Mock(side_effect=ValueError("boom")))
    bus.subscribe(emit, second_handler)
    payload = _TaskCreated(10)

    # Act
    with pytest.raises(ExceptionGroup):
        bus.handle_subscribe(payload, config=emit)

    # Assert
    second_handler.assert_called_once_with(payload)


def test_handler_exceptions_collected_into_exception_group(bus: LocalBus):
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
    bus.subscribe(emit, Mock(side_effect=error_a))
    bus.subscribe(emit, Mock(side_effect=error_b))
    payload = _TaskCreated(11)

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exc_info:
        bus.handle_subscribe(payload, config=emit)

    assert set(exc_info.value.exceptions) == {error_a, error_b}


def test_handler_can_access_envelope_during_dispatch(bus_with_envelope: LocalBus):
    """
    Handlers should be able to access a valid EventEnvelope during dispatch.

    Given: A handler that captures the current envelope
    When: An event is emitted
    Then: The captured value should be an EventEnvelope instance
    """
    # Arrange
    emit = bus_with_envelope.publish(_TaskCreated)
    captured: list[EventEnvelope] = []

    def handler(_: EventSchema) -> None:
        envelope = EventEnvelope.current()
        assert envelope is not None
        captured.append(envelope)

    bus_with_envelope.subscribe(emit, handler)

    # Act
    emit(task_id=1)

    # Assert
    assert len(captured) == 1
    assert isinstance(captured[0], EventEnvelope)


def test_each_emission_creates_independent_envelope(bus_with_envelope: LocalBus):
    """
    Each top-level emission should produce an envelope with a unique correlation id.

    Given: A handler that captures the current envelope
    When: Two separate events are emitted
    Then: Each emission should have a distinct correlation id
    """
    # Arrange
    emit = bus_with_envelope.publish(_TaskCreated)
    captured: list[EventEnvelope] = []

    def handler(_: EventSchema) -> None:
        envelope = EventEnvelope.current()
        assert envelope is not None
        captured.append(envelope)

    bus_with_envelope.subscribe(emit, handler)

    # Act
    emit(task_id=1)
    emit(task_id=2)

    # Assert
    assert captured[0].correlation_id != captured[1].correlation_id


def test_nested_emission_produces_child_envelope(bus_with_envelope: LocalBus):
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
    outer_envelopes: list[EventEnvelope] = []
    inner_envelopes: list[EventEnvelope] = []

    @bus_with_envelope.subscribe(emit_outer)
    def _(_: EventSchema) -> None:
        envelope = EventEnvelope.current()
        assert envelope is not None
        outer_envelopes.append(envelope)
        emit_inner(task_id=99)

    @bus_with_envelope.subscribe(emit_inner)
    def _(_: EventSchema) -> None:
        envelope = EventEnvelope.current()
        assert envelope is not None
        inner_envelopes.append(envelope)

    # Act
    emit_outer(task_id=1)

    # Assert
    assert inner_envelopes[0].correlation_id == outer_envelopes[0].correlation_id
    assert inner_envelopes[0].causation_id == outer_envelopes[0].message_id
    assert inner_envelopes[0].message_id != outer_envelopes[0].message_id


def test_envelope_cleaned_up_after_dispatch(bus_with_envelope: LocalBus):
    """
    The EventEnvelope should not be accessible after dispatch completes.

    Given: A LocalBus with a subscribed handler
    When: An event is emitted and dispatch completes
    Then: Accessing the current envelope should return None
    """
    # Arrange
    emit = bus_with_envelope.publish(_TaskCreated)

    @bus_with_envelope.subscribe(emit)
    def _(_: _TaskCreated) -> None: ...

    # Act
    emit(task_id=1)

    # Assert
    assert EventEnvelope.current() is None
