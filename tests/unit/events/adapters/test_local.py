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
- Handlers can access the EventEnvelope during dispatch.
- Each top-level emission creates an independent envelope.
- A handler that emits an event receives a child envelope.
- The envelope is cleaned up after dispatch completes.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from stratae.events.adapters.local import LocalBus
from stratae.events.channel import Channel
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
    """Return a fresh LocalBus instance."""
    return LocalBus()


@pytest.fixture
def channel() -> Channel:
    """Return a Channel for use in tests."""
    return Channel("tasks")


def test_publish_returns_bound_event(bus: LocalBus, channel: Channel):
    """
    Publish should return a BoundEvent bound to emit_publish.

    Given: A LocalBus and a channel
    When: publish is called with a schema
    Then: A BoundEvent should be returned
    """
    assert isinstance(bus.publish(channel, _TaskCreated), BoundEvent)


def test_calling_bound_event_dispatches_to_handler(bus: LocalBus, channel: Channel):
    """
    Calling a BoundEvent should dispatch the constructed payload to registered handlers.

    Given: A handler subscribed to a channel
    When: The BoundEvent for that channel is called
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = Mock()
    bus.subscribe(channel, handler)
    emit = bus.publish(channel, _TaskCreated)

    # Act
    emit(task_id=1)

    # Assert
    handler.assert_called_once_with(_TaskCreated(1))


def test_dispatches_to_all_handlers_on_channel(bus: LocalBus, channel: Channel):
    """
    All handlers registered on a channel should receive the payload.

    Given: Two handlers subscribed to the same channel
    When: An event is emitted on that channel
    Then: Both handlers should be called with the payload
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()
    bus.subscribe(channel, handler_a)
    bus.subscribe(channel, handler_b)
    emit = bus.publish(channel, _TaskCreated)

    # Act
    emit(task_id=2)

    # Assert
    handler_a.assert_called_once_with(_TaskCreated(2))
    handler_b.assert_called_once_with(_TaskCreated(2))


def test_channel_isolation(bus: LocalBus):
    """
    Handlers on one channel should not receive events emitted on another channel.

    Given: Two handlers each subscribed to a different channel
    When: An event is emitted on one channel
    Then: Only the handler on that channel should be called
    """
    # Arrange
    tasks = Channel("tasks")
    orders = Channel("orders")
    task_handler = Mock()
    order_handler = Mock()
    bus.subscribe(tasks, task_handler)
    bus.subscribe(orders, order_handler)
    emit_task = bus.publish(tasks, _TaskCreated)

    # Act
    emit_task(task_id=3)

    # Assert
    task_handler.assert_called_once_with(_TaskCreated(3))
    order_handler.assert_not_called()


def test_subscribe_returns_handler(bus: LocalBus, channel: Channel):
    """
    Subscribe should return the Handler wrapping the registered callable.

    Given: A LocalBus and a channel
    When: subscribe is called with a callable
    Then: The returned Handler should wrap that callable
    """
    fn = Mock()
    handle = bus.subscribe(channel, fn)

    assert handle.call is fn


def test_unsubscribe_prevents_further_dispatch(bus: LocalBus, channel: Channel):
    """
    Unsubscribed handlers should not receive subsequent emissions.

    Given: A handler subscribed and then unsubscribed from a channel
    When: An event is emitted on that channel
    Then: The handler should not be called
    """
    # Arrange
    handler = Mock()
    handle = bus.subscribe(channel, handler)
    bus.unsubscribe(channel, handle)
    emit = bus.publish(channel, _TaskCreated)

    # Act
    emit(task_id=4)

    # Assert
    handler.assert_not_called()


def test_same_callable_registered_twice_called_twice(bus: LocalBus, channel: Channel):
    """
    Registering the same callable twice should produce two independent subscriptions.

    Given: The same callable subscribed to a channel twice
    When: An event is emitted on that channel
    Then: The callable should be invoked twice
    """
    # Arrange
    handler = Mock()
    bus.subscribe(channel, handler)
    bus.subscribe(channel, handler)
    emit = bus.publish(channel, _TaskCreated)

    # Act
    emit(task_id=5)

    # Assert
    assert handler.call_count == 2


def test_emit_publish_dispatches_directly(bus: LocalBus, channel: Channel):
    """
    emit_publish should dispatch the payload directly to all registered handlers.

    Given: A handler subscribed to a channel
    When: emit_publish is called directly
    Then: The handler should receive the payload
    """
    # Arrange
    handler = Mock()
    bus.subscribe(channel, handler)
    payload = _TaskCreated(6)

    # Act
    bus.emit_publish(channel, payload, meta=None)

    # Assert
    handler.assert_called_once_with(payload)


def test_handle_subscribe_invokes_all_handlers(bus: LocalBus, channel: Channel):
    """
    handle_subscribe should invoke every handler registered on the channel.

    Given: Two handlers subscribed to a channel
    When: handle_subscribe is called directly
    Then: Both handlers should receive the payload
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()
    bus.subscribe(channel, handler_a)
    bus.subscribe(channel, handler_b)
    payload = _TaskCreated(7)

    # Act
    bus.handle_subscribe(channel, payload, meta=None)

    # Assert
    handler_a.assert_called_once_with(payload)
    handler_b.assert_called_once_with(payload)


def test_handler_can_access_envelope_during_dispatch(bus: LocalBus, channel: Channel):
    """
    Handlers should be able to access a valid EventEnvelope during dispatch.

    Given: A handler that captures the current envelope
    When: An event is emitted
    Then: The captured value should be an EventEnvelope instance
    """
    # Arrange
    captured: list[EventEnvelope] = []

    def handler(_payload: EventSchema) -> None:
        captured.append(EventEnvelope.current())

    bus.subscribe(channel, handler)

    # Act
    bus.publish(channel, _TaskCreated)(task_id=1)

    # Assert
    assert len(captured) == 1
    assert isinstance(captured[0], EventEnvelope)


def test_each_emission_creates_independent_envelope(bus: LocalBus, channel: Channel):
    """
    Each top-level emission should produce an envelope with a unique correlation id.

    Given: A handler that captures the current envelope
    When: Two separate events are emitted
    Then: Each emission should have a distinct correlation id
    """
    # Arrange
    captured: list[EventEnvelope] = []

    def handler(_payload: EventSchema) -> None:
        captured.append(EventEnvelope.current())

    bus.subscribe(channel, handler)
    emit = bus.publish(channel, _TaskCreated)

    # Act
    emit(task_id=1)
    emit(task_id=2)

    # Assert
    assert captured[0].correlation_id != captured[1].correlation_id


def test_nested_emission_produces_child_envelope(bus: LocalBus):
    """
    A handler that emits an event should receive a child envelope linked to the outer one.

    Given: An outer handler that emits on a second channel, and an inner handler on that channel
    When: The outer event is emitted
    Then: The inner envelope should share the outer correlation id and
          have the outer message id as its causation id
    """
    # Arrange
    outer_channel = Channel("outer")
    inner_channel = Channel("inner")
    outer_envelopes: list[EventEnvelope] = []
    inner_envelopes: list[EventEnvelope] = []
    emit_inner = bus.publish(inner_channel, _TaskCreated)

    def outer_handler(_payload: EventSchema) -> None:
        outer_envelopes.append(EventEnvelope.current())
        emit_inner(task_id=99)

    def inner_handler(_payload: EventSchema) -> None:
        inner_envelopes.append(EventEnvelope.current())

    bus.subscribe(outer_channel, outer_handler)
    bus.subscribe(inner_channel, inner_handler)

    # Act
    bus.publish(outer_channel, _TaskCreated)(task_id=1)

    # Assert
    assert inner_envelopes[0].correlation_id == outer_envelopes[0].correlation_id
    assert inner_envelopes[0].causation_id == outer_envelopes[0].message_id
    assert inner_envelopes[0].message_id != outer_envelopes[0].message_id


def test_envelope_cleaned_up_after_dispatch(bus: LocalBus, channel: Channel):
    """
    The EventEnvelope should not be accessible after dispatch completes.

    Given: A LocalBus with a subscribed handler
    When: An event is emitted and dispatch completes
    Then: Accessing the current envelope should raise LookupError
    """
    # Arrange
    bus.subscribe(channel, lambda _payload: None)

    # Act
    bus.publish(channel, _TaskCreated)(task_id=1)

    # Assert
    with pytest.raises(LookupError):
        EventEnvelope.current()
