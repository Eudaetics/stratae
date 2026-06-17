"""
Unit tests for the LocalBus adapter.

This test suite verifies the following behaviors:

LocalBus:
- bind returns a BoundEvent.
- Calling the BoundEvent dispatches the payload to a registered handler.
- All handlers registered on a channel receive the payload.
- Handlers on different channels are isolated from each other.
- handle returns a Handler.
- remove removes a handler; subsequent emits do not invoke it.
- The same callable may be registered multiple times independently.
- emit dispatches the payload to all registered handlers.
- A raising handler does not prevent other handlers from running.
- All handler exceptions are collected and re-raised as an ExceptionGroup.

LocalBus (with envelope):
- Handlers can access the Envelope during dispatch.
- Each top-level emission creates an independent envelope.
- A handler that emits an event receives a child envelope.
- The envelope is cleaned up after dispatch completes.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from stratae.events.adapters.direct import DirectBus
from stratae.events.bound import BoundEvent
from stratae.events.envelope import Envelope
from stratae.events.event import EventConfig, Payload, PubSub


class _TaskCreated(Payload):
    def __init__(self, task_id: int) -> None:
        self.task_id = task_id

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, _TaskCreated):
            return NotImplemented
        return self.task_id == other.task_id


_task_created = EventConfig(_TaskCreated, PubSub)


@pytest.fixture
def bus() -> DirectBus:
    """Return a fresh LocalBus instance with no envelope."""
    return DirectBus()


@pytest.fixture
def bus_with_envelope() -> DirectBus:
    """Return a fresh LocalBus instance with envelope tracking enabled."""
    return DirectBus(use_envelope=True)


def test_bind_returns_bound_event(bus: DirectBus):
    """
    ``bind`` should return a BoundEvent bound to bus.emit.

    Given: A LocalBus
    When: bind is called with an EventConfig
    Then: A BoundEvent should be returned
    """
    assert isinstance(bus.bind(_task_created), BoundEvent)


def test_calling_bound_event_dispatches_to_handler(bus: DirectBus):
    """
    Calling a BoundEvent should dispatch the constructed payload to registered handlers.

    Given: A handler registered via bus.handle
    When: The BoundEvent is called
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created)
    bus.handle(emit.event, handler)

    # Act
    emit(task_id=1)

    # Assert
    handler.assert_called_once_with(_TaskCreated(1))


def test_dispatches_to_all_handlers_on_channel(bus: DirectBus):
    """
    All handlers registered on the same EventConfig should receive the payload.

    Given: Two handlers registered to the same EventConfig
    When: An event is emitted
    Then: Both handlers should be called with the payload
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()
    emit = bus.bind(_task_created)
    bus.handle(emit.event, handler_a)
    bus.handle(emit.event, handler_b)

    # Act
    emit(task_id=2)

    # Assert
    handler_a.assert_called_once_with(_TaskCreated(2))
    handler_b.assert_called_once_with(_TaskCreated(2))


def test_channel_isolation(bus: DirectBus):
    """
    Handlers registered to one EventConfig should not receive events emitted on another.

    Given: Two handlers each registered to a different EventConfig
    When: An event is emitted on one EventConfig
    Then: Only the handler for that EventConfig should be called
    """
    # Arrange
    task_event = EventConfig(_TaskCreated, PubSub)
    order_event = EventConfig(_TaskCreated, PubSub)
    emit_task = bus.bind(task_event)
    task_handler = Mock()
    order_handler = Mock()
    bus.handle(task_event, task_handler)
    bus.handle(order_event, order_handler)

    # Act
    emit_task(task_id=3)

    # Assert
    task_handler.assert_called_once_with(_TaskCreated(3))
    order_handler.assert_not_called()


def test_handle_returns_handler(bus: DirectBus):
    """
    ``handle`` should return the Handler wrapping the registered callable.

    Given: A LocalBus
    When: handle is called with a callable
    Then: The returned Handler should wrap that callable
    """
    fn = Mock()
    emit = bus.bind(_task_created)
    handle = bus.handle(emit.event, fn)

    assert handle.call is fn


def test_remove_prevents_further_dispatch(bus: DirectBus):
    """
    Removed handlers should not receive subsequent emissions.

    Given: A handler registered and then removed
    When: An event is emitted
    Then: The handler should not be called
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created)
    handle = bus.handle(emit.event, handler)
    bus.remove(handle)

    # Act
    emit(task_id=4)

    # Assert
    handler.assert_not_called()


def test_same_callable_registered_twice_called_twice(bus: DirectBus):
    """
    Registering the same callable twice should produce two independent registrations.

    Given: The same callable registered to an EventConfig twice
    When: An event is emitted
    Then: The callable should be invoked twice
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created)
    bus.handle(emit.event, handler)
    bus.handle(emit.event, handler)

    # Act
    emit(task_id=5)

    # Assert
    assert handler.call_count == 2


def test_emit_dispatches_payload(bus: DirectBus):
    """
    ``emit`` should dispatch the payload to registered handlers.

    Given: A handler registered to an EventConfig
    When: emit is called with that EventConfig
    Then: The handler should receive the payload
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created)
    bus.handle(emit.event, handler)
    payload = _TaskCreated(6)

    # Act
    bus.emit(payload, emit.event, None)

    # Assert
    handler.assert_called_once_with(payload)


def test_emit_invokes_all_handlers(bus: DirectBus):
    """
    ``emit`` should invoke every handler registered for the given EventConfig.

    Given: Two handlers registered to an EventConfig
    When: emit is called directly
    Then: Both handlers should receive the payload
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()
    emit = bus.bind(_task_created)
    bus.handle(emit.event, handler_a)
    bus.handle(emit.event, handler_b)
    payload = _TaskCreated(7)

    # Act
    bus.emit(payload, emit.event)

    # Assert
    handler_a.assert_called_once_with(payload)
    handler_b.assert_called_once_with(payload)


def test_raising_handler_does_not_prevent_other_handlers(bus: DirectBus):
    """
    A handler that raises should not prevent subsequent handlers from running.

    Given: Two handlers registered to an EventConfig, the first of which raises
    When: an event is emitted
    Then: The second handler should still be called
    """
    # Arrange
    second_handler = Mock()
    emit = bus.bind(_task_created)
    bus.handle(emit.event, Mock(side_effect=ValueError("boom")))
    bus.handle(emit.event, second_handler)
    payload = _TaskCreated(10)

    # Act
    with pytest.raises(ExceptionGroup):
        bus.emit(payload, emit.event)

    # Assert
    second_handler.assert_called_once_with(payload)


def test_handler_exceptions_collected_into_exception_group(bus: DirectBus):
    """
    All handler exceptions should be collected and raised together as an ExceptionGroup.

    Given: Two handlers registered to an EventConfig, both of which raise
    When: an event is emitted
    Then: An ExceptionGroup containing both exceptions should be raised
    """
    # Arrange
    error_a = ValueError("first")
    error_b = RuntimeError("second")
    emit = bus.bind(_task_created)
    bus.handle(emit.event, Mock(side_effect=error_a))
    bus.handle(emit.event, Mock(side_effect=error_b))
    payload = _TaskCreated(11)

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exc_info:
        bus.emit(payload, emit.event)

    assert set(exc_info.value.exceptions) == {error_a, error_b}


def test_handler_can_access_envelope_during_dispatch(bus_with_envelope: DirectBus):
    """
    Handlers should be able to access a valid Envelope during dispatch.

    Given: A handler that captures the current envelope
    When: An event is emitted
    Then: The captured value should be an Envelope instance
    """
    # Arrange
    emit = bus_with_envelope.bind(_task_created)
    captured: list[Envelope] = []

    def handler(_: Payload) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        captured.append(envelope)

    bus_with_envelope.handle(emit.event, handler)

    # Act
    emit(task_id=1)

    # Assert
    assert len(captured) == 1
    assert isinstance(captured[0], Envelope)


def test_each_emission_creates_independent_envelope(bus_with_envelope: DirectBus):
    """
    Each top-level emission should produce an envelope with a unique correlation id.

    Given: A handler that captures the current envelope
    When: Two separate events are emitted
    Then: Each emission should have a distinct correlation id
    """
    # Arrange
    emit = bus_with_envelope.bind(_task_created)
    captured: list[Envelope] = []

    def handler(_: Payload) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        captured.append(envelope)

    bus_with_envelope.handle(emit.event, handler)

    # Act
    emit(task_id=1)
    emit(task_id=2)

    # Assert
    assert captured[0].correlation_id != captured[1].correlation_id


def test_nested_emission_produces_child_envelope(bus_with_envelope: DirectBus):
    """
    A handler that emits an event should receive a child envelope linked to the outer one.

    Given: An outer handler that emits on a second EventConfig, and an inner handler on that event
    When: The outer event is emitted
    Then: The inner envelope should share the outer correlation id and
          have the outer message id as its causation id
    """
    # Arrange
    outer_event = EventConfig(_TaskCreated, PubSub)
    inner_event = EventConfig(_TaskCreated, PubSub)
    emit_outer = bus_with_envelope.bind(outer_event)
    emit_inner = bus_with_envelope.bind(inner_event)
    outer_envelopes: list[Envelope] = []
    inner_envelopes: list[Envelope] = []

    @bus_with_envelope.handle(outer_event)
    def _(_: Payload) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        outer_envelopes.append(envelope)
        emit_inner(task_id=99)

    @bus_with_envelope.handle(inner_event)
    def _(_: Payload) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        inner_envelopes.append(envelope)

    # Act
    emit_outer(task_id=1)

    # Assert
    assert inner_envelopes[0].correlation_id == outer_envelopes[0].correlation_id
    assert inner_envelopes[0].causation_id == outer_envelopes[0].message_id
    assert inner_envelopes[0].message_id != outer_envelopes[0].message_id


def test_envelope_cleaned_up_after_dispatch(bus_with_envelope: DirectBus):
    """
    The Envelope should not be accessible after dispatch completes.

    Given: A LocalBus with a registered handler
    When: An event is emitted and dispatch completes
    Then: Accessing the current envelope should return None
    """
    # Arrange
    emit = bus_with_envelope.bind(_task_created)

    @bus_with_envelope.handle(emit.event)
    def _(_: _TaskCreated) -> None: ...

    # Act
    emit(task_id=1)

    # Assert
    assert Envelope.current() is None
