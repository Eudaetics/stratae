"""
Unit tests for the DirectBus adapter.

This test suite verifies the following behaviors:

DirectBus:
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
- A handler that removes a registration during dispatch does not prevent
  other handlers on the same emission from running.

DirectBus (request events):
- emit returns the responder's reply.
- A BoundEvent for a request event returns the reply when called.
- emit raises NoResponderError when no responder is registered.
- emit raises MultipleRespondersError when several responders are registered.
- A responder's exception propagates directly, not as an ExceptionGroup.
- A responder may be registered via the decorator form of handle.

DirectBus (with envelope):
- Handlers can access the Envelope during dispatch.
- Each top-level emission creates an independent envelope.
- A handler that emits an event receives a child envelope.
- The envelope is cleaned up after dispatch completes.
- A request reply is returned when envelope tracking is enabled.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from stratae.events.adapters.direct import DirectBus
from stratae.events.bound import BoundEvent
from stratae.events.envelope import Envelope
from stratae.events.event import EventConfig, PubSub, Request
from stratae.events.exceptions import MultipleRespondersError, NoResponderError
from stratae.events.protocols import Consumer, EmitCallable, Producer


class _TaskCreated:
    def __init__(self, task_id: int) -> None:
        self.task_id = task_id

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, _TaskCreated):
            return NotImplemented
        return self.task_id == other.task_id


_task_created = EventConfig(_TaskCreated, PubSub)


class _BookQuery:
    def __init__(self, query: str) -> None:
        self.query = query


class _BookResult:
    def __init__(self, title: str) -> None:
        self.title = title


_find_book = EventConfig(_BookQuery, Request[_BookResult])


@pytest.fixture
def bus() -> DirectBus:
    """Return a fresh DirectBus instance with no envelope."""
    return DirectBus()


@pytest.fixture
def bus_with_envelope() -> DirectBus:
    """Return a fresh DirectBus instance with envelope tracking enabled."""
    return DirectBus(use_envelope=True)


def test_bus_satisfies_producer_protocol(bus: DirectBus):
    """
    DirectBus should satisfy the Producer protocol.

    Given: A DirectBus
    When: checked against the Producer protocol
    Then: isinstance should return True
    """
    assert isinstance(bus, Producer)


def test_bus_emit_satisfies_emit_callable_protocol(bus: DirectBus):
    """
    DirectBus.emit should satisfy the EmitCallable protocol.

    Given: A DirectBus
    When: its bound emit method is checked against the EmitCallable protocol
    Then: isinstance should return True
    """
    assert isinstance(bus.emit, EmitCallable)


def test_bus_satisfies_consumer_protocol(bus: DirectBus):
    """
    DirectBus should satisfy the Consumer protocol.

    Given: A DirectBus
    When: checked against the Consumer protocol
    Then: isinstance should return True
    """
    assert isinstance(bus, Consumer)


def test_bind_returns_bound_event(bus: DirectBus):
    """
    ``bind`` should return a BoundEvent bound to bus.emit.

    Given: A DirectBus
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

    Given: A DirectBus
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


def test_handler_removing_registration_during_dispatch_does_not_break_other_handlers(
    bus: DirectBus,
):
    """
    A removed handler mid-dispatch should not prevent other handlers from running.

    Handlers are stored in a set, so iteration order is not guaranteed; several other
    handlers are registered alongside the self-removing to reduce the chances of degenerate
    ordering causing the test to fail.

    Given: A self-removing handler and several other handlers registered to an EventConfig
    When: an event is emitted
    Then: Every other handler should still be called, and no error should be raised
    """
    # Arrange
    other_handlers = [Mock() for _ in range(20)]
    emit = bus.bind(_task_created)

    def self_removing(_: object) -> None:
        bus.remove(first_handle)

    first_handle = bus.handle(emit.event, self_removing)
    for handler in other_handlers:
        bus.handle(emit.event, handler)
    payload = _TaskCreated(12)

    # Act
    bus.emit(payload, emit.event)

    # Assert
    for handler in other_handlers:
        handler.assert_called_once_with(payload)


def test_request_emit_returns_responder_reply(bus: DirectBus):
    """
    ``emit`` should return the responder's reply for a request event.

    Given: A responder registered to a request EventConfig
    When: emit is called with that EventConfig
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus.handle(_find_book, Mock(return_value=reply))
    payload = _BookQuery("dune")

    # Act
    result = bus.emit(payload, _find_book)

    # Assert
    assert result is reply


def test_bound_request_event_returns_reply(bus: DirectBus):
    """
    Calling a BoundEvent for a request event should return the responder's reply.

    Given: A responder registered to a request EventConfig bound via bus.bind
    When: The BoundEvent is called
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    find_book = bus.bind(_find_book)
    bus.handle(find_book.event, Mock(return_value=reply))

    # Act
    result = find_book(query="dune")

    # Assert
    assert result is reply


def test_request_emit_raises_without_responder(bus: DirectBus):
    """
    ``emit`` should raise NoResponderError when a request event has no responder.

    Given: A request EventConfig with no registered responder
    When: emit is called with that EventConfig
    Then: A NoResponderError should be raised
    """
    with pytest.raises(NoResponderError):
        bus.emit(_BookQuery("dune"), _find_book)


def test_request_emit_raises_with_multiple_responders(bus: DirectBus):
    """
    ``emit`` should raise MultipleRespondersError when several responders are registered.

    Given: Two responders registered to a request EventConfig
    When: emit is called with that EventConfig
    Then: A MultipleRespondersError should be raised
    """
    # Arrange
    bus.handle(_find_book, Mock(return_value=_BookResult("first")))
    bus.handle(_find_book, Mock(return_value=_BookResult("second")))

    # Act / Assert
    with pytest.raises(MultipleRespondersError):
        bus.emit(_BookQuery("dune"), _find_book)


def test_request_responder_exception_propagates_directly(bus: DirectBus):
    """
    A responder's exception should propagate to the emitter unwrapped.

    Given: A responder that raises, registered to a request EventConfig
    When: emit is called with that EventConfig
    Then: The responder's exception should be raised directly, not as an ExceptionGroup
    """
    # Arrange
    error = ValueError("boom")
    bus.handle(_find_book, Mock(side_effect=error))

    # Act / Assert
    with pytest.raises(ValueError) as exc_info:
        bus.emit(_BookQuery("dune"), _find_book)

    assert exc_info.value is error


def test_request_responder_registered_via_decorator(bus: DirectBus):
    """
    A responder registered via the decorator form of handle should serve requests.

    Given: A responder registered with @bus.handle on a request EventConfig
    When: emit is called with that EventConfig
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")

    @bus.handle(_find_book)
    def _(_: _BookQuery) -> _BookResult:
        return reply

    # Act
    result = bus.emit(_BookQuery("dune"), _find_book)

    # Assert
    assert result is reply


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

    def handler(_: object) -> None:
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

    def handler(_: object) -> None:
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
    def _(_: object) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        outer_envelopes.append(envelope)
        emit_inner(task_id=99)

    @bus_with_envelope.handle(inner_event)
    def _(_: object) -> None:
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

    Given: A DirectBus with a registered handler
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


def test_request_reply_returned_with_envelope(bus_with_envelope: DirectBus):
    """
    A request event should return its reply when envelope tracking is enabled.

    Given: A responder registered to a request EventConfig on an envelope-tracking bus
    When: emit is called with that EventConfig
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus_with_envelope.handle(_find_book, Mock(return_value=reply))

    # Act
    result = bus_with_envelope.emit(_BookQuery("dune"), _find_book)

    # Assert
    assert result is reply
