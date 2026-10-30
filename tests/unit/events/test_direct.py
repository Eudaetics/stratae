"""
Unit tests for the DirectBus adapter.

This test suite verifies the following behaviors:

DirectBus:
- Calling a factory-bound callable dispatches the constructed payload to a registered handler.
- All handlers registered on a channel receive the payload.
- Handlers on different channels are isolated from each other.
- handle returns a Handler.
- Async handlers are rejected with TypeError at registration, in both the
  direct and decorator forms of handle.
- remove removes a handler; subsequent emits do not invoke it.
- handlers returns an empty tuple for an event with no handlers.
- handlers returns every handler registered for an event.
- handlers excludes a removed handler.
- handlers returns a schema-less event's handlers.
- handlers returns a snapshot that later registrations do not change.
- The same callable may be registered multiple times independently.
- emit dispatches the payload to all registered handlers.
- A raising handler does not prevent other handlers from running.
- All handler exceptions are collected and re-raised as an ExceptionGroup.
- A handler that removes a registration during dispatch does not prevent
  other handlers on the same emission from running.
- A schema-less PubSub event's handler is called with no arguments.

DirectBus (request events):
- emit returns the responder's reply.
- A callable bound with a factory for a request event returns the reply when called.
- emit raises NoResponderError when no responder is registered.
- emit raises MultipleRespondersError when several responders are registered.
- A responder's exception propagates directly, not as an ExceptionGroup.
- A responder may be registered via the decorator form of handle.
- A schema-less Request event's responder is called with no arguments.

DirectBus (dispatchers):
- A custom pattern is dispatched through a dispatcher passed with dispatchers=.
- dispatchers= replaces a default pattern's dispatcher.
- A subclass of a supported pattern is dispatched as that pattern.
- An unsupported pattern raises TypeError at handle, at bind, and at emit.

DirectBus (hooks):
- Hooks passed to bind run around each emit and see a request's reply.
- Hooks passed to emit run once around a pub/sub emit; hooks passed to handle run around
  their own handler only.
- Hooks passed to handle see a responder's failure.
- A payload-less event's hooks receive a NoPayload instance.
- Hooks run inside the emission's envelope.
- Async hooks raise TypeError at handle, at bind, and at emit.
- An observing hook returning an awaitable is logged; the reply returns and complete runs.

DirectBus (middleware):
- Publish middleware runs once around each emit, outside its hooks.
- Handle middleware runs around each handler, outside its hooks, and sees the reply.
- Publish middleware runs inside the emission's envelope.
- Handle middleware raising on entry refuses the handler call.
- A KeyboardInterrupt a handle middleware tries to swallow stops fan-out.
- An async context manager class is refused as middleware when the bus is built.
- An unsupported pattern emitted directly fails before any middleware runs.
- A call object a middleware keeps doesn't keep the bus alive.

DirectBus (with envelope):
- Handlers can access the Envelope during dispatch.
- Each top-level emission creates an independent envelope.
- A handler that emits an event receives a child envelope.
- The envelope is cleaned up after dispatch completes.
- A request reply is returned when envelope tracking is enabled.
"""

import gc
import weakref
from contextlib import contextmanager
from typing import Any, Collection, Generator, Literal, cast
from unittest.mock import Mock

import pytest

from stratae.events import (
    Consumer,
    DirectBus,
    Dispatch,
    EmitCallable,
    Envelope,
    HandleCall,
    Handler,
    Hooks,
    Immutable,
    Middleware,
    NoPayload,
    Producer,
    PublishCall,
    PubSub,
    Request,
)
from stratae.events.exceptions import (
    MultipleRespondersError,
    NoResponderError,
)


class _TaskCreated:
    def __init__(self, task_id: int) -> None:
        self.task_id = task_id

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, _TaskCreated):
            return NotImplemented
        return self.task_id == other.task_id


_task_created = PubSub(_TaskCreated)


class _BookQuery:
    def __init__(self, query: str) -> None:
        self.query = query


class _BookResult:
    def __init__(self, title: str) -> None:
        self.title = title


_find_book = Request(_BookQuery, reply=_BookResult)

_heartbeat = PubSub(name="heartbeat")
_ping = Request(reply=str, name="ping")


class _Gather[S, R](Immutable):
    """A custom pattern: every handler replies, and the emitter gets every reply."""

    __slots__ = ("emit_returns", "handler_returns", "is_signal", "name", "schema")

    name: str
    schema: type[S]
    is_signal: Literal[False]
    emit_returns: type[list[R]]
    handler_returns: type[R]

    def __init__(self, schema: type[S], *, reply: type[R]) -> None:
        self._assign(
            name=schema.__name__,
            schema=schema,
            is_signal=False,
            emit_returns=cast(type[list[R]], list),
            handler_returns=reply,
        )


def _gather_replies[S, R](
    event: _Gather[S, R], handlers: Collection[Handler[[S], Any, R]], payload: S
) -> list[R]:
    """Dispatch a _Gather event: call every handler and collect the replies."""
    return [handler(payload) for handler in handlers]


class _Command(Request[_BookQuery, _BookResult, Literal[False]]):
    """A subclass of a supported pattern, with no dispatcher of its own."""

    __slots__ = ()


_quotes = _Gather(_BookQuery, reply=int)


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


def test_calling_bound_event_forwards_payload_to_handler(bus: DirectBus):
    """
    Calling a callable bound with no factory should dispatch an already-built payload.

    Given: A handler registered via bus.handle, and a callable bound with no factory
    When: The callable is called with a ready-made payload
    Then: The handler should be called with that payload
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created)
    bus.handle(_task_created, handler)

    # Act
    emit(_TaskCreated(1))

    # Assert
    handler.assert_called_once_with(_TaskCreated(1))


def test_calling_factory_bound_event_dispatches_to_handler(bus: DirectBus):
    """
    Calling a callable bound with a factory should dispatch the constructed payload.

    Given: A handler registered via bus.handle
    When: The callable bound with a factory is called
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    bus.handle(_task_created, handler)

    # Act
    emit(task_id=1)

    # Assert
    handler.assert_called_once_with(_TaskCreated(1))


def test_dispatches_to_all_handlers_on_channel(bus: DirectBus):
    """
    All handlers registered on the same event should receive the payload.

    Given: Two handlers registered to the same event
    When: An event is emitted
    Then: Both handlers should be called with the payload
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    bus.handle(_task_created, handler_a)
    bus.handle(_task_created, handler_b)

    # Act
    emit(task_id=2)

    # Assert
    handler_a.assert_called_once_with(_TaskCreated(2))
    handler_b.assert_called_once_with(_TaskCreated(2))


def test_channel_isolation(bus: DirectBus):
    """
    Handlers registered to one event should not receive events emitted on another.

    Given: Two handlers each registered to a different event
    When: An event is emitted on one event
    Then: Only the handler for that event should be called
    """
    # Arrange
    task_event = PubSub(_TaskCreated)
    order_event = PubSub(_TaskCreated)
    emit_task = bus.bind(task_event, factory=_TaskCreated)
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
    handle = bus.handle(_task_created, fn)

    assert handle.call is fn


def test_async_handler_rejected_at_registration(bus: DirectBus):
    """
    Registering an async handler directly should raise TypeError.

    Given: An async callable
    When: handle is called with it and an event
    Then: A TypeError should be raised
    """

    # Arrange
    async def handler(_: _TaskCreated) -> None: ...

    # Act / Assert
    with pytest.raises(TypeError):
        bus.handle(_task_created, handler)


def test_async_handler_rejected_in_decorator_form(bus: DirectBus):
    """
    The decorator form of handle should reject an async handler.

    Given: A decorator produced by handle without a callable
    When: The decorator is applied to an async callable
    Then: A TypeError should be raised
    """
    # Arrange
    decorator = bus.handle(_task_created)

    async def handler(_: _TaskCreated) -> None: ...

    # Act / Assert
    with pytest.raises(TypeError):
        decorator(handler)


def test_remove_prevents_further_dispatch(bus: DirectBus):
    """
    Removed handlers should not receive subsequent emissions.

    Given: A handler registered and then removed
    When: An event is emitted
    Then: The handler should not be called
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    handle = bus.handle(_task_created, handler)
    bus.remove(handle)

    # Act
    emit(task_id=4)

    # Assert
    handler.assert_not_called()


def test_handlers_empty_for_event_without_handlers(bus: DirectBus):
    """
    ``handlers`` should return an empty tuple for an event nothing handles.

    Given: A DirectBus with no handlers registered
    When: handlers is called for an event
    Then: An empty tuple should be returned
    """
    # Act
    result = bus.handlers(_task_created)

    # Assert
    assert result == ()


def test_handlers_returns_registered_handlers(bus: DirectBus):
    """
    ``handlers`` should return every handler registered for an event.

    Given: Two handlers registered to the same event
    When: handlers is called for that event
    Then: Both Handlers should be returned
    """
    # Arrange
    handle_a = bus.handle(_task_created, Mock())
    handle_b = bus.handle(_task_created, Mock())

    # Act
    result = bus.handlers(_task_created)

    # Assert
    assert set(result) == {handle_a, handle_b}


def test_handlers_excludes_removed_handler(bus: DirectBus):
    """
    ``handlers`` should not return a handler after it is removed.

    Given: Two handlers registered to the same event, one of them then removed
    When: handlers is called for that event
    Then: Only the remaining Handler should be returned
    """
    # Arrange
    kept = bus.handle(_task_created, Mock())
    removed = bus.handle(_task_created, Mock())
    bus.remove(removed)

    # Act
    result = bus.handlers(_task_created)

    # Assert
    assert result == (kept,)


def test_handlers_returns_signal_event_handlers(bus: DirectBus):
    """
    ``handlers`` should return the handlers of a schema-less event.

    Given: A responder registered to a schema-less Request event
    When: handlers is called for that event
    Then: The responder's Handler should be returned
    """
    # Arrange
    responder = bus.handle(_ping, Mock(return_value="pong"))

    # Act
    result = bus.handlers(_ping)

    # Assert
    assert result == (responder,)


def test_handlers_snapshot_unchanged_by_later_registration(bus: DirectBus):
    """
    ``handlers`` should return a snapshot that later registrations don't change.

    Given: The handlers of an event, looked up while it has one handler
    When: Another handler is registered to that event
    Then: The earlier result should still hold only the first Handler
    """
    # Arrange
    first = bus.handle(_task_created, Mock())
    result = bus.handlers(_task_created)

    # Act
    bus.handle(_task_created, Mock())

    # Assert
    assert result == (first,)


def test_same_callable_registered_twice_called_twice(bus: DirectBus):
    """
    Registering the same callable twice should produce two independent registrations.

    Given: The same callable registered to an event twice
    When: An event is emitted
    Then: The callable should be invoked twice
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    bus.handle(_task_created, handler)
    bus.handle(_task_created, handler)

    # Act
    emit(task_id=5)

    # Assert
    assert handler.call_count == 2


def test_emit_dispatches_payload(bus: DirectBus):
    """
    ``emit`` should dispatch the payload to registered handlers.

    Given: A handler registered to an event
    When: emit is called with that event
    Then: The handler should receive the payload
    """
    # Arrange
    handler = Mock()
    bus.handle(_task_created, handler)
    payload = _TaskCreated(6)

    # Act
    bus.emit(_task_created, None, payload)

    # Assert
    handler.assert_called_once_with(payload)


def test_emit_invokes_all_handlers(bus: DirectBus):
    """
    ``emit`` should invoke every handler registered for the given event.

    Given: Two handlers registered to an event
    When: emit is called directly
    Then: Both handlers should receive the payload
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()
    bus.handle(_task_created, handler_a)
    bus.handle(_task_created, handler_b)
    payload = _TaskCreated(7)

    # Act
    bus.emit(_task_created, None, payload)

    # Assert
    handler_a.assert_called_once_with(payload)
    handler_b.assert_called_once_with(payload)


def test_raising_handler_does_not_prevent_other_handlers(bus: DirectBus):
    """
    A handler that raises should not prevent subsequent handlers from running.

    Given: Two handlers registered to an event, the first of which raises
    When: an event is emitted
    Then: The second handler should still be called
    """
    # Arrange
    second_handler = Mock()
    bus.handle(_task_created, Mock(side_effect=ValueError("boom")))
    bus.handle(_task_created, second_handler)
    payload = _TaskCreated(10)

    # Act
    with pytest.raises(ExceptionGroup):
        bus.emit(_task_created, None, payload)

    # Assert
    second_handler.assert_called_once_with(payload)


def test_handler_exceptions_collected_into_exception_group(bus: DirectBus):
    """
    All handler exceptions should be collected and raised together as an ExceptionGroup.

    Given: Two handlers registered to an event, both of which raise
    When: an event is emitted
    Then: An ExceptionGroup containing both exceptions should be raised
    """
    # Arrange
    error_a = ValueError("first")
    error_b = RuntimeError("second")
    bus.handle(_task_created, Mock(side_effect=error_a))
    bus.handle(_task_created, Mock(side_effect=error_b))
    payload = _TaskCreated(11)

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exc_info:
        bus.emit(_task_created, None, payload)

    assert set(exc_info.value.exceptions) == {error_a, error_b}


def test_handler_removing_registration_during_dispatch_does_not_break_other_handlers(
    bus: DirectBus,
):
    """
    A removed handler mid-dispatch should not prevent other handlers from running.

    Handlers are stored in a set, so iteration order is not guaranteed; several other
    handlers are registered alongside the self-removing to reduce the chances of degenerate
    ordering causing the test to fail.

    Given: A self-removing handler and several other handlers registered to an event
    When: an event is emitted
    Then: Every other handler should still be called, and no error should be raised
    """
    # Arrange
    other_handlers = [Mock() for _ in range(20)]

    def self_removing(_: object) -> None:
        bus.remove(first_handle)

    first_handle = bus.handle(_task_created, self_removing)
    for handler in other_handlers:
        bus.handle(_task_created, handler)
    payload = _TaskCreated(12)

    # Act
    bus.emit(_task_created, None, payload)

    # Assert
    for handler in other_handlers:
        handler.assert_called_once_with(payload)


def test_signal_pubsub_calls_handler_with_no_arguments(bus: DirectBus):
    """
    A schema-less PubSub event's handler should be called with no arguments.

    Given: A handler that takes no parameters, registered to a PubSub event with no schema
    When: The event is emitted via its bound BoundSignal callable
    Then: The handler should run, receiving no arguments
    """
    # Arrange
    calls: list[bool] = []

    @bus.handle(_heartbeat)
    def _() -> None:
        calls.append(True)

    signal = bus.bind(_heartbeat)

    # Act
    signal()

    # Assert
    assert calls == [True]


def test_request_emit_returns_responder_reply(bus: DirectBus):
    """
    ``emit`` should return the responder's reply for a request event.

    Given: A responder registered to a request event
    When: emit is called with that event
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus.handle(_find_book, Mock(return_value=reply))
    payload = _BookQuery("dune")

    # Act
    result = bus.emit(_find_book, None, payload)

    # Assert
    assert result is reply


def test_bound_request_event_returns_reply(bus: DirectBus):
    """
    Calling a callable bound with a factory for a request event should return the responder's reply.

    Given: A responder registered to a request event bound via bus.bind
    When: The callable is called
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    find_book = bus.bind(_find_book, factory=_BookQuery)
    bus.handle(_find_book, Mock(return_value=reply))

    # Act
    result = find_book(query="dune")

    # Assert
    assert result is reply


def test_request_emit_raises_without_responder(bus: DirectBus):
    """
    ``emit`` should raise NoResponderError when a request event has no responder.

    Given: A request event with no registered responder
    When: emit is called with that event
    Then: A NoResponderError should be raised
    """
    with pytest.raises(NoResponderError):
        bus.emit(_find_book, None, _BookQuery("dune"))


def test_request_emit_raises_with_multiple_responders(bus: DirectBus):
    """
    ``emit`` should raise MultipleRespondersError when several responders are registered.

    Given: Two responders registered to a request event
    When: emit is called with that event
    Then: A MultipleRespondersError should be raised
    """
    # Arrange
    bus.handle(_find_book, Mock(return_value=_BookResult("first")))
    bus.handle(_find_book, Mock(return_value=_BookResult("second")))

    # Act / Assert
    with pytest.raises(MultipleRespondersError):
        bus.emit(_find_book, None, _BookQuery("dune"))


def test_request_responder_exception_propagates_directly(bus: DirectBus):
    """
    A responder's exception should propagate to the emitter unwrapped.

    Given: A responder that raises, registered to a request event
    When: emit is called with that event
    Then: The responder's exception should be raised directly, not as an ExceptionGroup
    """
    # Arrange
    error = ValueError("boom")
    bus.handle(_find_book, Mock(side_effect=error))

    # Act / Assert
    with pytest.raises(ValueError) as exc_info:
        bus.emit(_find_book, None, _BookQuery("dune"))

    assert exc_info.value is error


def test_request_responder_registered_via_decorator(bus: DirectBus):
    """
    A responder registered via the decorator form of handle should serve requests.

    Given: A responder registered with @bus.handle on a request event
    When: emit is called with that event
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")

    @bus.handle(_find_book)
    def _(_: _BookQuery) -> _BookResult:
        return reply

    # Act
    result = bus.emit(_find_book, None, _BookQuery("dune"))

    # Assert
    assert result is reply


def test_signal_request_calls_responder_with_no_arguments(bus: DirectBus):
    """
    A schema-less Request event's responder should be called with no arguments.

    Given: A responder that takes no parameters, registered to a Request event with no schema
    When: The event is emitted via its bound BoundSignal callable
    Then: The responder's return value should be returned
    """

    # Arrange
    @bus.handle(_ping)
    def _() -> str:
        return "pong"

    ping = bus.bind(_ping)

    # Act
    result = ping()

    # Assert
    assert result == "pong"


def test_handler_can_access_envelope_during_dispatch(bus_with_envelope: DirectBus):
    """
    Handlers should be able to access a valid Envelope during dispatch.

    Given: A handler that captures the current envelope
    When: An event is emitted
    Then: The captured value should be an Envelope instance
    """
    # Arrange
    emit = bus_with_envelope.bind(_task_created, factory=_TaskCreated)
    captured: list[Envelope] = []

    def handler(_: object) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        captured.append(envelope)

    bus_with_envelope.handle(_task_created, handler)

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
    emit = bus_with_envelope.bind(_task_created, factory=_TaskCreated)
    captured: list[Envelope] = []

    def handler(_: object) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        captured.append(envelope)

    bus_with_envelope.handle(_task_created, handler)

    # Act
    emit(task_id=1)
    emit(task_id=2)

    # Assert
    assert captured[0].correlation_id != captured[1].correlation_id


def test_nested_emission_produces_child_envelope(bus_with_envelope: DirectBus):
    """
    A handler that emits an event should receive a child envelope linked to the outer one.

    Given: An outer handler that emits on a second event, and an inner handler on that event
    When: The outer event is emitted
    Then: The inner envelope should share the outer correlation id and
          have the outer message id as its causation id
    """
    # Arrange
    outer_event = PubSub(_TaskCreated)
    inner_event = PubSub(_TaskCreated)
    emit_outer = bus_with_envelope.bind(outer_event, factory=_TaskCreated)
    emit_inner = bus_with_envelope.bind(inner_event, factory=_TaskCreated)
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
    emit = bus_with_envelope.bind(_task_created, factory=_TaskCreated)

    @bus_with_envelope.handle(_task_created)
    def _(_: _TaskCreated) -> None: ...

    # Act
    emit(task_id=1)

    # Assert
    assert Envelope.current() is None


def test_request_reply_returned_with_envelope(bus_with_envelope: DirectBus):
    """
    A request event should return its reply when envelope tracking is enabled.

    Given: A responder registered to a request event on an envelope-tracking bus
    When: emit is called with that event
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus_with_envelope.handle(_find_book, Mock(return_value=reply))

    # Act
    result = bus_with_envelope.emit(_find_book, None, _BookQuery("dune"))

    # Assert
    assert result is reply


def test_async_callable_object_rejected_at_registration(bus: DirectBus):
    """
    Registering an object with async __call__ should raise TypeError, like an async function.

    Given: An instance of a class whose __call__ is async
    When: handle is called with it and an event
    Then: A TypeError should be raised
    """

    # Arrange
    class Handler:
        async def __call__(self, _: _TaskCreated) -> None: ...

    # Act / Assert
    with pytest.raises(TypeError):
        bus.handle(_task_created, Handler())


# region: Dispatchers


def test_custom_pattern_dispatched_via_dispatchers_argument():
    """
    A custom pattern should be dispatched through a dispatcher passed to the bus.

    Given: A bus constructed with a dispatcher for a custom pattern, and two handlers
    When: An event of that pattern is emitted
    Then: The emitter gets the dispatcher's result, every handler's reply
    """
    # Arrange
    bus = DirectBus(dispatchers=[Dispatch(_Gather, _gather_replies)])
    bus.handle(_quotes, lambda query: len(query.query))
    bus.handle(_quotes, lambda query: 2 * len(query.query))

    # Act
    replies = bus.emit(_quotes, None, _BookQuery("dune"))

    # Assert
    assert sorted(replies) == [4, 8]


def test_dispatchers_argument_replaces_default():
    """
    A dispatcher passed for a default pattern should replace the bus's own.

    Given: A bus whose PubSub dispatcher is replaced by one that records the payload
    When: A pub/sub event is emitted
    Then: The replacement runs instead of the default fan-out
    """
    # Arrange
    dispatched: list[Any] = []

    def record[S](
        event: PubSub[S, Any], handlers: Collection[Handler[[S], Any, object]], payload: S
    ) -> None:
        dispatched.append(payload)

    bus = DirectBus(dispatchers=[Dispatch(PubSub, record)])
    handler = Mock()
    bus.handle(_task_created, handler)

    # Act
    bus.emit(_task_created, None, _TaskCreated(1))

    # Assert
    assert dispatched == [_TaskCreated(1)]
    handler.assert_not_called()


def test_request_subclass_dispatched_as_request(bus: DirectBus):
    """
    A subclass of a supported pattern should be dispatched as that pattern.

    Given: A subclass of Request with no dispatcher of its own, and a responder for it
    When: An event of the subclass is emitted
    Then: The responder's reply is returned, as for a Request
    """
    # Arrange
    command = _Command(_BookQuery, reply=_BookResult)
    reply = _BookResult("Dune")
    bus.handle(command, Mock(return_value=reply))

    # Act
    result = bus.emit(command, None, _BookQuery("dune"))

    # Assert
    assert result is reply


def test_unsupported_pattern_refused_at_handle(bus: DirectBus):
    """
    Registering a handler for an unsupported pattern should fail immediately.

    Given: A bus with no dispatcher for a custom pattern
    When: A handler is registered for an event of that pattern
    Then: A TypeError names the bus and the pattern, and nothing is registered
    """
    # Act / Assert
    with pytest.raises(TypeError, match="DirectBus has no dispatcher for _Gather"):
        bus.handle(_quotes, lambda query: 1)
    assert bus.handlers(_quotes) == ()


def test_unsupported_pattern_refused_at_bind(bus: DirectBus):
    """
    Binding an unsupported pattern should fail before anything emits it.

    Given: A bus with no dispatcher for a custom pattern
    When: An event of that pattern is bound
    Then: A TypeError names the bus and the pattern
    """
    # Act / Assert
    with pytest.raises(TypeError, match="DirectBus has no dispatcher for _Gather"):
        bus.bind(_quotes, factory=_BookQuery)


def test_unsupported_pattern_refused_at_emit(bus: DirectBus):
    """
    Emitting an unsupported pattern directly should fail.

    Given: A bus with no dispatcher for a custom pattern
    When: An event of that pattern is emitted without being registered or bound
    Then: A TypeError names the pattern
    """
    # Act / Assert
    with pytest.raises(TypeError, match="no dispatcher for _Gather"):
        bus.emit(_quotes, None, _BookQuery("dune"))


# endregion


# region: Hooks


def _log_hooks(log: list[str], label: str) -> Hooks[Any, Any]:
    """Return hooks logging, under a label, the payload they see and the reply or error."""

    def before(event: Any, payload: Any) -> None:
        log.append(f"{label} before {type(payload).__name__}")

    def success(event: Any, payload: Any, reply: Any) -> None:
        log.append(f"{label} success {reply!r}")

    def failure(event: Any, payload: Any, error: Exception) -> None:
        log.append(f"{label} failure {error}")

    return Hooks(before=(before,), success=(success,), failure=(failure,))


def test_publish_hooks_see_request_reply(bus: DirectBus):
    """
    Hooks passed to bind should run around each emit and see the responder's reply.

    Given: A request event bound with hooks, and a responder
    When: The bound callable is called
    Then: The hooks run before and after, and success sees the reply
    """
    # Arrange
    log: list[str] = []
    find_book = Request(_BookQuery, reply=str)
    bus.handle(find_book, lambda query: query.query.title())
    find = bus.bind(find_book, factory=_BookQuery, hooks=_log_hooks(log, "publish"))

    # Act
    reply = find(query="dune")

    # Assert
    assert reply == "Dune"
    assert log == ["publish before _BookQuery", "publish success 'Dune'"]


def test_emit_and_handle_hooks_on_pubsub(bus: DirectBus):
    """
    Emit hooks should run once around a pub/sub emit, and handle hooks around their handler.

    Given: A pub/sub event with two handlers, only one registered with hooks
    When: The event is emitted with hooks
    Then: The emit hooks run around the whole emit; the handler's hooks run around that
        handler only, seeing its return
    """
    # Arrange
    log: list[str] = []
    bus.handle(_task_created, lambda task: task.task_id, hooks=_log_hooks(log, "handle"))
    bus.handle(_task_created, lambda task: task.task_id * 10)

    # Act
    bus.emit(_task_created, None, _TaskCreated(4), hooks=_log_hooks(log, "publish"))

    # Assert
    assert log == [
        "publish before _TaskCreated",
        "handle before _TaskCreated",
        "handle success 4",
        "publish success None",
    ]


def test_handle_hooks_see_responder_failure(bus: DirectBus):
    """
    Hooks passed to handle should see a responder's failure, which still propagates.

    Given: A request event whose responder raises, registered with hooks
    When: The event is emitted
    Then: The failure hook sees the error, and the error propagates
    """
    # Arrange
    log: list[str] = []

    def missing(_: _BookQuery) -> _BookResult:
        raise LookupError("no such book")

    bus.handle(_find_book, missing, hooks=_log_hooks(log, "handle"))

    # Act
    with pytest.raises(LookupError, match="no such book"):
        bus.emit(_find_book, None, _BookQuery("dune"))

    # Assert
    assert log == ["handle before _BookQuery", "handle failure no such book"]


def test_signal_event_hooks_receive_no_payload(bus: DirectBus):
    """
    A payload-less event's hooks should receive a NoPayload instance, matching its type.

    Given: A payload-less request bound with hooks, and a responder registered with hooks
    When: The bound callable is called
    Then: Both hooks see a NoPayload payload, and the responder is called with no arguments
    """
    # Arrange
    payloads: list[object] = []

    def record(event: Any, payload: NoPayload) -> None:
        payloads.append(payload)

    bus.handle(_ping, lambda: "pong", hooks=Hooks(before=(record,)))

    # Act
    reply = bus.bind(_ping, hooks=Hooks(before=(record,)))()

    # Assert
    assert reply == "pong"
    assert len(payloads) == 2
    assert all(isinstance(payload, NoPayload) for payload in payloads)


def test_hooks_run_inside_envelope(bus_with_envelope: DirectBus):
    """
    Emit and handle hooks should run inside the emission's envelope.

    Given: A bus with envelopes, and a handler registered with hooks
    When: An event is emitted with hooks
    Then: Both hooks see the same envelope as the handler
    """
    # Arrange
    seen: list[Envelope | None] = []

    def record(event: Any, payload: Any) -> None:
        seen.append(Envelope.current())

    bus_with_envelope.handle(
        _task_created, lambda task: seen.append(Envelope.current()), hooks=Hooks(before=(record,))
    )

    # Act
    bus_with_envelope.emit(_task_created, None, _TaskCreated(1), hooks=Hooks(before=(record,)))

    # Assert
    assert len(seen) == 3
    assert seen[0] is not None
    assert seen[0] is seen[1] is seen[2]


async def _async_before(event: Any, payload: Any) -> None:
    """Do nothing, asynchronously: a hook DirectBus can't run."""


def test_async_handle_hooks_refused_at_handle(bus: DirectBus):
    """
    Registering a handler with async hooks should fail immediately.

    Given: Hooks including an async hook
    When: A handler is registered with them
    Then: A TypeError names the event, and nothing is registered
    """
    # Act / Assert
    with pytest.raises(TypeError, match="async hooks for '_TaskCreated'"):
        bus.handle(_task_created, lambda task: None, hooks=Hooks(before=(_async_before,)))
    assert bus.handlers(_task_created) == ()


def test_async_publish_hooks_refused_at_bind(bus: DirectBus):
    """
    Binding with async hooks should fail before anything emits.

    Given: Hooks including an async hook
    When: An event is bound with them
    Then: A TypeError names the event
    """
    # Act / Assert
    with pytest.raises(TypeError, match="async hooks for '_TaskCreated'"):
        bus.bind(_task_created, factory=_TaskCreated, hooks=Hooks(before=(_async_before,)))


def test_async_publish_hooks_refused_at_emit(bus: DirectBus):
    """
    Emitting with async hooks should fail.

    Given: Hooks including an async hook
    When: An event is emitted with them
    Then: A TypeError names the event, before any hook or handler runs
    """
    # Act / Assert
    with pytest.raises(TypeError, match="async hooks for '_TaskCreated'"):
        bus.emit(_task_created, None, _TaskCreated(1), hooks=Hooks(before=(_async_before,)))


def test_observing_hook_returning_awaitable_is_logged(
    bus: DirectBus, caplog: pytest.LogCaptureFixture
):
    """
    A success hook returning an awaitable should be logged, not break the call.

    Given: A success hook that returns a coroutine without being detected as async, and a
        complete hook recording that it ran
    When: An event is emitted with them
    Then: The reply is returned, the complete hook runs, and the hook's error is logged
    """
    # Arrange
    completed: list[BaseException | None] = []

    async def work() -> None: ...

    def returns_coroutine(event: Any, payload: Any, reply: Any) -> object:
        return work()

    def record(event: Any, payload: Any, error: BaseException | None) -> None:
        completed.append(error)

    bus.handle(_find_book, lambda query: _BookResult(query.query))
    hooks: Hooks[_BookQuery, _BookResult] = Hooks(success=(returns_coroutine,), complete=(record,))

    # Act
    reply = bus.emit(_find_book, None, _BookQuery("dune"), hooks=hooks)

    # Assert
    assert reply.title == "dune"
    assert completed == [None]
    assert "returned an awaitable" in caplog.text


# endregion


# region: Middleware


def _labelled(log: list[str], label: str) -> Any:
    """Return a sync context manager class logging under a label as it's entered and exited."""

    class Labelled:
        def __init__(self, call: PublishCall | HandleCall) -> None: ...

        def __enter__(self) -> None:
            log.append(f"{label} in")

        def __exit__(self, *exc: object) -> None:
            log.append(f"{label} out")

    return Labelled


def _labelled_hooks(log: list[str], label: str) -> Hooks[Any, Any]:
    """Return hooks logging under a label before and after the call."""

    def before(event: Any, payload: Any) -> None:
        log.append(f"{label} before")

    def complete(event: Any, payload: Any, error: BaseException | None) -> None:
        log.append(f"{label} complete")

    return Hooks(before=(before,), complete=(complete,))


def test_middleware_wraps_hooks_and_handlers():
    """
    Publish middleware should wrap each emit, and handle middleware each handler, outside hooks.

    Given: A bus with publish and handle middleware, and two handlers, one with hooks
    When: A pub/sub event is emitted with hooks
    Then: Publish middleware wraps the emit's hooks once; handle middleware wraps each
        handler, outside that handler's hooks
    """
    # Arrange
    log: list[str] = []
    bus = DirectBus(
        middleware=Middleware(
            publish=(_labelled(log, "publish mw"),), handle=(_labelled(log, "handle mw"),)
        )
    )
    bus.handle(_task_created, lambda task: log.append("handler"), hooks=_labelled_hooks(log, "h"))

    # Act
    bus.emit(_task_created, None, _TaskCreated(1), hooks=_labelled_hooks(log, "p"))

    # Assert
    assert log == [
        "publish mw in",
        "p before",
        "handle mw in",
        "h before",
        "handler",
        "h complete",
        "handle mw out",
        "p complete",
        "publish mw out",
    ]


def test_handle_middleware_sees_event_and_reply():
    """
    Handle middleware should see the event, the payload and the responder's reply.

    Given: A bus with handle middleware recording what it sees, and a responder
    When: A request is emitted
    Then: The middleware sees the event, payload and reply, and the reply is returned
    """
    # Arrange
    seen: list[object] = []

    @contextmanager
    def record(call: HandleCall) -> Generator[None, None, None]:
        yield
        seen.extend((call.event, call.payload.query, call.reply))

    bus = DirectBus(middleware=Middleware(handle=(record,)))
    bus.handle(_find_book, lambda query: _BookResult(query.query))

    # Act
    reply = bus.emit(_find_book, None, _BookQuery("dune"))

    # Assert
    assert reply.title == "dune"
    assert seen == [_find_book, "dune", reply]


def test_publish_middleware_inside_envelope():
    """
    Publish middleware should run inside the emission's envelope.

    Given: A bus with envelopes and publish middleware recording the current envelope
    When: An event is emitted
    Then: The middleware sees the same envelope as the handler
    """
    # Arrange
    seen: list[Envelope | None] = []

    @contextmanager
    def record(call: PublishCall) -> Generator[None, None, None]:
        seen.append(Envelope.current())
        yield

    bus = DirectBus(use_envelope=True, middleware=Middleware(publish=(record,)))
    bus.handle(_task_created, lambda task: seen.append(Envelope.current()))

    # Act
    bus.emit(_task_created, None, _TaskCreated(1))

    # Assert
    assert seen[0] is not None
    assert seen == [seen[0], seen[0]]


def test_handle_middleware_raising_on_entry_refuses_handler():
    """
    Handle middleware raising on entry should refuse the handler call.

    Given: A bus whose handle middleware raises on entry, and a responder
    When: A request is emitted
    Then: The middleware's error is raised, and the responder doesn't run
    """
    # Arrange
    calls: list[str] = []

    @contextmanager
    def refuse(call: HandleCall) -> Generator[None, None, None]:
        raise PermissionError("rate limited")
        yield

    def respond(query: _BookQuery) -> _BookResult:
        calls.append("responder")
        return _BookResult(query.query)

    bus = DirectBus(middleware=Middleware(handle=(refuse,)))
    bus.handle(_find_book, respond)

    # Act / Assert
    with pytest.raises(PermissionError, match="rate limited"):
        bus.emit(_find_book, None, _BookQuery("dune"))
    assert calls == []


def test_swallowed_interrupt_stops_fan_out():
    """
    A KeyboardInterrupt that handle middleware tries to swallow should still stop the emit.

    Given: A bus whose handle middleware's exit returns True, and two handlers that each
        record they ran, then raise KeyboardInterrupt
    When: A pub/sub event is emitted
    Then: KeyboardInterrupt propagates, and only the first handler ran
    """
    # Arrange
    ran: list[str] = []

    class Suppressing:
        def __init__(self, call: HandleCall) -> None: ...

        def __enter__(self) -> None: ...

        def __exit__(self, *exc: object) -> bool:
            return True

    def interrupt(task: _TaskCreated) -> None:
        ran.append("handler")
        raise KeyboardInterrupt

    bus = DirectBus(middleware=Middleware(handle=(Suppressing,)))
    bus.handle(_task_created, interrupt)
    bus.handle(_task_created, interrupt)

    # Act / Assert
    with pytest.raises(KeyboardInterrupt):
        bus.emit(_task_created, None, _TaskCreated(1))
    assert ran == ["handler"]


def test_async_middleware_refused():
    """
    DirectBus should refuse an async context manager class as middleware when it's built.

    Given: Publish middleware that's an async context manager class
    When: A DirectBus is built with it
    Then: A TypeError says DirectBus enters middleware synchronously
    """

    # Arrange
    class Trace:
        def __init__(self, call: PublishCall) -> None: ...

        async def __aenter__(self) -> None: ...

        async def __aexit__(self, *exc: object) -> None: ...

    # Act / Assert
    with pytest.raises(TypeError, match="DirectBus enters middleware synchronously"):
        DirectBus(middleware=Middleware(publish=(Trace,)))


def test_unsupported_pattern_fails_before_middleware():
    """
    Emitting an unsupported pattern directly should fail before any middleware runs.

    Given: A bus with publish middleware recording each emit, and no dispatcher for a pattern
    When: An event of that pattern is emitted directly
    Then: TypeError is raised, and the middleware never ran
    """
    # Arrange
    log: list[str] = []
    bus = DirectBus(middleware=Middleware(publish=(_labelled(log, "mw"),)))

    # Act / Assert
    with pytest.raises(TypeError, match="no dispatcher for _Gather"):
        bus.emit(_quotes, None, _BookQuery("dune"))
    assert log == []


def test_kept_call_does_not_keep_bus_alive():
    """
    A call object a middleware keeps shouldn't keep the bus alive.

    Given: A bus whose publish middleware keeps every call object, and a responder
    When: A request is emitted, and the bus is dropped
    Then: The bus is collected, though the call object is still kept
    """
    # Arrange
    kept: list[PublishCall] = []

    @contextmanager
    def keep(call: PublishCall) -> Generator[None, None, None]:
        kept.append(call)
        yield

    bus = DirectBus(middleware=Middleware(publish=(keep,)))
    bus.handle(_find_book, lambda query: _BookResult(query.query))
    bus.emit(_find_book, None, _BookQuery("dune"))
    collected = weakref.ref(bus)

    # Act
    del bus
    gc.collect()

    # Assert
    assert len(kept) == 1
    assert collected() is None


# endregion
