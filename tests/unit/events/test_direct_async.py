"""
Unit tests for the AsyncDirectBus adapter.

This test suite verifies the following behaviors:

AsyncDirectBus:
- Awaiting a factory-bound callable dispatches the payload to a registered sync handler.
- Awaiting a factory-bound callable dispatches the payload to a registered async handler.
- Mixed sync and async handlers on the same channel both receive the payload.
- All handlers on a channel receive the payload.
- Handlers on different channels are isolated from each other.
- handle returns a Handler.
- remove removes a handler; subsequent emits do not invoke it.
- handlers returns an empty tuple for an event with no handlers.
- handlers returns every handler registered for an event, sync and async.
- handlers excludes a removed handler.
- handlers returns a schema-less event's handlers.
- handlers returns a snapshot that later registrations do not change.
- The same callable may be registered multiple times independently.
- emit called directly dispatches to the event's handlers.
- A raising handler does not prevent other handlers from running.
- All handler exceptions are collected and re-raised as an ExceptionGroup.
- A handler's CancelledError is re-raised in a BaseExceptionGroup, not swallowed.
- A schema-less PubSub event's sync and async handlers are called with no arguments.

AsyncDirectBus (request events):
- emit awaits and returns an async responder's reply.
- emit returns a sync responder's reply.
- A factory-bound callable for a request event resolves to the reply when awaited.
- emit raises NoResponderError when no responder is registered.
- emit raises MultipleRespondersError when several responders are registered.
- A responder's exception propagates directly, not as an ExceptionGroup.
- An async responder may be registered via the decorator form of handle.
- A schema-less Request event's sync and async responders are called with no arguments.

AsyncDirectBus (dispatchers):
- A custom pattern is dispatched through an async dispatcher passed with dispatchers=.
- An unsupported pattern raises TypeError at handle and at bind.

AsyncDirectBus (hooks):
- Sync and async hooks passed to bind run around each emit and see a request's reply.
- A sync handler registered with async hooks is awaited, with the hooks around it.
- Hooks passed to emit run inside the emission's envelope when envelopes are tracked.
- A payload-less event's hooks receive a NoPayload instance.

AsyncDirectBus (middleware):
- Async publish and handle middleware run outside hooks, inside the emission's envelope.
- Publish middleware wraps an emit without hooks.
- Sync middleware is entered alongside async middleware.
- A class that isn't a context manager is refused as middleware when the bus is built.

AsyncDirectBus (with envelope):
- Handlers can access the Envelope during dispatch.
- Each top-level emission creates an independent envelope.
- A handler that emits an event receives a child envelope.
- The envelope is cleaned up after dispatch completes.
- A request reply is returned when envelope tracking is enabled.
"""

import asyncio
from contextlib import contextmanager
from typing import Any, Awaitable, Collection, Generator, Literal, cast
from unittest.mock import AsyncMock, Mock

import pytest

from stratae.events import (
    AsyncDirectBus,
    Consumer,
    Dispatch,
    EmitCallable,
    Envelope,
    HandleCall,
    Handler,
    Hooks,
    Immutable,
    Middleware,
    Producer,
    PublishCall,
    PubSub,
    Request,
)
from stratae.events.exceptions import MultipleRespondersError, NoResponderError


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


async def _gather_replies[S, R](
    event: _Gather[S, R], handlers: Collection[Handler[[S], Any, Awaitable[R]]], payload: S
) -> list[R]:
    """Dispatch a _Gather event of async handlers: await each and collect the replies."""
    return [await handler(payload) for handler in handlers]


_quotes = _Gather(_BookQuery, reply=int)


@pytest.fixture
def bus() -> AsyncDirectBus:
    """Return a fresh AsyncDirectBus instance with no envelope."""
    return AsyncDirectBus()


@pytest.fixture
def bus_with_envelope() -> AsyncDirectBus:
    """Return a fresh AsyncDirectBus instance with envelope tracking enabled."""
    return AsyncDirectBus(use_envelope=True)


def test_bus_satisfies_producer_protocol(bus: AsyncDirectBus):
    """
    AsyncDirectBus should satisfy the Producer protocol.

    Given: An AsyncDirectBus
    When: checked against the Producer protocol
    Then: isinstance should return True
    """
    assert isinstance(bus, Producer)


def test_bus_emit_satisfies_emit_callable_protocol(bus: AsyncDirectBus):
    """
    AsyncDirectBus.emit should satisfy the EmitCallable protocol.

    Given: An AsyncDirectBus
    When: its bound emit method is checked against the EmitCallable protocol
    Then: isinstance should return True
    """
    assert isinstance(bus.emit, EmitCallable)


def test_bus_satisfies_consumer_protocol(bus: AsyncDirectBus):
    """
    AsyncDirectBus should satisfy the Consumer protocol.

    Given: An AsyncDirectBus
    When: checked against the Consumer protocol
    Then: isinstance should return True
    """
    assert isinstance(bus, Consumer)


async def test_awaiting_bound_event_forwards_payload_to_handler(bus: AsyncDirectBus):
    """
    Awaiting a passthrough callable should dispatch an already-built payload.

    Given: A handler registered via bus.handle, and a callable bound with no factory
    When: The callable is awaited with a ready-made payload
    Then: The handler should be called with that payload
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created)
    bus.handle(_task_created, handler)

    # Act
    await emit(_TaskCreated(1))

    # Assert
    handler.assert_called_once_with(_TaskCreated(1))


async def test_dispatches_to_sync_handler(bus: AsyncDirectBus):
    """
    Awaiting a factory-bound callable should dispatch the payload to a registered sync handler.

    Given: A sync handler registered via bus.handle
    When: The callable is awaited
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    bus.handle(_task_created, handler)

    # Act
    await emit(task_id=1)

    # Assert
    handler.assert_called_once_with(_TaskCreated(1))


async def test_dispatches_to_async_handler(bus: AsyncDirectBus):
    """
    Awaiting a factory-bound callable should dispatch the payload to a registered async handler.

    Given: An async handler registered via bus.handle
    When: The callable is awaited
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    bus.handle(_task_created, handler)

    # Act
    await emit(task_id=2)

    # Assert
    handler.assert_called_once_with(_TaskCreated(2))


async def test_dispatches_to_mixed_handlers(bus: AsyncDirectBus):
    """
    Both sync and async handlers registered to the same event should receive the payload.

    Given: A sync and an async handler registered to the same event
    When: An event is emitted
    Then: Both handlers should be called with the payload
    """
    # Arrange
    sync_handler = Mock()
    async_handler = AsyncMock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    bus.handle(_task_created, sync_handler)
    bus.handle(_task_created, async_handler)

    # Act
    await emit(task_id=3)

    # Assert
    sync_handler.assert_called_once_with(_TaskCreated(3))
    async_handler.assert_called_once_with(_TaskCreated(3))


async def test_dispatches_to_all_handlers_on_channel(bus: AsyncDirectBus):
    """
    All handlers registered on the same event should receive the payload.

    Given: Two async handlers registered to the same event
    When: An event is emitted
    Then: Both handlers should be called with the payload
    """
    # Arrange
    handler_a = AsyncMock()
    handler_b = AsyncMock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    bus.handle(_task_created, handler_a)
    bus.handle(_task_created, handler_b)

    # Act
    await emit(task_id=4)

    # Assert
    handler_a.assert_called_once_with(_TaskCreated(4))
    handler_b.assert_called_once_with(_TaskCreated(4))


async def test_channel_isolation(bus: AsyncDirectBus):
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
    task_handler = AsyncMock()
    order_handler = AsyncMock()
    bus.handle(task_event, task_handler)
    bus.handle(order_event, order_handler)

    # Act
    await emit_task(task_id=5)

    # Assert
    task_handler.assert_called_once_with(_TaskCreated(5))
    order_handler.assert_not_called()


def test_handle_returns_handler(bus: AsyncDirectBus):
    """
    ``handle`` should return the Handler wrapping the registered callable.

    Given: An AsyncDirectBus
    When: handle is called with a callable
    Then: The returned Handler should wrap that callable
    """
    fn = Mock()
    handler = bus.handle(_task_created, fn)

    assert handler.call is fn


async def test_remove_prevents_further_dispatch(bus: AsyncDirectBus):
    """
    Removed handlers should not receive subsequent emissions.

    Given: A handler registered and then removed
    When: An event is emitted
    Then: The handler should not be called
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    handle = bus.handle(_task_created, handler)
    bus.remove(handle)

    # Act
    await emit(task_id=6)

    # Assert
    handler.assert_not_called()


def test_handlers_empty_for_event_without_handlers(bus: AsyncDirectBus):
    """
    ``handlers`` should return an empty tuple for an event nothing handles.

    Given: An AsyncDirectBus with no handlers registered
    When: handlers is called for an event
    Then: An empty tuple should be returned
    """
    # Act
    result = bus.handlers(_task_created)

    # Assert
    assert result == ()


def test_handlers_returns_registered_handlers(bus: AsyncDirectBus):
    """
    ``handlers`` should return every handler registered for an event, sync and async.

    Given: A sync and an async handler registered to the same event
    When: handlers is called for that event
    Then: Both Handlers should be returned
    """
    # Arrange
    sync_handle = bus.handle(_task_created, Mock())
    async_handle = bus.handle(_task_created, AsyncMock())

    # Act
    result = bus.handlers(_task_created)

    # Assert
    assert set(result) == {sync_handle, async_handle}


def test_handlers_excludes_removed_handler(bus: AsyncDirectBus):
    """
    ``handlers`` should not return a handler after it is removed.

    Given: Two handlers registered to the same event, one of them then removed
    When: handlers is called for that event
    Then: Only the remaining Handler should be returned
    """
    # Arrange
    kept = bus.handle(_task_created, AsyncMock())
    removed = bus.handle(_task_created, AsyncMock())
    bus.remove(removed)

    # Act
    result = bus.handlers(_task_created)

    # Assert
    assert result == (kept,)


def test_handlers_returns_signal_event_handlers(bus: AsyncDirectBus):
    """
    ``handlers`` should return the handlers of a schema-less event.

    Given: A responder registered to a schema-less Request event
    When: handlers is called for that event
    Then: The responder's Handler should be returned
    """
    # Arrange
    responder = bus.handle(_ping, AsyncMock(return_value="pong"))

    # Act
    result = bus.handlers(_ping)

    # Assert
    assert result == (responder,)


def test_handlers_snapshot_unchanged_by_later_registration(bus: AsyncDirectBus):
    """
    ``handlers`` should return a snapshot that later registrations don't change.

    Given: The handlers of an event, looked up while it has one handler
    When: Another handler is registered to that event
    Then: The earlier result should still hold only the first Handler
    """
    # Arrange
    first = bus.handle(_task_created, AsyncMock())
    result = bus.handlers(_task_created)

    # Act
    bus.handle(_task_created, AsyncMock())

    # Assert
    assert result == (first,)


async def test_same_callable_registered_twice_called_twice(bus: AsyncDirectBus):
    """
    Registering the same callable twice should produce two independent registrations.

    Given: The same callable registered to an event twice
    When: An event is emitted
    Then: The callable should be invoked twice
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.bind(_task_created, factory=_TaskCreated)
    bus.handle(_task_created, handler)
    bus.handle(_task_created, handler)

    # Act
    await emit(task_id=7)

    # Assert
    assert handler.call_count == 2


async def test_emit_dispatches_directly(bus: AsyncDirectBus):
    """
    ``emit`` should dispatch the payload directly to all registered handlers.

    Given: A handler registered to an event
    When: emit is called directly with that event
    Then: The handler should receive the payload
    """
    # Arrange
    handler = AsyncMock()
    bus.handle(_task_created, handler)
    payload = _TaskCreated(8)

    # Act
    await bus.emit(_task_created, None, payload)

    # Assert
    handler.assert_called_once_with(payload)


async def test_raising_handler_does_not_prevent_other_handlers(bus: AsyncDirectBus):
    """
    A handler that raises should not prevent subsequent handlers from running.

    Given: Two handlers registered to an event, the first of which raises
    When: an event is emitted
    Then: The second handler should still be called
    """
    # Arrange
    second_handler = AsyncMock()
    bus.handle(_task_created, AsyncMock(side_effect=ValueError("boom")))
    bus.handle(_task_created, second_handler)
    payload = _TaskCreated(10)

    # Act
    with pytest.raises(ExceptionGroup):
        await bus.emit(_task_created, None, payload)

    # Assert
    second_handler.assert_called_once_with(payload)


async def test_handler_exceptions_collected_into_exception_group(bus: AsyncDirectBus):
    """
    All handler exceptions should be collected and raised together as an ExceptionGroup.

    Given: Two handlers registered to an event, both of which raise
    When: an event is emitted
    Then: An ExceptionGroup containing both exceptions should be raised
    """
    # Arrange
    error_a = ValueError("first")
    error_b = RuntimeError("second")
    bus.handle(_task_created, AsyncMock(side_effect=error_a))
    bus.handle(_task_created, AsyncMock(side_effect=error_b))
    payload = _TaskCreated(11)

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exc_info:
        await bus.emit(_task_created, None, payload)

    assert set(exc_info.value.exceptions) == {error_a, error_b}


async def test_handler_cancelled_error_is_not_swallowed(bus: AsyncDirectBus):
    """
    A handler's CancelledError is re-raised alongside other handler exceptions.

    Given: Two handlers registered to an event, one raising CancelledError and one ValueError
    When: an event is dispatched
    Then: A BaseExceptionGroup containing both is raised, rather than the CancelledError
        being dropped
    """
    # Arrange
    error = ValueError("failed")
    bus.handle(_task_created, AsyncMock(side_effect=asyncio.CancelledError()))
    bus.handle(_task_created, AsyncMock(side_effect=error))

    # Act / Assert
    with pytest.raises(BaseExceptionGroup) as exc_info:
        await bus.emit(_task_created, None, _TaskCreated(12))

    assert not isinstance(exc_info.value, ExceptionGroup)
    assert {type(exc) for exc in exc_info.value.exceptions} == {asyncio.CancelledError, ValueError}
    assert error in exc_info.value.exceptions


async def test_signal_pubsub_calls_sync_handler_with_no_arguments(bus: AsyncDirectBus):
    """
    A schema-less PubSub event's sync handler should be called with no arguments.

    Given: A sync handler that takes no parameters, registered to a PubSub event with no schema
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
    await signal()

    # Assert
    assert calls == [True]


async def test_signal_pubsub_calls_async_handler_with_no_arguments(bus: AsyncDirectBus):
    """
    A schema-less PubSub event's async handler should be called with no arguments.

    Given: An async handler that takes no parameters, registered to a PubSub event with no schema
    When: The event is emitted via its bound BoundSignal callable
    Then: The handler should run, receiving no arguments
    """
    # Arrange
    calls: list[bool] = []

    @bus.handle(_heartbeat)
    async def _() -> None:
        calls.append(True)

    signal = bus.bind(_heartbeat)

    # Act
    await signal()

    # Assert
    assert calls == [True]


async def test_request_emit_returns_async_responder_reply(bus: AsyncDirectBus):
    """
    ``emit`` should await and return an async responder's reply for a request event.

    Given: An async responder registered to a request event
    When: emit is awaited with that event
    Then: The responder's resolved return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus.handle(_find_book, AsyncMock(return_value=reply))

    # Act
    result = await bus.emit(_find_book, None, _BookQuery("dune"))

    # Assert
    assert result is reply


async def test_request_emit_returns_sync_responder_reply(bus: AsyncDirectBus):
    """
    ``emit`` should return a sync responder's reply for a request event.

    Given: A sync responder registered to a request event
    When: emit is awaited with that event
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus.handle(_find_book, Mock(return_value=reply))

    # Act
    result = await bus.emit(_find_book, None, _BookQuery("dune"))

    # Assert
    assert result is reply


async def test_bound_request_event_resolves_to_reply(bus: AsyncDirectBus):
    """
    Awaiting a factory-bound callable for a request event should resolve to the responder's reply.

    Given: An async responder registered to a request event bound via bus.bind
    When: The callable is awaited
    Then: The responder's resolved return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    find_book = bus.bind(_find_book, factory=_BookQuery)
    bus.handle(_find_book, AsyncMock(return_value=reply))

    # Act
    result = await find_book(query="dune")

    # Assert
    assert result is reply


async def test_request_emit_raises_without_responder(bus: AsyncDirectBus):
    """
    ``emit`` should raise NoResponderError when a request event has no responder.

    Given: A request event with no registered responder
    When: emit is awaited with that event
    Then: A NoResponderError should be raised
    """
    with pytest.raises(NoResponderError):
        await bus.emit(_find_book, None, _BookQuery("dune"))


async def test_request_emit_raises_with_multiple_responders(bus: AsyncDirectBus):
    """
    ``emit`` should raise MultipleRespondersError when several responders are registered.

    Given: Two responders registered to a request event
    When: emit is awaited with that event
    Then: A MultipleRespondersError should be raised
    """
    # Arrange
    bus.handle(_find_book, AsyncMock(return_value=_BookResult("first")))
    bus.handle(_find_book, AsyncMock(return_value=_BookResult("second")))

    # Act / Assert
    with pytest.raises(MultipleRespondersError):
        await bus.emit(_find_book, None, _BookQuery("dune"))


async def test_request_responder_exception_propagates_directly(bus: AsyncDirectBus):
    """
    A responder's exception should propagate to the emitter unwrapped.

    Given: An async responder that raises, registered to a request event
    When: emit is awaited with that event
    Then: The responder's exception should be raised directly, not as an ExceptionGroup
    """
    # Arrange
    error = ValueError("boom")
    bus.handle(_find_book, AsyncMock(side_effect=error))

    # Act / Assert
    with pytest.raises(ValueError) as exc_info:
        await bus.emit(_find_book, None, _BookQuery("dune"))

    assert exc_info.value is error


async def test_request_responder_registered_via_decorator(bus: AsyncDirectBus):
    """
    An async responder registered via the decorator form of handle should serve requests.

    Given: An async responder registered with @bus.handle on a request event
    When: emit is awaited with that event
    Then: The responder's resolved return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")

    @bus.handle(_find_book)
    async def _(_: _BookQuery) -> _BookResult:
        return reply

    # Act
    result = await bus.emit(_find_book, None, _BookQuery("dune"))

    # Assert
    assert result is reply


async def test_signal_request_calls_sync_responder_with_no_arguments(bus: AsyncDirectBus):
    """
    A schema-less Request event's sync responder should be called with no arguments.

    Given: A sync responder that takes no parameters, registered to a Request event with no schema
    When: The event is emitted via its bound BoundSignal callable
    Then: The responder's return value should be returned
    """

    # Arrange
    @bus.handle(_ping)
    def _() -> str:
        return "pong"

    ping = bus.bind(_ping)

    # Act
    result = await ping()

    # Assert
    assert result == "pong"


async def test_signal_request_calls_async_responder_with_no_arguments(bus: AsyncDirectBus):
    """
    A schema-less Request event's async responder should be called with no arguments.

    Given: An async responder that takes no parameters, registered to a Request event with no schema
    When: The event is emitted via its bound BoundSignal callable
    Then: The responder's return value should be returned
    """

    # Arrange
    @bus.handle(_ping)
    async def _() -> str:
        return "pong"

    ping = bus.bind(_ping)

    # Act
    result = await ping()

    # Assert
    assert result == "pong"


async def test_handler_can_access_envelope_during_dispatch(bus_with_envelope: AsyncDirectBus):
    """
    Handlers should be able to access a valid Envelope during dispatch.

    Given: A handler that captures the current envelope
    When: An event is emitted
    Then: The captured value should be an Envelope instance
    """
    # Arrange
    emit = bus_with_envelope.bind(_task_created, factory=_TaskCreated)
    captured: list[Envelope] = []

    @bus_with_envelope.handle(_task_created)
    async def _(_) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        captured.append(envelope)

    # Act
    await emit(task_id=1)

    # Assert
    assert len(captured) == 1
    assert isinstance(captured[0], Envelope)


async def test_each_emission_creates_independent_envelope(bus_with_envelope: AsyncDirectBus):
    """
    Each top-level emission should produce an envelope with a unique correlation id.

    Given: A handler that captures the current envelope
    When: Two separate events are emitted
    Then: Each emission should have a distinct correlation id
    """
    # Arrange
    emit = bus_with_envelope.bind(_task_created, factory=_TaskCreated)
    captured: list[Envelope] = []

    @bus_with_envelope.handle(_task_created)
    async def _(_) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        captured.append(envelope)

    # Act
    await emit(task_id=1)
    await emit(task_id=2)

    # Assert
    assert captured[0].correlation_id != captured[1].correlation_id


async def test_nested_emission_produces_child_envelope(bus_with_envelope: AsyncDirectBus):
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
    async def _(_) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        outer_envelopes.append(envelope)
        await emit_inner(task_id=99)

    @bus_with_envelope.handle(inner_event)
    async def _(_) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        inner_envelopes.append(envelope)

    # Act
    await emit_outer(task_id=1)

    # Assert
    assert inner_envelopes[0].correlation_id == outer_envelopes[0].correlation_id
    assert inner_envelopes[0].causation_id == outer_envelopes[0].message_id
    assert inner_envelopes[0].message_id != outer_envelopes[0].message_id


async def test_envelope_cleaned_up_after_dispatch(bus_with_envelope: AsyncDirectBus):
    """
    The Envelope should not be accessible after dispatch completes.

    Given: An AsyncDirectBus with a registered handler
    When: An event is emitted and dispatch completes
    Then: Accessing the current envelope should return None
    """
    # Arrange
    emit = bus_with_envelope.bind(_task_created, factory=_TaskCreated)
    bus_with_envelope.handle(_task_created, AsyncMock())

    # Act
    await emit(task_id=1)

    # Assert
    assert Envelope.current() is None


async def test_request_reply_returned_with_envelope(bus_with_envelope: AsyncDirectBus):
    """
    A request event should resolve to its reply when envelope tracking is enabled.

    Given: An async responder registered to a request event on an envelope-tracking bus
    When: emit is awaited with that event
    Then: The responder's resolved return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus_with_envelope.handle(_find_book, AsyncMock(return_value=reply))

    # Act
    result = await bus_with_envelope.emit(_find_book, None, _BookQuery("dune"))

    # Assert
    assert result is reply


async def test_async_callable_object_handler_is_awaited(bus: AsyncDirectBus):
    """
    A pub/sub handler that's an object with async __call__ is awaited.

    Given: An instance of a class whose __call__ is async, registered to a PubSub event
    When: an event is dispatched
    Then: Its __call__ runs to completion
    """

    # Arrange
    class Handler:
        def __init__(self) -> None:
            self.received: list[_TaskCreated] = []

        async def __call__(self, payload: _TaskCreated) -> None:
            self.received.append(payload)

    handler = Handler()
    bus.handle(_task_created, handler)

    # Act
    await bus.emit(_task_created, None, _TaskCreated(13))

    # Assert
    assert handler.received == [_TaskCreated(13)]


async def test_async_callable_object_responder_reply_is_awaited(bus: AsyncDirectBus):
    """
    A request responder that's an object with async __call__ has its reply awaited.

    Given: An instance of a class whose __call__ is async, registered to a request event
    When: emit is awaited with that event
    Then: The resolved reply is returned, not a coroutine
    """
    # Arrange
    reply = _BookResult("Dune")

    class Responder:
        async def __call__(self, _: _BookQuery) -> _BookResult:
            return reply

    bus.handle(_find_book, Responder())

    # Act
    result = await bus.emit(_find_book, None, _BookQuery("dune"))

    # Assert
    assert result is reply


# region: Dispatchers


async def test_custom_pattern_dispatched_via_async_dispatcher():
    """
    A custom pattern should be dispatched through an async dispatcher passed to the bus.

    Given: A bus constructed with an async dispatcher for a custom pattern, and two
        async handlers
    When: An event of that pattern is emitted
    Then: The emitter gets the dispatcher's result, every handler's reply
    """

    # Arrange
    async def length(query: _BookQuery) -> int:
        return len(query.query)

    async def double_length(query: _BookQuery) -> int:
        return 2 * len(query.query)

    bus = AsyncDirectBus(dispatchers=[Dispatch(_Gather, _gather_replies)])
    bus.handle(_quotes, length)
    bus.handle(_quotes, double_length)

    # Act
    replies = await bus.emit(_quotes, None, _BookQuery("dune"))

    # Assert
    assert sorted(replies) == [4, 8]


def test_unsupported_pattern_refused_at_handle(bus: AsyncDirectBus):
    """
    Registering a handler for an unsupported pattern should fail immediately.

    Given: A bus with no dispatcher for a custom pattern
    When: A handler is registered for an event of that pattern
    Then: A TypeError names the bus and the pattern, and nothing is registered
    """
    # Act / Assert
    with pytest.raises(TypeError, match="AsyncDirectBus has no dispatcher for _Gather"):
        bus.handle(_quotes, lambda query: 1)
    assert bus.handlers(_quotes) == ()


def test_unsupported_pattern_refused_at_bind(bus: AsyncDirectBus):
    """
    Binding an unsupported pattern should fail before anything emits it.

    Given: A bus with no dispatcher for a custom pattern
    When: An event of that pattern is bound
    Then: A TypeError names the bus and the pattern
    """
    # Act / Assert
    with pytest.raises(TypeError, match="AsyncDirectBus has no dispatcher for _Gather"):
        bus.bind(_quotes, factory=_BookQuery)


# endregion


# region: Hooks


def _log(log: list[str], label: str) -> Any:
    """Return a sync hook logging its label and the reply or payload type it sees."""

    def hook(event: Any, payload: Any, *rest: Any) -> None:
        log.append(f"{label} {rest[0]!r}" if rest else f"{label} {type(payload).__name__}")

    return hook


def _alog(log: list[str], label: str) -> Any:
    """Return an async hook logging its label and the reply or payload type it sees."""

    async def hook(event: Any, payload: Any, *rest: Any) -> None:
        log.append(f"{label} {rest[0]!r}" if rest else f"{label} {type(payload).__name__}")

    return hook


async def test_publish_hooks_see_request_reply(bus: AsyncDirectBus):
    """
    Sync and async publish hooks should run around an emit and see the reply.

    Given: A request event bound with an async before hook and a sync success hook, and an
        async responder
    When: The event is emitted through a bound callable
    Then: Both hooks run in order, and success sees the reply
    """
    # Arrange
    log: list[str] = []
    find_book = Request(_BookQuery, reply=str)
    hooks: Hooks[_BookQuery, str] = Hooks(
        before=(_alog(log, "before"),), success=(_log(log, "success"),)
    )

    async def respond(query: _BookQuery) -> str:
        return query.query.title()

    bus.handle(find_book, respond)
    find = bus.bind(find_book, factory=_BookQuery, hooks=hooks)

    # Act
    reply = await find(query="dune")

    # Assert
    assert reply == "Dune"
    assert log == ["before _BookQuery", "success 'Dune'"]


async def test_sync_handler_with_async_handle_hooks_awaited(bus: AsyncDirectBus):
    """
    A sync handler whose event has async handle hooks should still run, with the hooks.

    Given: A sync handler registered with async hooks
    When: The event is emitted
    Then: The hooks run around the handler and see its return
    """
    # Arrange
    log: list[str] = []
    hooks: Hooks[_TaskCreated, object] = Hooks(
        before=(_alog(log, "before"),), success=(_alog(log, "success"),)
    )
    bus.handle(_task_created, lambda task: task.task_id, hooks=hooks)

    # Act
    await bus.emit(_task_created, None, _TaskCreated(7))

    # Assert
    assert log == ["before _TaskCreated", "success 7"]


async def test_publish_hooks_run_inside_envelope(bus_with_envelope: AsyncDirectBus):
    """
    Publish hooks should run inside the emission's envelope when envelopes are tracked.

    Given: A bus with envelopes, and a pub/sub handler
    When: The event is emitted with an async hook
    Then: The hook and the handler see the same envelope
    """
    # Arrange
    seen: list[Envelope | None] = []

    async def record(event: Any, payload: Any) -> None:
        seen.append(Envelope.current())

    bus_with_envelope.handle(_task_created, lambda task: seen.append(Envelope.current()))

    # Act
    await bus_with_envelope.emit(
        _task_created, None, _TaskCreated(1), hooks=Hooks(before=(record,))
    )

    # Assert
    assert len(seen) == 2
    assert seen[0] is not None
    assert seen[0] is seen[1]


async def test_signal_event_hooks_receive_no_payload(bus: AsyncDirectBus):
    """
    A payload-less event's hooks should receive a NoPayload instance, matching its type.

    Given: A payload-less pub/sub event's handler registered with an async hook
    When: The event is emitted through a callable bound with an async hook
    Then: Both hooks see a NoPayload payload
    """
    # Arrange
    log: list[str] = []
    bus.handle(_heartbeat, lambda: None, hooks=Hooks(before=(_alog(log, "handle"),)))

    # Act
    await bus.bind(_heartbeat, hooks=Hooks(before=(_alog(log, "publish"),)))()

    # Assert
    assert log == ["publish NoPayload", "handle NoPayload"]


# endregion


# region: Middleware


def _alabelled(log: list[str], label: str) -> Any:
    """Return an async context manager class logging under a label, and the envelope."""

    class Labelled:
        def __init__(self, call: PublishCall | HandleCall) -> None: ...

        async def __aenter__(self) -> None:
            log.append(f"{label} in {Envelope.current() is not None}")

        async def __aexit__(self, *exc: object) -> None:
            log.append(f"{label} out")

    return Labelled


async def test_middleware_wraps_hooks_inside_envelope():
    """
    Async middleware should wrap hooks, inside the emission's envelope.

    Given: A bus with envelopes and async publish and handle middleware, and a sync handler
        with an async hook
    When: An event is emitted with an async hook
    Then: Publish middleware wraps the emit's hook, handle middleware wraps the handler's
        hook, and both run inside an envelope
    """
    # Arrange
    log: list[str] = []
    bus = AsyncDirectBus(
        use_envelope=True,
        middleware=Middleware(
            publish=(_alabelled(log, "publish mw"),), handle=(_alabelled(log, "handle mw"),)
        ),
    )
    bus.handle(
        _task_created,
        lambda task: log.append("handler"),
        hooks=Hooks(before=(_alog(log, "h before"),)),
    )

    # Act
    await bus.emit(
        _task_created, None, _TaskCreated(1), hooks=Hooks(before=(_alog(log, "p before"),))
    )

    # Assert
    assert log == [
        "publish mw in True",
        "p before _TaskCreated",
        "handle mw in True",
        "h before _TaskCreated",
        "handler",
        "handle mw out",
        "publish mw out",
    ]


async def test_publish_middleware_without_hooks():
    """
    Publish middleware should wrap an emit that has no hooks.

    Given: A bus with async publish middleware, and an async responder
    When: A request is emitted without hooks
    Then: The middleware runs around it, and the reply is returned
    """
    # Arrange
    log: list[str] = []
    bus = AsyncDirectBus(middleware=Middleware(publish=(_alabelled(log, "mw"),)))

    async def respond(query: _BookQuery) -> str:
        return query.query.title()

    find_book = Request(_BookQuery, reply=str)
    bus.handle(find_book, respond)

    # Act
    reply = await bus.emit(find_book, None, _BookQuery("dune"))

    # Assert
    assert reply == "Dune"
    assert log == ["mw in False", "mw out"]


async def test_sync_middleware_entered_alongside_async():
    """
    AsyncDirectBus should enter sync middleware as well as async middleware.

    Given: A bus with sync contextmanager handle middleware outside async handle middleware
    When: An event is emitted
    Then: Both wrap the handler, the sync one outermost
    """
    # Arrange
    log: list[str] = []

    @contextmanager
    def sync(call: HandleCall) -> Generator[None, None, None]:
        log.append("sync in")
        yield
        log.append("sync out")

    bus = AsyncDirectBus(middleware=Middleware(handle=(sync, _alabelled(log, "async"))))
    bus.handle(_task_created, lambda task: log.append("handler"))

    # Act
    await bus.emit(_task_created, None, _TaskCreated(1))

    # Assert
    assert log == ["sync in", "async in False", "handler", "async out", "sync out"]


def test_non_manager_middleware_refused():
    """
    AsyncDirectBus should refuse a class that isn't a context manager when it's built.

    Given: Handle middleware that's a plain class
    When: An AsyncDirectBus is built with it
    Then: A TypeError says AsyncDirectBus can't enter it
    """

    # Arrange
    class Plain:
        def __init__(self, call: HandleCall) -> None: ...

    # Act / Assert
    with pytest.raises(TypeError, match="AsyncDirectBus can't enter"):
        AsyncDirectBus(middleware=Middleware(handle=(cast(Any, Plain),)))


# endregion
