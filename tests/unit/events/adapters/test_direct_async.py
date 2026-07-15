"""
Unit tests for the AsyncDirectBus adapter.

This test suite verifies the following behaviors:

AsyncDirectBus:
- bind returns an AsyncBoundEvent.
- Awaiting the AsyncBoundEvent dispatches the payload to a registered sync handler.
- Awaiting the AsyncBoundEvent dispatches the payload to a registered async handler.
- Mixed sync and async handlers on the same channel both receive the payload.
- All handlers on a channel receive the payload.
- Handlers on different channels are isolated from each other.
- handle returns a Handler.
- remove removes a handler; subsequent emits do not invoke it.
- The same callable may be registered multiple times independently.
- emit directly dispatches to dispatch.
- dispatch invokes all handlers registered on the channel.
- A raising handler does not prevent other handlers from running.
- All handler exceptions are collected and re-raised as an ExceptionGroup.

AsyncDirectBus (request events):
- emit awaits and returns an async responder's reply.
- emit returns a sync responder's reply.
- An AsyncBoundEvent for a request event resolves to the reply when awaited.
- emit raises NoResponderError when no responder is registered.
- emit raises MultipleRespondersError when several responders are registered.
- A responder's exception propagates directly, not as an ExceptionGroup.
- An async responder may be registered via the decorator form of handle.

AsyncDirectBus (with envelope):
- Handlers can access the Envelope during dispatch.
- Each top-level emission creates an independent envelope.
- A handler that emits an event receives a child envelope.
- The envelope is cleaned up after dispatch completes.
- A request reply is returned when envelope tracking is enabled.
"""

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from stratae.events.adapters.direct_async import AsyncDirectBus
from stratae.events.bound import AsyncBoundEvent
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


class _BookQuery:
    def __init__(self, query: str) -> None:
        self.query = query


class _BookResult:
    def __init__(self, title: str) -> None:
        self.title = title


_find_book = EventConfig(_BookQuery, Request[_BookResult])


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


def test_bind_returns_async_bound_event(bus: AsyncDirectBus):
    """
    ``bind`` should return an AsyncBoundEvent bound to bus.emit.

    Given: An AsyncDirectBus
    When: bind is called with an EventConfig
    Then: An AsyncBoundEvent should be returned
    """
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))

    assert isinstance(emit, AsyncBoundEvent)


async def test_dispatches_to_sync_handler(bus: AsyncDirectBus):
    """
    Awaiting an AsyncBoundEvent should dispatch the payload to a registered sync handler.

    Given: A sync handler registered via bus.handle
    When: The AsyncBoundEvent is awaited
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = Mock()
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    bus.handle(emit.event, handler)

    # Act
    await emit(task_id=1)

    # Assert
    handler.assert_called_once_with(_TaskCreated(1))


async def test_dispatches_to_async_handler(bus: AsyncDirectBus):
    """
    Awaiting an AsyncBoundEvent should dispatch the payload to a registered async handler.

    Given: An async handler registered via bus.handle
    When: The AsyncBoundEvent is awaited
    Then: The handler should be called with the constructed payload
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    bus.handle(emit.event, handler)

    # Act
    await emit(task_id=2)

    # Assert
    handler.assert_called_once_with(_TaskCreated(2))


async def test_dispatches_to_mixed_handlers(bus: AsyncDirectBus):
    """
    Both sync and async handlers registered to the same EventConfig should receive the payload.

    Given: A sync and an async handler registered to the same EventConfig
    When: An event is emitted
    Then: Both handlers should be called with the payload
    """
    # Arrange
    sync_handler = Mock()
    async_handler = AsyncMock()
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    bus.handle(emit.event, sync_handler)
    bus.handle(emit.event, async_handler)

    # Act
    await emit(task_id=3)

    # Assert
    sync_handler.assert_called_once_with(_TaskCreated(3))
    async_handler.assert_called_once_with(_TaskCreated(3))


async def test_dispatches_to_all_handlers_on_channel(bus: AsyncDirectBus):
    """
    All handlers registered on the same EventConfig should receive the payload.

    Given: Two async handlers registered to the same EventConfig
    When: An event is emitted
    Then: Both handlers should be called with the payload
    """
    # Arrange
    handler_a = AsyncMock()
    handler_b = AsyncMock()
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    bus.handle(emit.event, handler_a)
    bus.handle(emit.event, handler_b)

    # Act
    await emit(task_id=4)

    # Assert
    handler_a.assert_called_once_with(_TaskCreated(4))
    handler_b.assert_called_once_with(_TaskCreated(4))


async def test_channel_isolation(bus: AsyncDirectBus):
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
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    handler = bus.handle(emit.event, fn)

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
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    handle = bus.handle(emit.event, handler)
    bus.remove(handle)

    # Act
    await emit(task_id=6)

    # Assert
    handler.assert_not_called()


async def test_same_callable_registered_twice_called_twice(bus: AsyncDirectBus):
    """
    Registering the same callable twice should produce two independent registrations.

    Given: The same callable registered to an EventConfig twice
    When: An event is emitted
    Then: The callable should be invoked twice
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    bus.handle(emit.event, handler)
    bus.handle(emit.event, handler)

    # Act
    await emit(task_id=7)

    # Assert
    assert handler.call_count == 2


async def test_emit_dispatches_directly(bus: AsyncDirectBus):
    """
    ``emit`` should dispatch the payload directly to all registered handlers.

    Given: A handler registered to an EventConfig
    When: emit is called directly with that EventConfig
    Then: The handler should receive the payload
    """
    # Arrange
    handler = AsyncMock()
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    bus.handle(emit.event, handler)
    payload = _TaskCreated(8)

    # Act
    await bus.emit(payload, emit.event)

    # Assert
    handler.assert_called_once_with(payload)


async def test_dispatch_invokes_all_handlers(bus: AsyncDirectBus):
    """
    ``dispatch`` should invoke every handler registered for the given EventConfig.

    Given: Two handlers registered to an EventConfig
    When: dispatch is called directly
    Then: Both handlers should receive the payload
    """
    # Arrange
    handler_a = AsyncMock()
    handler_b = AsyncMock()
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    bus.handle(emit.event, handler_a)
    bus.handle(emit.event, handler_b)
    payload = _TaskCreated(9)

    # Act
    await bus.dispatch(payload, config=emit.event)

    # Assert
    handler_a.assert_called_once_with(payload)
    handler_b.assert_called_once_with(payload)


async def test_raising_handler_does_not_prevent_other_handlers(bus: AsyncDirectBus):
    """
    A handler that raises should not prevent subsequent handlers from running.

    Given: Two handlers registered to an EventConfig, the first of which raises
    When: an event is emitted
    Then: The second handler should still be called
    """
    # Arrange
    second_handler = AsyncMock()
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    bus.handle(emit.event, AsyncMock(side_effect=ValueError("boom")))
    bus.handle(emit.event, second_handler)
    payload = _TaskCreated(10)

    # Act
    with pytest.raises(ExceptionGroup):
        await bus.dispatch(payload, config=emit.event)

    # Assert
    second_handler.assert_called_once_with(payload)


async def test_handler_exceptions_collected_into_exception_group(bus: AsyncDirectBus):
    """
    All handler exceptions should be collected and raised together as an ExceptionGroup.

    Given: Two handlers registered to an EventConfig, both of which raise
    When: an event is emitted
    Then: An ExceptionGroup containing both exceptions should be raised
    """
    # Arrange
    error_a = ValueError("first")
    error_b = RuntimeError("second")
    emit = bus.bind(EventConfig(_TaskCreated, PubSub))
    bus.handle(emit.event, AsyncMock(side_effect=error_a))
    bus.handle(emit.event, AsyncMock(side_effect=error_b))
    payload = _TaskCreated(11)

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exc_info:
        await bus.dispatch(payload, config=emit.event)

    assert set(exc_info.value.exceptions) == {error_a, error_b}


async def test_request_emit_returns_async_responder_reply(bus: AsyncDirectBus):
    """
    ``emit`` should await and return an async responder's reply for a request event.

    Given: An async responder registered to a request EventConfig
    When: emit is awaited with that EventConfig
    Then: The responder's resolved return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus.handle(_find_book, AsyncMock(return_value=reply))

    # Act
    result = await bus.emit(_BookQuery("dune"), _find_book)

    # Assert
    assert result is reply


async def test_request_emit_returns_sync_responder_reply(bus: AsyncDirectBus):
    """
    ``emit`` should return a sync responder's reply for a request event.

    Given: A sync responder registered to a request EventConfig
    When: emit is awaited with that EventConfig
    Then: The responder's return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus.handle(_find_book, Mock(return_value=reply))

    # Act
    result = await bus.emit(_BookQuery("dune"), _find_book)

    # Assert
    assert result is reply


async def test_bound_request_event_resolves_to_reply(bus: AsyncDirectBus):
    """
    Awaiting an AsyncBoundEvent for a request event should resolve to the responder's reply.

    Given: An async responder registered to a request EventConfig bound via bus.bind
    When: The AsyncBoundEvent is awaited
    Then: The responder's resolved return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    find_book = bus.bind(_find_book)
    bus.handle(find_book.event, AsyncMock(return_value=reply))

    # Act
    result = await find_book(query="dune")

    # Assert
    assert result is reply


async def test_request_emit_raises_without_responder(bus: AsyncDirectBus):
    """
    ``emit`` should raise NoResponderError when a request event has no responder.

    Given: A request EventConfig with no registered responder
    When: emit is awaited with that EventConfig
    Then: A NoResponderError should be raised
    """
    with pytest.raises(NoResponderError):
        await bus.emit(_BookQuery("dune"), _find_book)


async def test_request_emit_raises_with_multiple_responders(bus: AsyncDirectBus):
    """
    ``emit`` should raise MultipleRespondersError when several responders are registered.

    Given: Two responders registered to a request EventConfig
    When: emit is awaited with that EventConfig
    Then: A MultipleRespondersError should be raised
    """
    # Arrange
    bus.handle(_find_book, AsyncMock(return_value=_BookResult("first")))
    bus.handle(_find_book, AsyncMock(return_value=_BookResult("second")))

    # Act / Assert
    with pytest.raises(MultipleRespondersError):
        await bus.emit(_BookQuery("dune"), _find_book)


async def test_request_responder_exception_propagates_directly(bus: AsyncDirectBus):
    """
    A responder's exception should propagate to the emitter unwrapped.

    Given: An async responder that raises, registered to a request EventConfig
    When: emit is awaited with that EventConfig
    Then: The responder's exception should be raised directly, not as an ExceptionGroup
    """
    # Arrange
    error = ValueError("boom")
    bus.handle(_find_book, AsyncMock(side_effect=error))

    # Act / Assert
    with pytest.raises(ValueError) as exc_info:
        await bus.emit(_BookQuery("dune"), _find_book)

    assert exc_info.value is error


async def test_request_responder_registered_via_decorator(bus: AsyncDirectBus):
    """
    An async responder registered via the decorator form of handle should serve requests.

    Given: An async responder registered with @bus.handle on a request EventConfig
    When: emit is awaited with that EventConfig
    Then: The responder's resolved return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")

    @bus.handle(_find_book)
    async def _(_: _BookQuery) -> _BookResult:
        return reply

    # Act
    result = await bus.emit(_BookQuery("dune"), _find_book)

    # Assert
    assert result is reply


async def test_handler_can_access_envelope_during_dispatch(bus_with_envelope: AsyncDirectBus):
    """
    Handlers should be able to access a valid Envelope during dispatch.

    Given: A handler that captures the current envelope
    When: An event is emitted
    Then: The captured value should be an Envelope instance
    """
    # Arrange
    emit = bus_with_envelope.bind(EventConfig(_TaskCreated, PubSub))
    captured: list[Envelope] = []

    @bus_with_envelope.handle(emit.event)
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
    emit = bus_with_envelope.bind(EventConfig(_TaskCreated, PubSub))
    captured: list[Envelope] = []

    @bus_with_envelope.handle(emit.event)
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
    emit = bus_with_envelope.bind(EventConfig(_TaskCreated, PubSub))
    bus_with_envelope.handle(emit.event, AsyncMock())

    # Act
    await emit(task_id=1)

    # Assert
    assert Envelope.current() is None


async def test_request_reply_returned_with_envelope(bus_with_envelope: AsyncDirectBus):
    """
    A request event should resolve to its reply when envelope tracking is enabled.

    Given: An async responder registered to a request EventConfig on an envelope-tracking bus
    When: emit is awaited with that EventConfig
    Then: The responder's resolved return value should be returned
    """
    # Arrange
    reply = _BookResult("Dune")
    bus_with_envelope.handle(_find_book, AsyncMock(return_value=reply))

    # Act
    result = await bus_with_envelope.emit(_BookQuery("dune"), _find_book)

    # Assert
    assert result is reply
