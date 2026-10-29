"""
Static typing tests for the event patterns, checked by pyright as part of `just check`.

Each test also runs, so a wrong runtime value fails too, but its purpose is the
`assert_type` calls: pyright fails the check if an inferred type drifts.

This test suite verifies the following behaviors:

Event patterns:
- Constructors infer the payload type, reply type and signal flag.
- A request without a reply type replies None.

DirectBus:
- emit and bound callables return a request's reply type, and None for pub/sub.
- A payload-less event binds to a zero-argument callable.
- A factory-bound callable takes the factory's parameters.
- handle returns a Handler typed by the handler's payload and reply.

AsyncDirectBus:
- emit and bound callables resolve to a request's reply type, and None for pub/sub.
- A payload-less event binds to a zero-argument coroutine function.

Hooks:
- Hooks passed to handle, bind and emit are typed by the event's payload and reply.
- A hook typed loosely, for any event, fits every event.

Built-in dispatchers:
- Each fits its pattern for any payload and reply type, returning what the pattern emits.
"""

from typing import Any, Awaitable, Callable, Collection, Literal, assert_type

from stratae.events import AsyncDirectBus, DirectBus, Handler, Hooks, NoPayload, PubSub, Request
from stratae.events._dispatch import (
    dispatch_request,
    dispatch_request_async,
    fan_out,
    fan_out_async,
)
from stratae.events.protocols import EventType

type _AnyEvent = EventType[Any, Any, Any, Any]


class _PlaceOrder:
    def __init__(self, item: str) -> None:
        self.item = item


class _Order:
    def __init__(self, item: str) -> None:
        self.item = item


class _OrderPlaced:
    def __init__(self, item: str) -> None:
        self.item = item


_place_order = Request(_PlaceOrder, reply=_Order)
_cancel_order = Request(_PlaceOrder)
_order_placed = PubSub(_OrderPlaced)
_ping = Request(reply=str, name="ping")
_heartbeat = PubSub(name="heartbeat")


def _make_order(command: _PlaceOrder) -> _Order:
    return _Order(command.item)


def _cancel(_: _PlaceOrder) -> None:
    return None


def _on_placed(_: _OrderPlaced) -> None:
    return None


def _pong() -> str:
    return "pong"


def _beat() -> None:
    return None


def test_constructors_infer_event_types() -> None:
    """
    Pattern constructors should infer every type parameter from their arguments.

    Given: Request and PubSub events with and without payloads and replies
    When: Their types are inspected
    Then: Each carries its payload type, reply type and signal flag
    """
    # Assert
    assert_type(_place_order, Request[_PlaceOrder, _Order, Literal[False]])
    assert_type(_cancel_order, Request[_PlaceOrder, None, Literal[False]])
    assert_type(_order_placed, PubSub[_OrderPlaced, Literal[False]])
    assert_type(_ping, Request[NoPayload, str, Literal[True]])
    assert_type(_heartbeat, PubSub[NoPayload, Literal[True]])


def test_patterns_satisfy_event_type() -> None:
    """
    Every pattern should satisfy EventType with its emit and handler reply types.

    Given: A request, a command and a pub/sub event
    When: Each is assigned to an EventType annotation
    Then: The assignments type-check
    """
    # Act
    place_order: EventType[_PlaceOrder, _Order, _Order, Literal[False]] = _place_order
    cancel_order: EventType[_PlaceOrder, None, None, Literal[False]] = _cancel_order
    order_placed: EventType[_OrderPlaced, None, Any, Literal[False]] = _order_placed

    # Assert
    assert place_order is _place_order
    assert cancel_order is _cancel_order
    assert order_placed is _order_placed


def test_direct_bus_types() -> None:
    """
    DirectBus emit, bind and handle should carry the event's types.

    Given: A DirectBus with handlers for each kind of event
    When: Events are emitted directly and through bound callables
    Then: Each result has the event's emit reply type
    """
    # Arrange
    bus = DirectBus()
    assert_type(bus.handle(_place_order, _make_order), Handler[[_PlaceOrder], _AnyEvent, _Order])
    bus.handle(_cancel_order, _cancel)
    bus.handle(_order_placed, _on_placed)
    assert_type(bus.handle(_ping, _pong), Handler[[], _AnyEvent, str])
    bus.handle(_heartbeat, _beat)

    place = bus.bind(_place_order, factory=_PlaceOrder)
    place_built = bus.bind(_place_order)
    ping = bus.bind(_ping)
    beat = bus.bind(_heartbeat)

    # Act
    order = bus.emit(_place_order, None, _PlaceOrder("lamp"))
    cancelled = bus.emit(_cancel_order, None, _PlaceOrder("lamp"))
    published = bus.emit(_order_placed, None, _OrderPlaced("lamp"))

    # Assert
    assert_type(order, _Order)
    assert_type(cancelled, None)
    assert_type(published, None)
    assert_type(place(item="desk"), _Order)
    assert_type(place_built, Callable[[_PlaceOrder], _Order])
    assert_type(ping, Callable[[], str])
    assert_type(beat, Callable[[], None])
    assert order.item == "lamp"
    assert place(item="desk").item == "desk"
    assert ping() == "pong"


async def test_async_direct_bus_types() -> None:
    """
    AsyncDirectBus emit and bind should carry the event's types.

    Given: An AsyncDirectBus with an async responder and a payload-less responder
    When: Events are emitted directly and through bound callables
    Then: Each awaited result has the event's emit reply type
    """

    # Arrange
    async def make_order(command: _PlaceOrder) -> _Order:
        return _Order(command.item)

    bus = AsyncDirectBus()
    assert_type(
        bus.handle(_place_order, make_order), Handler[[_PlaceOrder], _AnyEvent, Awaitable[_Order]]
    )
    bus.handle(_order_placed, _on_placed)
    bus.handle(_ping, _pong)

    place = bus.bind(_place_order, factory=_PlaceOrder)
    ping = bus.bind(_ping)

    # Act
    order = await bus.emit(_place_order, None, _PlaceOrder("lamp"))
    published = await bus.emit(_order_placed, None, _OrderPlaced("lamp"))

    # Assert
    assert_type(order, _Order)
    assert_type(published, None)
    assert_type(await place(item="desk"), _Order)
    assert_type(ping, Callable[[], Awaitable[str]])
    assert (await place(item="desk")).item == "desk"
    assert await ping() == "pong"


class _S:
    """An opaque payload type the built-in dispatchers know nothing about."""


class _R:
    """An opaque reply type the built-in dispatchers know nothing about."""


def test_builtin_dispatchers_fit_their_patterns() -> None:
    """
    Each built-in dispatcher should return what its pattern emits, for any payload and reply.

    Given: The built-in dispatchers, and opaque payload and reply types they can't special-case
    When: Each is assigned to the signature its pattern implies, with those types
    Then: The assignments type-check, so each works for every payload and reply type and
        returns the pattern's emit type: the reply for a request, nothing for pub/sub
    """
    # Act
    request: Callable[[Request[_S, _R, Any], Collection[Handler[[_S], Any, _R]], _S], _R] = (
        dispatch_request
    )
    request_async: Callable[
        [Request[_S, _R, Any], Collection[Handler[[_S], Any, _R | Awaitable[_R]]], _S],
        Awaitable[_R],
    ] = dispatch_request_async
    pubsub: Callable[[PubSub[_S, Any], Collection[Handler[[_S], Any, object]], _S], None] = fan_out
    pubsub_async: Callable[
        [PubSub[_S, Any], Collection[Handler[[_S], Any, Any]], _S], Awaitable[None]
    ] = fan_out_async

    # Assert
    assert (request, request_async) == (dispatch_request, dispatch_request_async)
    assert (pubsub, pubsub_async) == (fan_out, fan_out_async)


def _audit_any(event: _AnyEvent, payload: object, reply: object) -> None:
    """Audit a success, for any event."""


def _audit_order(event: _AnyEvent, payload: _PlaceOrder, reply: _Order) -> None:
    """Audit a placed order."""


def test_hooks_typed_by_event() -> None:
    """
    Hooks passed to a bus should be typed by the event's payload and reply.

    Given: Hooks typed for placing an order, and a hook typed for any event
    When: They're passed to handle, bind and emit for matching events
    Then: The calls type-check, so pyright accepts hooks that fit the event
    """
    # Arrange
    bus = DirectBus()
    order_hooks: Hooks[_PlaceOrder, _Order] = Hooks(success=(_audit_order, _audit_any))
    any_hooks: Hooks[Any, Any] = Hooks(success=(_audit_any,))

    # Act
    bus.handle(_place_order, _make_order, hooks=order_hooks)
    place = bus.bind(_place_order, factory=_PlaceOrder, hooks=order_hooks)
    order = bus.emit(_place_order, None, _PlaceOrder("lamp"), hooks=order_hooks)
    bus.handle(_order_placed, _on_placed, hooks=Hooks(success=(_audit_any,)))
    bus.emit(_order_placed, None, _OrderPlaced("lamp"), hooks=any_hooks)
    bus.handle(_ping, _pong, hooks=Hooks(success=(_audit_any,)))

    # Assert
    assert_type(place(item="desk"), _Order)
    assert_type(order, _Order)
