"""
In-process event buses that dispatch straight to registered handlers, with no broker.

{py:class}`DirectBus` dispatches synchronously and rejects async handlers.
{py:class}`AsyncDirectBus` accepts both, running
{py:class}`PubSub <stratae.events.patterns.PubSub>` handlers concurrently. Register handlers
with `handle`, keyed by event, and create emitters with `bind`. A
{py:class}`Request <stratae.events.patterns.Request>` event needs exactly one responder, which
`handlers` can confirm before anything emits it.

Each bus delivers the patterns it has a dispatcher for, found by the event's class, so a
subclass of a supported pattern is delivered as that pattern. Add or replace a pattern with
`dispatchers=`, a list of {py:class}`Dispatch` pairs, fixed once the bus exists. A dispatcher
is checked against its pattern: pairing it with the wrong pattern, or returning something other
than what the pattern emits, is a type error. Registering or binding an event
of an unsupported pattern raises `TypeError`. With
`use_envelope=True`, each emission opens an
{py:class}`Envelope <stratae.events.envelope.Envelope>`.

````{example} Sending events over a DirectBus
```{code-block} python
from itertools import count
from types import SimpleNamespace
from stratae.events.direct import DirectBus
from stratae.events.patterns import PubSub, Request

class PlaceOrder:
    def __init__(self, customer: str, item: str) -> None:
        self.customer = customer
        self.item = item

class Order:
    def __init__(self, order_id: int, customer: str, item: str) -> None:
        self.order_id = order_id
        self.customer = customer
        self.item = item

class OrderPlaced:
    def __init__(self, order_id: int, item: str) -> None:
        self.order_id = order_id
        self.item = item

bus = DirectBus()
inventory = {"widget": 5}
reservations: list[str] = []
shipments: list[str] = []

place_order_event = Request(PlaceOrder, reply=Order)
order_placed_event = PubSub(OrderPlaced)

order = SimpleNamespace(
    place=bus.bind(place_order_event, factory=PlaceOrder),
    placed=bus.bind(order_placed_event, factory=OrderPlaced),
)

@bus.handle(order_placed_event)
def reserve_inventory(placed: OrderPlaced) -> None:
    inventory[placed.item] -= 1
    reservations.append(
        f"reserved 1 {placed.item}, {inventory[placed.item]} left"
    )

@bus.handle(order_placed_event)
def schedule_shipment(placed: OrderPlaced) -> None:
    shipments.append(f"scheduled shipment for order {placed.order_id}")

order_ids = count(1)

@bus.handle(place_order_event)
def handle_place_order(cmd: PlaceOrder) -> Order:
    new_order = Order(
        order_id=next(order_ids), customer=cmd.customer, item=cmd.item
    )
    order.placed(order_id=new_order.order_id, item=new_order.item)
    return new_order

print(f"place order responders: {len(bus.handlers(place_order_event))}")

customer_order = order.place(customer="ada", item="widget")
print(f"order {customer_order.order_id} placed for {customer_order.customer}")
print(reservations[0])
print(shipments[0])
```
```{output}
place order responders: 1
order 1 placed for ada
reserved 1 widget, 4 left
scheduled shipment for order 1
```
````
"""

from collections import defaultdict
from contextlib import nullcontext
from typing import Any, Awaitable, Callable, Collection, Protocol, overload

from stratae._callables import is_async_callable
from stratae.events._dispatch import (
    build_dispatcher,
    dispatch_request,
    dispatch_request_async,
    fan_out,
    fan_out_async,
    no_dispatcher,
)
from stratae.events.binding import AsyncBindMixin, BindMixin
from stratae.events.envelope import Envelope
from stratae.events.handler import Handler, WithoutPayload
from stratae.events.hooks import Hooks, run_hooks, run_hooks_async, with_handle_hooks
from stratae.events.patterns import NoPayload, PubSub, Request
from stratae.events.protocols import AnyEvent, EventType, PayloadEvent, SignalEvent

_AnyEvent = AnyEvent

_UNSUPPORTED_PATTERN = (
    "{bus} has no dispatcher for {pattern} events; register one with dispatchers="
)

_ASYNC_HANDLER_REJECTED = (
    "DirectBus dispatches synchronously and cannot await async handlers;"
    " register them on AsyncDirectBus instead"
)

_ASYNC_HOOKS_REJECTED = (
    "DirectBus runs hooks synchronously and cannot await async hooks for '{}';"
    " use AsyncDirectBus instead"
)


class _AsyncHandlerDecorator[S: Any, R](Protocol):
    """Decorator form of handle, returning the Handler."""

    @overload
    def __call__(
        self, fn: Callable[[S], Awaitable[R]]
    ) -> Handler[[S], _AnyEvent, Awaitable[R]]: ...
    @overload
    def __call__(self, fn: Callable[[S], R]) -> Handler[[S], _AnyEvent, R]: ...


class _AsyncSignalHandlerDecorator[R](Protocol):
    """Decorator form of handle for a schema-less event, returning the Handler."""

    @overload
    def __call__(self, fn: Callable[[], Awaitable[R]]) -> Handler[[], _AnyEvent, Awaitable[R]]: ...
    @overload
    def __call__(self, fn: Callable[[], R]) -> Handler[[], _AnyEvent, R]: ...


class Dispatch:
    """
    Pair a pattern class with the dispatcher a direct bus delivers it with.

    The dispatcher is called with the event, its handlers and the payload, and returns what
    emitting the event returns, or an awaitable resolving to it on an
    {py:class}`AsyncDirectBus`. Write it generic in the pattern's type parameters, e.g.
    `def gather[S, R](event: Gather[S, R], handlers: Collection[Handler[[S], Any, R]],
    payload: S) -> list[R]`, so its body is checked. A dispatcher whose first parameter
    doesn't accept `pattern` is a type error. That its declared return matches what the
    pattern emits is the dispatcher author's to ensure.
    """

    __slots__ = ("dispatcher", "pattern")

    def __init__[P: AnyEvent](
        self,
        pattern: type[P],
        dispatcher: Callable[[P, Collection[Handler[Any, Any, Any]], Any], Any],
    ) -> None:
        """
        Pair a pattern with its dispatcher.

        :param pattern: The pattern class. Its subclasses are dispatched the same way.
        :param dispatcher: Delivers an event of that pattern to its handlers.
        """
        self.pattern = pattern
        self.dispatcher = dispatcher


_SYNC_DEFAULTS = (Dispatch(Request, dispatch_request), Dispatch(PubSub, fan_out))
"""The patterns a DirectBus supports by default."""

_ASYNC_DEFAULTS = (Dispatch(Request, dispatch_request_async), Dispatch(PubSub, fan_out_async))
"""The patterns an AsyncDirectBus supports by default."""


class BaseDirectBus:
    """Handler registry and pattern dispatch shared by the direct buses."""

    def __init__(self, dispatches: Collection[Dispatch]) -> None:
        """Create an empty handler registry, delivering the given patterns."""
        self._handlers: dict[_AnyEvent, set[Handler[Any, _AnyEvent, Any]]] = defaultdict(set)
        self._dispatcher = build_dispatcher((d.pattern, d.dispatcher) for d in dispatches)

    def _check_event(
        self,
        event: _AnyEvent,
        hooks: Hooks[Any, Any] | None = None,  # noqa: S1172
    ) -> None:
        """Raise TypeError if this bus has no dispatcher for the event's pattern."""
        if self._dispatcher.dispatch(type(event)) is no_dispatcher:
            raise TypeError(
                _UNSUPPORTED_PATTERN.format(bus=type(self).__name__, pattern=type(event).__name__)
            )

    def _register(
        self, config: _AnyEvent, fn: Callable[..., Any], hooks: Hooks[Any, Any] | None
    ) -> Handler[Any, _AnyEvent, Any]:
        """Wrap fn and its hooks as a Handler, and store it in the registry under config."""
        self._check_event(config, hooks)
        call: Callable[..., Any] = WithoutPayload(fn) if config.is_signal else fn
        call, is_async = with_handle_hooks(config, hooks, call, is_async_callable(fn))
        handler: Handler[Any, _AnyEvent, Any] = Handler(call, config, is_async)
        self._handlers[config].add(handler)
        return handler

    def remove(self, handler: Handler[Any, _AnyEvent, Any]) -> None:
        """
        Remove a registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>` from `handle`.
        """
        self._handlers[handler.config].discard(handler)

    def handlers(self, event: _AnyEvent) -> tuple[Handler[Any, _AnyEvent, Any], ...]:
        """
        Return the handlers registered for an event, in no particular order.

        Use it to check registrations up front, such as that a request event has exactly one
        responder before anything emits it.

        :param event: The event to look up.
        :returns: A snapshot of the event's handlers, empty if it has none. Later calls to
            `handle` or `remove` don't change it.
        """
        return tuple(self._handlers.get(event, ()))


class DirectBus(BaseDirectBus, BindMixin[None]):
    """
    In-process, synchronous event bus.

    {py:class}`PubSub <stratae.events.patterns.PubSub>` events fan out to every handler.
    {py:class}`Request <stratae.events.patterns.Request>` events return their one responder's
    reply. Async handlers are rejected with `TypeError`.

    :param use_envelope: Open an {py:class}`Envelope <stratae.events.envelope.Envelope>`
        for each emission.
    :param dispatchers: {py:class}`Dispatch` pairs for more patterns, or replacing the
        defaults for `Request` and `PubSub`.
    """

    def __init__(
        self,
        *,
        use_envelope: bool = False,
        dispatchers: Collection[Dispatch] = (),
    ) -> None:
        """Create a bus, optionally with envelope tracking."""
        super().__init__((*_SYNC_DEFAULTS, *dispatchers))
        self._dispatch: Callable[[Any, _AnyEvent, Hooks[Any, Any] | None], Any] = (
            self._dispatch_in_envelope if use_envelope else self._dispatch_plain
        )

    def emit[S, R](
        self,
        event: EventType[S, R, Any, Any],
        config: None,  # noqa: S1172
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,  # noqa: S1172
        hooks: Hooks[S, R] | None = None,
    ) -> R:
        """
        Dispatch a payload to the event's handlers.

        Pub/sub handler failures are raised together as an {py:exc}`ExceptionGroup`. A
        responder's exception propagates directly.

        :param event: The event to dispatch.
        :param config: Unused.
        :param payload: The payload to dispatch.
        :param serializer: Unused.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this emit.
        :returns: The responder's reply for a request event, otherwise `None`.
        :raises NoResponderError: If a request event has no responder.
        :raises MultipleRespondersError: If a request event has more than one responder.
        :raises TypeError: If the bus has no dispatcher for the event's pattern, or `hooks`
            includes an async hook.
        """
        return self._dispatch(payload, event, hooks)

    @overload
    def handle[R](
        self,
        config: SignalEvent[Any, R],
        fn: Callable[[], R],
        *,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> Handler[[], _AnyEvent, R]: ...

    @overload
    def handle[R](
        self,
        config: SignalEvent[Any, R],
        fn: None = None,
        *,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> Callable[[Callable[[], R]], Handler[[], _AnyEvent, R]]: ...

    @overload
    def handle[S, R](
        self,
        config: PayloadEvent[S, Any, R],
        fn: Callable[[S], R],
        *,
        hooks: Hooks[S, R] | None = None,
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S, R](
        self,
        config: PayloadEvent[S, Any, R],
        fn: None = None,
        *,
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[[Callable[[S], R]], Handler[[S], _AnyEvent, R]]: ...

    def handle(
        self,
        config: _AnyEvent,
        fn: Callable[..., Any] | None = None,
        *,
        hooks: Hooks[Any, Any] | None = None,
    ) -> Any:
        """
        Register a sync handler for an event, as a decorator or direct call.

        A request event's handler returns the reply; a pub/sub handler's return is ignored.

        :param config: The event to handle.
        :param fn: The handler. If omitted, returns a decorator.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this handler.
        :returns: The {py:class}`Handler <stratae.events.handler.Handler>`, for `remove`.
        """
        if fn is not None:
            return self._register(config, fn, hooks)

        def decorator(f: Callable[..., Any]) -> Handler[..., _AnyEvent, Any]:
            return self._register(config, f, hooks)

        return decorator

    def _register(
        self, config: _AnyEvent, fn: Callable[..., Any], hooks: Hooks[Any, Any] | None
    ) -> Handler[Any, _AnyEvent, Any]:
        """Wrap fn as a Handler, raising TypeError if it's async."""
        if is_async_callable(fn):
            raise TypeError(_ASYNC_HANDLER_REJECTED)
        return super()._register(config, fn, hooks)

    def _check_event(self, event: _AnyEvent, hooks: Hooks[Any, Any] | None = None) -> None:
        """Raise TypeError for an unsupported pattern, or async hooks."""
        super()._check_event(event, hooks)
        if hooks is not None and hooks.has_async:
            raise TypeError(_ASYNC_HOOKS_REJECTED.format(event.name))

    def _dispatch_plain(self, payload: Any, event: _AnyEvent, hooks: Hooks[Any, Any] | None) -> Any:
        """Dispatch payload without an envelope, inside the hooks if any."""
        handlers = self._handlers.get(event, ())
        if hooks is None:
            return self._dispatcher(event, handlers, payload)
        if hooks.has_async:
            raise TypeError(_ASYNC_HOOKS_REJECTED.format(event.name))
        return run_hooks(hooks, event, payload, self._dispatcher, event, handlers, payload)

    def _dispatch_in_envelope(
        self, payload: Any, event: _AnyEvent, hooks: Hooks[Any, Any] | None
    ) -> Any:
        """Dispatch payload inside a new envelope."""
        with Envelope.scope():
            return self._dispatch_plain(payload, event, hooks)


class AsyncDirectBus(BaseDirectBus, AsyncBindMixin[None]):
    """
    In-process, asynchronous event bus.

    Accepts sync and async handlers. {py:class}`PubSub <stratae.events.patterns.PubSub>`
    events fan out concurrently. {py:class}`Request <stratae.events.patterns.Request>` events
    resolve to their one responder's reply.

    :param use_envelope: Open an {py:class}`Envelope <stratae.events.envelope.Envelope>`
        for each emission.
    :param dispatchers: {py:class}`Dispatch` pairs for more patterns, or replacing the
        defaults for `Request` and `PubSub`. Their dispatchers return awaitables.
    """

    def __init__(
        self,
        *,
        use_envelope: bool = False,
        dispatchers: Collection[Dispatch] = (),
    ) -> None:
        """Create a bus, optionally with envelope tracking."""
        super().__init__((*_ASYNC_DEFAULTS, *dispatchers))
        self._use_envelope = use_envelope

    async def emit[S, R](
        self,
        event: EventType[S, R, Any, Any],
        config: None,  # noqa: S1172
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,  # noqa: S1172
        hooks: Hooks[S, R] | None = None,
    ) -> R:
        """
        Dispatch a payload to the event's handlers.

        Pub/sub handler failures are raised together as an exception group. A responder's
        exception propagates directly.

        :param event: The event to dispatch.
        :param config: Unused.
        :param payload: The payload to dispatch.
        :param serializer: Unused.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this emit.
        :returns: The responder's reply for a request event, otherwise `None`.
        :raises NoResponderError: If a request event has no responder.
        :raises MultipleRespondersError: If a request event has more than one responder.
        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        handlers = self._handlers.get(event, ())
        with Envelope.scope() if self._use_envelope else nullcontext():
            if hooks is None:
                return await self._dispatcher(event, handlers, payload)
            return await run_hooks_async(
                hooks, event, payload, self._dispatcher, event, handlers, payload
            )

    @overload
    def handle[R](
        self,
        config: SignalEvent[Any, R],
        fn: Callable[[], Awaitable[R]],
        *,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> Handler[[], _AnyEvent, Awaitable[R]]: ...

    @overload
    def handle[R](
        self,
        config: SignalEvent[Any, R],
        fn: Callable[[], R],
        *,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> Handler[[], _AnyEvent, R]: ...

    @overload
    def handle[R](
        self,
        config: SignalEvent[Any, R],
        fn: None = None,
        *,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> _AsyncSignalHandlerDecorator[R]: ...

    @overload
    def handle[S, R](
        self,
        config: PayloadEvent[S, Any, R],
        fn: Callable[[S], Awaitable[R]],
        *,
        hooks: Hooks[S, R] | None = None,
    ) -> Handler[[S], _AnyEvent, Awaitable[R]]: ...

    @overload
    def handle[S, R](
        self,
        config: PayloadEvent[S, Any, R],
        fn: Callable[[S], R],
        *,
        hooks: Hooks[S, R] | None = None,
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S, R](
        self,
        config: PayloadEvent[S, Any, R],
        fn: None = None,
        *,
        hooks: Hooks[S, R] | None = None,
    ) -> _AsyncHandlerDecorator[S, R]: ...

    def handle(
        self,
        config: _AnyEvent,
        fn: Callable[..., Any] | None = None,
        *,
        hooks: Hooks[Any, Any] | None = None,
    ) -> Any:
        """
        Register a sync or async handler for an event, as a decorator or direct call.

        A request event's handler returns the reply; a pub/sub handler's return is ignored.

        :param config: The event to handle.
        :param fn: The handler. If omitted, returns a decorator.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this handler.
        :returns: The {py:class}`Handler <stratae.events.handler.Handler>`, for `remove`.
        """
        if fn is not None:
            return self._register(config, fn, hooks)

        def decorator(f: Callable[..., Any]) -> Handler[..., _AnyEvent, Any]:
            return self._register(config, f, hooks)

        return decorator
