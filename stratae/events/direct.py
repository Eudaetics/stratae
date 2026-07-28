"""
In-process event buses that dispatch straight to registered handlers, with no broker.

{py:class}`BaseDirectBus` holds the handler registry and single-responder
lookup shared by both bus adapters below. It raises
{py:exc}`NoResponderError <stratae.events.exceptions.NoResponderError>` when a
{py:class}`Request <stratae.events.event.Request>` event has no registered
responder, and {py:exc}`MultipleRespondersError <stratae.events.exceptions.MultipleRespondersError>`
when it has more than one.

{py:class}`DirectBus` dispatches synchronously. Bind its `emit` method to an
{py:class}`Event <stratae.events.event.Event>` via
{py:func}`bind <stratae.events.bound.bind>` to get a callable that
dispatches through it, optionally building the payload from a factory.
Registering an async handler on it raises `TypeError`, since there is no
way to await one from inside synchronous dispatch.

{py:class}`AsyncDirectBus` dispatches through `asyncio` instead. Bind its
`emit` method via {py:func}`abind <stratae.events.bound.abind>` to get an
awaitable callable the same way. It accepts a mix of sync and async
handlers, running
{py:class}`PubSub <stratae.events.event.PubSub>` handlers concurrently with
`asyncio.gather`.

Both adapters register handlers with `handle`, using the same `Event` as
the routing key, and can open a scoped
{py:class}`Envelope <stratae.events.envelope.Envelope>` for each emission when
constructed with `use_envelope=True`.

````{example} Sending events over a DirectBus
```{code-block} python
from itertools import count
from types import SimpleNamespace
from stratae.events.direct import DirectBus
from stratae.events.event import Event, PubSub, Request

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

place_order_event = Event(Request[Order], PlaceOrder)
order_placed_event = Event(PubSub, OrderPlaced)

# Grouping the events for later simplicity
order = SimpleNamespace(
    # DirectBus.bind doesn't need a separate routing config
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

customer_order = order.place(customer="ada", item="widget")
print(f"order {customer_order.order_id} placed for {customer_order.customer}")
print(reservations[0])
print(shipments[0])
```
```{output}
order 1 placed for ada
reserved 1 widget, 4 left
scheduled shipment for order 1
```
````
"""

import asyncio
from collections import defaultdict
from inspect import iscoroutinefunction
from typing import Any, Awaitable, Callable, Protocol, overload

from stratae.events.bound import abind, bind
from stratae.events.envelope import Envelope
from stratae.events.event import DispatchPattern, Event, is_request
from stratae.events.exceptions import MultipleRespondersError, NoResponderError
from stratae.events.handler import Handler

_AnyEvent = Event[Any, Any]

_ASYNC_HANDLER_REJECTED = (
    "DirectBus dispatches synchronously and cannot await async handlers;"
    " register them on AsyncDirectBus instead"
)


class _AsyncHandlerDecorator[S: Any, R](Protocol):
    """Decorator form of handle: registers and returns the Handler, sync or async fn."""

    @overload
    def __call__(
        self, fn: Callable[[S], Awaitable[R]]
    ) -> Handler[[S], _AnyEvent, Awaitable[R]]: ...
    @overload
    def __call__(self, fn: Callable[[S], R]) -> Handler[[S], _AnyEvent, R]: ...


class BaseDirectBus:
    """
    Hold the handler registry and responder resolution shared by both direct bus adapters.

    {py:class}`DirectBus` and {py:class}`AsyncDirectBus` each own their
    emit/bind surfaces, typed `handle` overloads, and dispatch semantics.
    Both delegate registration to `_register` and share the config-keyed
    handler dict and single-responder lookup for
    {py:class}`Request <stratae.events.event.Request>` events defined here.
    """

    def __init__(self) -> None:
        """Initialise the handler registry."""
        self._handlers: dict[_AnyEvent, set[Handler[Any, _AnyEvent, Any]]] = defaultdict(set)

    def _register[**P, R](self, config: _AnyEvent, fn: Callable[P, R]) -> Handler[P, _AnyEvent, R]:
        """Wrap fn as a Handler and store it in the registry under config."""
        handler: Handler[P, _AnyEvent, R] = Handler(fn, config)
        self._handlers[config].add(handler)
        return handler

    def remove(self, handler: Handler[Any, _AnyEvent, Any]) -> None:
        """
        Remove a previously registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>`
            instance a prior `handle` call returned.
        """
        self._handlers[handler.config].discard(handler)

    def _single_responder(self, event: _AnyEvent) -> Handler[Any, _AnyEvent, Any]:
        """Return event's sole registered responder, or raise if there isn't exactly one."""
        responders = self._handlers.get(event)
        if not responders:
            raise NoResponderError(f"no responder registered for request event '{event.name}'")
        if len(responders) > 1:
            raise MultipleRespondersError(
                f"request event '{event.name}' has {len(responders)} responders;"
                " exactly one is required"
            )
        (responder,) = responders
        return responder


class DirectBus(BaseDirectBus):
    """
    In-process, synchronous event bus with no routing config.

    Bind `DirectBus.emit` to an {py:class}`Event <stratae.events.event.Event>`
    via `bind` to create a callable that dispatches payloads to registered
    handlers, optionally building them from a factory. Register handlers
    with `handle`, using the same `Event` as the routing key. Each `handle`
    call is an independent registration; the same callable may be
    registered multiple times.

    {py:class}`PubSub <stratae.events.event.PubSub>` events fan out to every
    registered handler and emit returns `None`.
    {py:class}`Request <stratae.events.event.Request>` events block until the
    registered responder returns and emit returns its reply; a request event
    must have exactly one responder at emit time, otherwise
    {py:exc}`NoResponderError <stratae.events.exceptions.NoResponderError>` or
    {py:exc}`MultipleRespondersError <stratae.events.exceptions.MultipleRespondersError>`
    is raised. Handlers must be synchronous callables; registering an async
    handler raises `TypeError`.

    :param use_envelope: When `True`, each emission opens a scoped
        {py:class}`Envelope <stratae.events.envelope.Envelope>` for
        correlation tracking. Defaults to `False` for pure in-process
        dispatch with no envelope overhead.
    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        super().__init__()
        self._dispatch: Callable[[Any, _AnyEvent], Any] = (
            self._dispatch_in_envelope if use_envelope else self._dispatch_plain
        )

    @overload
    def bind[**P, S, R](
        self, event: Event[DispatchPattern[R, Any], S], *, factory: Callable[P, S]
    ) -> Callable[P, R]: ...

    @overload
    def bind[S, R](self, event: Event[DispatchPattern[R, Any], S]) -> Callable[[S], R]: ...

    def bind(
        self, event: _AnyEvent, *, factory: Callable[..., Any] | None = None
    ) -> Callable[..., Any]:
        """
        Return a callable bound to this bus's `emit` and `event`, with config=None.

        :param event: The {py:class}`Event <stratae.events.event.Event>` to bind.
        :param factory: Builds the payload from the bound call's arguments.
            Omit it to pass an already-built payload straight through
            instead.
        :returns: A callable that builds the payload via `factory` when
            given, otherwise one that forwards an already-built payload
            straight through; either way wrapping this bus's `emit` and
            `event`.
        """
        return bind(self.emit, event, factory=factory, config=None, serializer=None)

    def emit[S, R](
        self,
        event: Event[DispatchPattern[R, Any], S],
        config: None,  # noqa: S1172
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,  # noqa: S1172
    ) -> R:
        """
        Dispatch the payload to registered handlers, opening an envelope scope if configured.

        {py:class}`PubSub <stratae.events.event.PubSub>` events fan out to
        every registered handler; handler exceptions are collected into an
        {py:exc}`ExceptionGroup`. {py:class}`Request <stratae.events.event.Request>`
        events dispatch to exactly one responder, block until it returns,
        and propagate its exceptions directly.

        :param event: The {py:class}`Event <stratae.events.event.Event>`
            used as the handler lookup key.
        :param config: Unused; `DirectBus` requires no routing config.
        :param payload: The constructed payload instance to dispatch.
        :param serializer: Unused; `DirectBus` requires no serializer.
        :returns: The responder's reply for request events; `None` for pub/sub.
        :raises NoResponderError: When a request event has no registered
            responder.
        :raises MultipleRespondersError: When a request event has more than
            one registered responder.
        """
        return self._dispatch(payload, event)

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S],
        fn: Callable[[S], R],
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S],
        fn: None = None,
    ) -> Callable[[Callable[[S], R]], Handler[[S], _AnyEvent, R]]: ...

    def handle(
        self,
        config: _AnyEvent,
        fn: Callable[..., Any] | None = None,
    ) -> Any:
        """
        Register a handler for a config, as a decorator or direct call.

        For request events the callable is the responder: it accepts the
        event's payload and must return the event's reply type. For pub/sub
        events the callable accepts the payload and its return value is
        ignored. Async callables are rejected with `TypeError` on this
        synchronous bus; register them on {py:class}`AsyncDirectBus` instead.

        Returns the {py:class}`Handler <stratae.events.handler.Handler>`
        instance in both forms so callers can pass it to `remove` later.

        :param config: The {py:class}`Event <stratae.events.event.Event>`
            used as the handler routing key.
        :param fn: When supplied, registers `fn` directly and returns its
            `Handler`. When omitted, returns a decorator that registers and
            returns the `Handler`.
        """
        if fn is not None:
            return self._register(config, fn)

        def decorator(f: Callable[..., Any]) -> Handler[..., _AnyEvent, Any]:
            return self._register(config, f)

        return decorator

    def _register[**P, R](self, config: _AnyEvent, fn: Callable[P, R]) -> Handler[P, _AnyEvent, R]:
        """Wrap fn as a Handler, rejecting it with TypeError if it's async."""
        if iscoroutinefunction(fn):
            raise TypeError(_ASYNC_HANDLER_REJECTED)
        return super()._register(config, fn)

    def _dispatch_plain(self, payload: Any, event: _AnyEvent) -> Any:
        """Dispatch payload as a request or pub/sub fan-out, without an envelope scope."""
        if is_request(event):
            return self._dispatch_request(payload, event)
        self._dispatch_fanout(payload, event)
        return None

    def _dispatch_fanout(self, payload: Any, event: _AnyEvent) -> None:
        """Call every handler registered for event, gathering their failures into one group."""
        exceptions: list[Exception] = []
        handlers = list(self._handlers.get(event, ()))
        for handler in handlers:
            try:
                handler(payload)
            except Exception as exc:
                exceptions.append(exc)
        if exceptions:
            raise ExceptionGroup("Handler Errors", exceptions)

    def _dispatch_request(self, payload: Any, event: _AnyEvent) -> Any:
        """Call event's sole responder with payload and return its reply."""
        return self._single_responder(event)(payload)

    def _dispatch_in_envelope(self, payload: Any, event: _AnyEvent) -> Any:
        """Dispatch payload for event inside a freshly opened envelope scope."""
        with Envelope.scope():
            return self._dispatch_plain(payload, event)


class AsyncDirectBus(BaseDirectBus):
    """
    In-process, asynchronous event bus with no routing config.

    Bind `AsyncDirectBus.emit` to an {py:class}`Event <stratae.events.event.Event>`
    via `abind` to create a callable that dispatches payloads to registered
    handlers, optionally building them from a factory. Register handlers
    with `handle`, using the same `Event` as the routing key. Sync and
    async handlers are both supported; all are dispatched concurrently via
    `asyncio.gather`. Each `handle` call is an independent registration; the
    same callable may be registered multiple times.

    {py:class}`PubSub <stratae.events.event.PubSub>` events fan out to every
    registered handler and emit resolves to `None`.
    {py:class}`Request <stratae.events.event.Request>` events dispatch to the
    registered responder, sync or async, and emit resolves to its reply; a
    request event must have exactly one responder at emit time, otherwise
    {py:exc}`NoResponderError <stratae.events.exceptions.NoResponderError>` or
    {py:exc}`MultipleRespondersError <stratae.events.exceptions.MultipleRespondersError>`
    is raised.

    :param use_envelope: When `True`, each emission opens a scoped
        {py:class}`Envelope <stratae.events.envelope.Envelope>` for
        correlation tracking. Defaults to `False` for pure in-process
        dispatch with no envelope overhead.
    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        super().__init__()
        self._use_envelope = use_envelope

    @overload
    def bind[**P, S, R](
        self,
        event: Event[DispatchPattern[R, Any], S],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
    ) -> Callable[P, Awaitable[R]]: ...

    @overload
    def bind[S, R](
        self, event: Event[DispatchPattern[R, Any], S]
    ) -> Callable[[S], Awaitable[R]]: ...

    def bind(
        self, event: _AnyEvent, *, factory: Callable[..., Any] | None = None
    ) -> Callable[..., Awaitable[Any]]:
        """
        Return an awaitable callable bound to this bus's `emit` and `event`, with config=None.

        :param event: The {py:class}`Event <stratae.events.event.Event>` to bind.
        :param factory: Builds the payload from the bound call's arguments,
            sync or async. Omit it to pass an already-built payload
            straight through instead.
        :returns: A callable that builds the payload via `factory`, sync or
            async, when given, otherwise one that forwards an already-built
            payload straight through; either way wrapping this bus's `emit`
            and `event`, and resolving to its result once awaited.
        """
        return abind(self.emit, event, factory=factory, config=None, serializer=None)

    async def emit[S, R](
        self,
        event: Event[DispatchPattern[R, Any], S],
        config: None,  # noqa: S1172
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,  # noqa: S1172
    ) -> R:
        """
        Dispatch the payload to registered handlers, opening an envelope scope if configured.

        When constructed with `use_envelope=True`, each emission runs inside
        its own {py:class}`Envelope <stratae.events.envelope.Envelope>`, or a
        child of the currently active one, enabling correlation across
        nested emissions.

        {py:class}`PubSub <stratae.events.event.PubSub>` events fan out to
        every registered handler concurrently; handler exceptions are
        collected into an {py:exc}`ExceptionGroup`.
        {py:class}`Request <stratae.events.event.Request>` events dispatch to
        exactly one responder, await its reply, and propagate its exceptions
        directly.

        :param event: The {py:class}`Event <stratae.events.event.Event>`
            used as the handler lookup key.
        :param config: Unused; `AsyncDirectBus` requires no routing config.
        :param payload: The constructed payload instance to dispatch.
        :param serializer: Unused; `AsyncDirectBus` requires no serializer.
        :returns: The responder's reply for request events; `None` for pub/sub.
        :raises NoResponderError: When a request event has no registered
            responder.
        :raises MultipleRespondersError: When a request event has more than
            one responder.
        """
        if self._use_envelope:
            with Envelope.scope():
                return await self._dispatch(payload, event)
        return await self._dispatch(payload, event)

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S],
        fn: Callable[[S], Awaitable[R]],
    ) -> Handler[[S], _AnyEvent, Awaitable[R]]: ...

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S],
        fn: Callable[[S], R],
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S],
        fn: None = None,
    ) -> _AsyncHandlerDecorator[S, R]: ...

    def handle(
        self,
        config: _AnyEvent,
        fn: Callable[..., Any] | None = None,
    ) -> Any:
        """
        Register a handler for a config, as a decorator or direct call.

        For request events the callable is the responder: it accepts the
        event's payload and must return the event's reply type, either
        directly or as an awaitable that resolves to it. For pub/sub events
        the callable accepts the payload and its return value is ignored.

        Returns the {py:class}`Handler <stratae.events.handler.Handler>`
        instance in both forms so callers can pass it to `remove` later.

        :param config: The {py:class}`Event <stratae.events.event.Event>`
            used as the handler routing key.
        :param fn: When supplied, registers `fn` directly and returns its
            `Handler`. When omitted, returns a decorator that registers and
            returns the `Handler`.
        """
        if fn is not None:
            return self._register(config, fn)

        def decorator(f: Callable[..., Any]) -> Handler[..., _AnyEvent, Any]:
            return self._register(config, f)

        return decorator

    async def _dispatch(self, payload: Any, event: _AnyEvent) -> Any:
        """Dispatch payload as a request or pub/sub fan-out for event."""
        if is_request(event):
            return await self._dispatch_request(payload, event)
        await self.dispatch(payload, config=event)
        return None

    async def _dispatch_request(self, payload: Any, event: _AnyEvent) -> Any:
        """Call event's sole responder with payload, awaiting it if it's async."""
        responder = self._single_responder(event)
        if responder.is_async:
            return await responder(payload)
        return responder(payload)

    async def dispatch(self, payload: Any, *, config: _AnyEvent) -> None:
        """
        Invoke every handler registered for the given Event concurrently.

        Sync handlers are called directly; async handlers are awaited. Both
        are dispatched via `asyncio.gather`.

        :param payload: The constructed payload instance to dispatch.
        :param config: The {py:class}`Event <stratae.events.event.Event>`
            used as the handler lookup key.
        :raises ExceptionGroup: If any handler raises. Gathers every
            handler's failure, since all handlers run regardless of earlier
            ones failing.
        """

        async def _call(handler: Handler[Any, _AnyEvent, Any]) -> None:
            if handler.is_async:
                await handler(payload)
            else:
                handler(payload)

        results = await asyncio.gather(
            *(_call(h) for h in self._handlers.get(config, [])),
            return_exceptions=True,
        )
        exceptions = [r for r in results if isinstance(r, Exception)]
        if exceptions:
            raise ExceptionGroup("handler errors", exceptions)
