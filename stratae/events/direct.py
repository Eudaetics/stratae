"""
In-process event buses that dispatch straight to registered handlers, with no broker.

{py:class}`DirectBus` dispatches synchronously and rejects async handlers.
{py:class}`AsyncDirectBus` accepts both, and runs several async handlers concurrently. Both
run each {py:class}`PubSub <stratae.events.patterns.PubSub>` handler in its own copy of the
context, so one handler's context variables never reach another, nor the emitter.

Register handlers with `handle`, keyed by event, and create emitters with `bind`. A
{py:class}`Request <stratae.events.patterns.Request>` event needs exactly one responder, which
`handlers` can confirm before anything emits it.

Each bus delivers the patterns it has a dispatcher for, found by the event's class, so a
subclass of a supported pattern is delivered as that pattern. Add or replace a pattern with
`dispatchers=`, a list of {py:class}`Dispatch` pairs, fixed once the bus exists. A dispatcher
is checked against its pattern: pairing it with the wrong pattern, or returning something other
than what the pattern emits, is a type error. Registering or binding an event
of an unsupported pattern raises `TypeError`. With
`envelope=EnvelopeConfig()`, each emission opens an
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

import threading
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Collection, Coroutine, Generator
from inspect import markcoroutinefunction
from types import CoroutineType
from typing import Any, ClassVar, NoReturn, Protocol, overload

from stratae.callables import is_async_callable
from stratae.events._dispatch import (
    build_dispatcher,
    dispatch_request,
    dispatch_request_async,
    fan_out,
    fan_out_async,
    no_dispatcher,
)
from stratae.events.binding import abind, bind
from stratae.events.envelope import EnvelopeConfig
from stratae.events.handler import Handler, Handlers
from stratae.events.hooks import Hooks
from stratae.events.middleware import Middleware
from stratae.events.patterns import NoPayload, PubSub, Request
from stratae.events.pipeline import (
    Outgoing,
    Publishing,
    Send,
    publish,
    publish_async,
    received,
    receiving,
    wrap_handler,
)
from stratae.events.protocols import AnyEvent, EventType, PayloadEvent, SignalEvent

_AnyEvent = AnyEvent

_NO_HANDLERS: Handlers[Handler[Any, _AnyEvent, Any]] = Handlers()
"""The handlers of an event nothing handles."""

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


class _Failed:
    """An awaitable raising an error found while starting an emit, once it's awaited."""

    __slots__ = ("_error",)

    def __init__(self, error: Exception) -> None:
        self._error = error

    def __await__(self) -> Generator[Any, Any, NoReturn]:
        raise self._error


async def _await[R](awaitable: Awaitable[R]) -> R:
    """Await what a custom dispatcher returned, so an emit is always a coroutine."""
    return await awaitable


class _Route:
    """
    An event's dispatcher, and its handlers as they are now.

    The dispatcher is fixed once the route is recorded. The handlers are an immutable
    {py:class}`Handlers <stratae.events.handler.Handlers>`, replaced whole when a handler is
    registered or removed. A callable bound to the event holds its route, so each call reads
    the handlers current then without looking the event up.
    """

    __slots__ = ("dispatcher", "handlers", "send")

    send: Send
    """Delivers one message to the handlers current when it's sent."""

    def __init__(
        self, dispatcher: Callable[..., Any], handlers: Handlers[Handler[Any, _AnyEvent, Any]]
    ) -> None:
        self.dispatcher = dispatcher
        self.handlers = handlers


class Dispatch:
    """
    Pair a pattern class with the dispatcher a direct bus delivers it with.

    The dispatcher is called with the event, its handlers and the payload, and returns what
    emitting the event returns, or an awaitable resolving to it on an
    {py:class}`AsyncDirectBus`. The handlers are a
    {py:class}`Handlers <stratae.events.handler.Handlers>` tuple, which doesn't change while
    the dispatcher runs, so it can be run as given. A dispatcher may take any collection of
    handlers in its place. Write it generic in the pattern's type parameters, e.g.
    `def gather[S, R](event: Gather[S, R], handlers: Collection[Handler[[S], Any, R]],
    payload: S) -> list[R]`, so its body is checked. A dispatcher whose first parameter
    doesn't accept `pattern` is a type error. That its declared return matches what the
    pattern emits is the dispatcher author's to ensure.
    """

    __slots__ = ("dispatcher", "pattern")

    def __init__[P: AnyEvent](
        self,
        pattern: type[P],
        dispatcher: Callable[[P, Handlers[Handler[Any, Any, Any]], Any], Any],
    ) -> None:
        """
        Pair a pattern with its dispatcher.

        :param pattern: The pattern class. Its subclasses are dispatched the same way.
        :param dispatcher: Delivers an event of that pattern to its handlers.
        """
        self.pattern: type[AnyEvent] = pattern
        self.dispatcher: Callable[[Any, Handlers[Handler[Any, Any, Any]], Any], Any] = dispatcher


_SYNC_DEFAULTS = (Dispatch(Request, dispatch_request), Dispatch(PubSub, fan_out))
"""The patterns a DirectBus supports by default."""

_ASYNC_DEFAULTS = (Dispatch(Request, dispatch_request_async), Dispatch(PubSub, fan_out_async))
"""The patterns an AsyncDirectBus supports by default."""


class BaseDirectBus(ABC):
    """
    Handler registry and pattern dispatch shared by the direct buses.

    An emit called directly goes through `_emit`, which looks the event up and refuses one the
    bus can't dispatch. A bound callable emits through `_emitter_for`'s emitter instead, which
    refused the event when it was bound, and holds its route. Either way, a plain emit, with
    no hooks, no middleware and no envelope, is delivered to its handlers directly; any other
    goes to the subclass's `_run`. That delivers it with the headers its publish middleware
    set, for handle middleware to read. A direct bus supplies only how it runs a checked emit,
    so it can't dispatch an event its dispatcher doesn't know.
    """

    _asynchronous: ClassVar[bool]
    """Whether the bus runs handlers and middleware asynchronously."""

    _defaults: ClassVar[tuple[Dispatch, ...]]
    """The patterns the bus supports before any `dispatchers=`."""

    _publish: ClassVar[Callable[..., Any]]
    """The shared emit the bus runs a checked emit through, sync or async."""

    def __init__(
        self,
        *,
        envelope: EnvelopeConfig | None = None,
        dispatchers: Collection[Dispatch] = (),
        middleware: Middleware = Middleware(),
    ) -> None:
        """
        Create a bus, optionally with envelope tracking, more patterns and middleware.

        :raises TypeError: If any middleware is the wrong kind for the bus.
        """
        middleware.require(type(self).__name__, asynchronous=self._asynchronous)
        self._routes: dict[_AnyEvent, _Route] = {}
        self._registering = threading.Lock()
        pairs = (*self._defaults, *dispatchers)
        self._dispatcher = build_dispatcher((d.pattern, d.dispatcher) for d in pairs)
        self._middleware = middleware
        self._publishing = Publishing(
            middleware.publish, None if envelope is None else envelope.new_id
        )
        self._plain = not (middleware.publish or middleware.handle) and envelope is None

    def _emitter_for(self, event: _AnyEvent) -> Callable[..., Any]:
        """
        Return what `bind` calls to emit the event: an emitter holding the event's route.

        The route is found by value once, here, and recorded. So a bound call reads the event's
        current handlers without looking it up, and an equal event handled or bound elsewhere
        shares the same route. Hooks the binding was given come in each call, as they do to
        `emit`; they were checked when it was bound.

        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        route, plain = self._route(event), self._plain
        publish_, publishing = self._publish, self._publishing

        def emit_routed(
            event: _AnyEvent, payload: Any, *, hooks: Hooks[Any, Any] | None = None
        ) -> Any:
            if hooks is None and plain:
                return route.dispatcher(event, route.handlers, payload)
            return publish_(publishing, route.send, event, payload, None, hooks)

        return emit_routed

    def _resolve(self, event: _AnyEvent) -> Callable[..., Any]:
        """Return the dispatcher for the event's pattern, or raise TypeError if there's none."""
        dispatcher = self._dispatcher(type(event))
        if dispatcher is no_dispatcher:
            raise TypeError(
                _UNSUPPORTED_PATTERN.format(bus=type(self).__name__, pattern=type(event).__name__)
            )
        return dispatcher

    def _route(self, event: _AnyEvent) -> _Route:
        """
        Return the event's route, recording it the first time.

        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        route = self._routes.get(event)
        if route is None:
            route = self._routes.setdefault(event, self._new_route(self._resolve(event)))
        return route

    def _new_route(self, dispatcher: Callable[..., Any]) -> _Route:
        """Make a route with no handlers yet, and the `send` that delivers to them."""
        route = _Route(dispatcher, _NO_HANDLERS)
        route.send = self._sender(route)
        return route

    @abstractmethod
    def _sender(self, route: _Route) -> Send:
        """Return what delivers a message to the route's handlers, as the shared emit calls."""

    def _check_hooks(self, event: _AnyEvent, hooks: Hooks[Any, Any]) -> None:
        """Raise TypeError for async hooks on a sync bus, which can't await them."""
        if hooks.has_async and not self._asynchronous:
            raise TypeError(_ASYNC_HOOKS_REJECTED.format(event.name))

    def _register(
        self, config: _AnyEvent, fn: Callable[..., Any], hooks: Hooks[Any, Any] | None
    ) -> Handler[Any, _AnyEvent, Any]:
        """Wrap fn and its hooks as a Handler, and add it to the event's handlers."""
        route = self._route(config)
        if hooks is not None:
            self._check_hooks(config, hooks)
        call, is_async = wrap_handler(
            config, None, fn, hooks, self._middleware.handle, self._asynchronous
        )
        handler: Handler[Any, _AnyEvent, Any] = Handler(call, config, is_async)
        with self._registering:
            route.handlers = route.handlers.adding(handler)
        return handler

    def _emit(self, event: _AnyEvent, payload: Any, hooks: Hooks[Any, Any] | None) -> Any:
        """
        Refuse an event this bus can't emit, then run it: the one emit entry point.

        An event that was handled or bound is found with one lookup, and its route passed on.
        Any other has no handlers. Its dispatcher is resolved, but its route isn't recorded, so
        emitting events made on the fly doesn't grow the registry. Its route is built only if
        the emit goes through the pipeline.
        """
        route = self._routes.get(event)
        if route is None:
            dispatcher = self._resolve(event)
            if hooks is None and self._plain:
                return dispatcher(event, _NO_HANDLERS, payload)
            route = self._new_route(dispatcher)
        if hooks is not None:
            self._check_hooks(event, hooks)
        elif self._plain:
            return route.dispatcher(event, route.handlers, payload)
        return self._publish(self._publishing, route.send, event, payload, None, hooks)

    def remove(self, handler: Handler[Any, _AnyEvent, Any]) -> None:
        """
        Remove a registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>` from `handle`.
            One this bus doesn't have is ignored.
        """
        route = self._routes.get(handler.config)
        if route is not None:
            with self._registering:
                route.handlers = route.handlers.removing(handler)

    def handlers(self, event: _AnyEvent) -> Handlers[Handler[Any, _AnyEvent, Any]]:
        """
        Return the handlers registered for an event, in no particular order.

        Use it to check registrations up front, such as that a request event has exactly one
        responder before anything emits it.

        :param event: The event to look up.
        :returns: The event's {py:class}`Handlers <stratae.events.handler.Handlers>`, empty if
            it has none. Later calls to `handle` or `remove` don't change it.
        """
        route = self._routes.get(event)
        return _NO_HANDLERS if route is None else route.handlers


class DirectBus(BaseDirectBus):
    """
    In-process, synchronous event bus.

    {py:class}`PubSub <stratae.events.patterns.PubSub>` events fan out to every handler, each
    in its own copy of the context, so context variables one sets reach neither the others nor
    the emitter. A handler raising an `Exception` or `CancelledError` doesn't stop the others.
    Their failures are raised together once every handler has run. Anything else, such as
    `KeyboardInterrupt`, propagates at once. {py:class}`Request
    <stratae.events.patterns.Request>` events return their one responder's reply. Async
    handlers are rejected with `TypeError`.

    :param envelope: Open an {py:class}`Envelope <stratae.events.envelope.Envelope>` for
        each emission, made as the
        {py:class}`EnvelopeConfig <stratae.events.envelope.EnvelopeConfig>` says. `None`,
        the default, opens none.
    :param dispatchers: {py:class}`Dispatch` pairs for more patterns, or replacing the
        defaults for `Request` and `PubSub`.
    :param middleware: Sync {py:class}`Middleware <stratae.events.middleware.Middleware>` run
        around every emit and handler call.
    :raises TypeError: If any middleware is a class that isn't a sync context manager.
    """

    _asynchronous = False
    _defaults = _SYNC_DEFAULTS
    _publish = staticmethod(publish)

    def emit[S, R](
        self,
        event: EventType[S, R, Any, Any],
        payload: S,
        *,
        hooks: Hooks[S, R] | None = None,
    ) -> R:
        """
        Dispatch a payload to the event's handlers.

        Pub/sub handler failures are raised together as {py:exc}`HandlerErrors
        <stratae.events.exceptions.HandlerErrors>`, an `ExceptionGroup`, or as
        {py:exc}`BaseHandlerErrors <stratae.events.exceptions.BaseHandlerErrors>` if one is a
        `CancelledError`. A responder's exception propagates directly.

        :param event: The event to dispatch.
        :param payload: The payload to dispatch.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this emit.
        :returns: The responder's reply for a request event, otherwise `None`.
        :raises NoResponderError: If a request event has no responder.
        :raises MultipleRespondersError: If a request event has more than one responder.
        :raises TypeError: If the bus has no dispatcher for the event's pattern, or `hooks`
            includes an async hook.
        """
        return self._emit(event, payload, hooks)

    @overload
    def bind[**P, S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: Callable[P, S],
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[P, R]: ...
    @overload
    def bind[R](
        self,
        event: SignalEvent[R, Any],
        *,
        factory: None = None,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> Callable[[], R]: ...
    @overload
    def bind[S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[[S], R]: ...

    def bind(
        self,
        event: _AnyEvent,
        *,
        factory: Callable[..., Any] | None = None,
        hooks: Hooks[Any, Any] | None = None,
    ) -> Callable[..., Any]:
        """
        Bind an event to this bus. See {py:func}`bind <stratae.events.binding.bind>`.

        :param event: The event to bind.
        :param factory: A sync callable building the payload from the call's arguments.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit.
        :returns: A callable that emits the event on this bus.
        :raises TypeError: If the bus has no dispatcher for the event's pattern, the factory is
            async, or `hooks` includes an async hook.
        """
        if hooks is not None:
            self._check_hooks(event, hooks)
        return bind(event, self._emitter_for(event), factory=factory, hooks=hooks)

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

    def _sender(self, route: _Route) -> Send:
        """Return what dispatches a message, with its headers if handle middleware reads them."""
        dispatcher = route.dispatcher
        if not self._middleware.handle:

            def send(message: Outgoing) -> Any:
                return dispatcher(message.event, route.handlers, message.payload)

            return send

        def send_received(message: Outgoing) -> Any:
            with receiving(received(message.headers)):
                return dispatcher(message.event, route.handlers, message.payload)

        return send_received


class AsyncDirectBus(BaseDirectBus):
    """
    In-process, asynchronous event bus.

    Accepts sync and async handlers. A {py:class}`PubSub <stratae.events.patterns.PubSub>`
    event's handlers each run in their own copy of the context, so context variables one sets
    reach neither the others nor the emitter. Several async handlers run concurrently.
    {py:class}`Request <stratae.events.patterns.Request>` events resolve to their one
    responder's reply.

    :param envelope: Open an {py:class}`Envelope <stratae.events.envelope.Envelope>` for
        each emission, made as the
        {py:class}`EnvelopeConfig <stratae.events.envelope.EnvelopeConfig>` says. `None`,
        the default, opens none.
    :param dispatchers: {py:class}`Dispatch` pairs for more patterns, or replacing the
        defaults for `Request` and `PubSub`. Their dispatchers return awaitables.
    :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>` run
        around every emit and handler call, sync or async.
    :raises TypeError: If any middleware is a class that isn't a context manager.
    """

    _asynchronous = True
    _defaults = _ASYNC_DEFAULTS
    _publish = staticmethod(publish_async)

    @markcoroutinefunction
    def emit[S, R](
        self,
        event: EventType[S, R, Any, Any],
        payload: S,
        *,
        hooks: Hooks[S, R] | None = None,
    ) -> Coroutine[Any, Any, R]:
        """
        Dispatch a payload to the event's handlers, returning a coroutine to await.

        Pub/sub handler failures are raised together as an exception group. A responder's
        exception propagates directly. Nothing runs, and nothing is raised, until the
        coroutine is awaited, as if this were an `async def`. It returns the dispatch's own
        coroutine rather than one of its own around it, which saves a coroutine per emit.

        :param event: The event to dispatch.
        :param payload: The payload to dispatch.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this emit.
        :returns: The responder's reply for a request event, otherwise `None`.
        :raises NoResponderError: If a request event has no responder.
        :raises MultipleRespondersError: If a request event has more than one responder.
        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        try:
            dispatched = self._emit(event, payload, hooks)
        except Exception as error:
            return _await(_Failed(error))
        if dispatched.__class__ is CoroutineType:
            return dispatched
        return _await(dispatched)

    @overload
    def bind[**P, S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[P, Awaitable[R]]: ...
    @overload
    def bind[R](
        self,
        event: SignalEvent[R, Any],
        *,
        factory: None = None,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> Callable[[], Awaitable[R]]: ...
    @overload
    def bind[S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[[S], Awaitable[R]]: ...

    def bind(
        self,
        event: _AnyEvent,
        *,
        factory: Callable[..., Any] | None = None,
        hooks: Hooks[Any, Any] | None = None,
    ) -> Callable[..., Awaitable[Any]]:
        """
        Bind an event to this bus. See {py:func}`abind <stratae.events.binding.abind>`.

        :param event: The event to bind.
        :param factory: A sync or async callable building the payload from the call's arguments.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit.
        :returns: An async callable that emits the event on this bus.
        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        return abind(event, self._emitter_for(event), factory=factory, hooks=hooks)

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

    def _sender(self, route: _Route) -> Send:
        """Return what dispatches a message, with its headers if handle middleware reads them."""
        dispatcher = route.dispatcher
        if not self._middleware.handle:

            def send(message: Outgoing) -> Awaitable[Any]:
                return dispatcher(message.event, route.handlers, message.payload)

            return send

        async def send_received(message: Outgoing) -> Any:
            with receiving(received(message.headers)):
                return await dispatcher(message.event, route.handlers, message.payload)

        return send_received
