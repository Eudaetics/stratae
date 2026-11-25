"""
In-process event buses that dispatch straight to registered handlers, with no broker.

{py:class}`DirectBus` dispatches synchronously and rejects async handlers.
{py:class}`AsyncDirectBus` accepts both. Both run each
{py:class}`PubSub <stratae.events.patterns.PubSub>` handler in its own copy of the context, so
one handler's context variables never reach another, nor the emitter.

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

Both buses run an event's handlers one at a time. So handlers can share a resource that
refuses concurrent use, such as the database session of the unit of work they run in. An
{py:class}`AsyncDirectBus` whose handlers share nothing like that can run several async
handlers concurrently. Pass it `Dispatch(PubSub, fan_out_concurrent)`, using
{py:func}`fan_out_concurrent <stratae.events.dispatch.fan_out_concurrent>`.

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
from collections.abc import Awaitable, Callable, Collection, Coroutine, Generator, Iterable
from functools import partial, singledispatch
from inspect import markcoroutinefunction
from types import CoroutineType
from typing import Any, NoReturn, Protocol, overload

from stratae.callables import is_async_callable
from stratae.events.binding import abind, bind
from stratae.events.dispatch import (
    Dispatcher,
    dispatch_request,
    dispatch_request_async,
    fan_out,
    fan_out_async,
)
from stratae.events.envelope import Envelope, EnvelopeConfig
from stratae.events.handler import AsyncHandler, Handler, Handlers, SyncHandler
from stratae.events.hooks import Hooks
from stratae.events.middleware import (
    NO_HEADERS,
    HandleCall,
    Middleware,
    PublishMiddleware,
    from_fields,
)
from stratae.events.patterns import NoPayload, PubSub, Request
from stratae.events.pipeline import (
    Outgoing,
    Publishing,
    Send,
    publish,
    publish_async,
    publish_enveloped,
    publish_enveloped_async,
    wrap_handler,
)
from stratae.events.protocols import AnyEvent, EventType, PayloadEvent, SignalEvent, reserved_for

_NONE_RESERVED: frozenset[str] = frozenset()
"""What a bus that opens no envelope reserves: no header names."""

_NO_HANDLERS: Handlers[AnyEvent, Any] = Handlers()
"""The handlers of an event nothing handles."""

_UNSUPPORTED_PATTERN = (
    "{bus} has no dispatcher for {pattern} events; register one with dispatchers="
)


def _no_dispatcher(event: AnyEvent, handlers: Collection[Any], payload: Any) -> Any:
    """
    Mark a pattern the bus has no dispatcher for; never called.

    A bus compares its dispatch function's choice against this, and refuses the event with
    `TypeError` in `handle`, `bind` and `emit`. Every emit goes through that check, so
    nothing dispatches to this.
    """
    ...


def _build_dispatcher(
    pairs: Iterable[tuple[type, Dispatcher[Any]]],
) -> Callable[[type], Dispatcher[Any]]:
    """
    Build a bus's dispatch function from pattern and dispatcher pairs; later pairs win.

    It takes an event's class and returns its dispatcher, or `_no_dispatcher` if there's none.
    """
    dispatch = singledispatch(_no_dispatcher)
    for pattern, dispatcher in pairs:
        dispatch.register(pattern, dispatcher)
    return dispatch.dispatch


_ASYNC_HANDLER_REJECTED = (
    "DirectBus dispatches synchronously and cannot await async handlers;"
    " register them on AsyncDirectBus instead"
)

_ASYNC_HOOKS_REJECTED = (
    "DirectBus runs hooks synchronously and cannot await async hooks for '{}';"
    " use AsyncDirectBus instead"
)


class _AsyncHandlerDecorator[Payload: Any, Result](Protocol):
    """Decorator form of handle, returning the Handler."""

    @overload
    def __call__(
        self, fn: Callable[[Payload], Awaitable[Result]]
    ) -> AsyncHandler[AnyEvent, Result]: ...
    @overload
    def __call__(self, fn: Callable[[Payload], Result]) -> Handler[AnyEvent, Result]: ...


class _AsyncSignalHandlerDecorator[Result](Protocol):
    """Decorator form of handle for a schema-less event, returning the Handler."""

    @overload
    def __call__(self, fn: Callable[[], Awaitable[Result]]) -> AsyncHandler[AnyEvent, Result]: ...
    @overload
    def __call__(self, fn: Callable[[], Result]) -> Handler[AnyEvent, Result]: ...


class _Failed:
    """An awaitable raising an error found while starting an emit, once it's awaited."""

    __slots__ = ("_error",)

    def __init__(self, error: Exception) -> None:
        self._error = error

    def __await__(self) -> Generator[Any, Any, NoReturn]:
        raise self._error


async def _await[Result](awaitable: Awaitable[Result]) -> Result:
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

    send: Send[None, Envelope | None]
    """Delivers one message to the handlers current when it's sent."""

    def __init__(self, dispatcher: Dispatcher[Any], handlers: Handlers[AnyEvent, Any]) -> None:
        self.dispatcher = dispatcher
        self.handlers = handlers


class Dispatch:
    """
    Pair a pattern class with the dispatcher a direct bus delivers it with.

    The dispatcher is called with the event, its handlers and the message being handled, a
    {py:class}`HandleCall <stratae.events.middleware.HandleCall>`, and returns what
    emitting the event returns, or an awaitable resolving to it on an
    {py:class}`AsyncDirectBus`. The handlers are a
    {py:class}`Handlers <stratae.events.handler.Handlers>` tuple, which doesn't change while
    the dispatcher runs, so it can be run as given. It keeps its sync and async handlers apart,
    and a {py:class}`DirectBus`'s are all sync. A dispatcher may take any collection of
    handlers in its place. Write it generic in the pattern's type parameters, e.g.
    `def gather[Payload, Result](event: Gather[Payload, Result],
    handlers: Handlers[Any, Result], message: HandleCall) -> list[Result]`,
    so its body is checked. A dispatcher whose first parameter
    doesn't accept `pattern` is a type error. That its declared return matches what the
    pattern emits is the dispatcher author's to ensure.
    """

    __slots__ = ("dispatcher", "pattern")

    def __init__[Event: AnyEvent](
        self,
        pattern: type[Event],
        dispatcher: Dispatcher[Event],
    ) -> None:
        """
        Pair a pattern with its dispatcher.

        :param pattern: The pattern class. Its subclasses are dispatched the same way.
        :param dispatcher: Delivers an event of that pattern to its handlers.
        """
        self.pattern: type[AnyEvent] = pattern
        self.dispatcher: Dispatcher[Any] = dispatcher


_SYNC_DEFAULTS = (Dispatch(Request, dispatch_request), Dispatch(PubSub, fan_out))
"""The patterns a DirectBus supports by default."""

_ASYNC_DEFAULTS = (Dispatch(Request, dispatch_request_async), Dispatch(PubSub, fan_out_async))
"""The patterns an AsyncDirectBus supports by default."""


type _Publish = Callable[
    [Send[None, Envelope | None], frozenset[str], AnyEvent, Any, None, Hooks[Any, Any] | None],
    Any,
]
"""Emits through the pipeline with a route's `send`: the entry point a bus binds once."""


def _publisher(
    middleware: tuple[PublishMiddleware, ...], envelope: EnvelopeConfig | None, asynchronous: bool
) -> _Publish:
    """Return the pipeline's entry point for a bus, with its middleware and envelopes bound."""
    if envelope is None:
        bare: Publishing[None, None] = Publishing(middleware, asynchronous=asynchronous)
        if asynchronous:
            return partial(publish_async, bare)
        return partial(publish, bare)
    enveloped: Publishing[None, Envelope] = Publishing(middleware, asynchronous=asynchronous)
    if asynchronous:
        return partial(publish_enveloped_async, enveloped, envelope.new_id)
    return partial(publish_enveloped, enveloped, envelope.new_id)


def _sender(route: _Route) -> Send[None, Envelope | None]:
    """Return what dispatches a message to the route's handlers, current when it's sent."""

    def send(message: Outgoing[None, Envelope | None]) -> Any:
        event, config, payload, _, headers = message
        received = from_fields(HandleCall, (event, config, payload, headers))
        return route.dispatcher(event, route.handlers, received)

    return send


class _Core:
    """
    Handler registry and pattern dispatch, for a sync or an async direct bus.

    Whether the bus is async decides its default dispatchers, its shared emit, how it delivers
    a message, and which handlers and hooks it refuses. All are chosen here, once.

    An emit called directly goes through `emit`, which looks the event up and refuses one the
    bus can't dispatch. A bound callable emits through `emitter_for`'s emitter instead, which
    refused the event when it was bound, and holds its route. Either way, a plain emit, with
    no hooks, no middleware and no envelope, is delivered to its handlers directly. Any other
    goes through the shared emit, with the headers its publish middleware set, for handle
    middleware to read.
    """

    __slots__ = (
        "_asynchronous",
        "_dispatcher",
        "_middleware",
        "_name",
        "_plain",
        "_publish",
        "_registering",
        "_reserved",
        "_routes",
    )

    def __init__(
        self,
        name: str,
        asynchronous: bool,
        envelope: EnvelopeConfig | None,
        dispatchers: Collection[Dispatch],
        middleware: Middleware,
    ) -> None:
        """
        Create the core of a bus.

        :param name: The bus's class name, for error messages.
        :param asynchronous: Whether the bus runs handlers, hooks and middleware asynchronously.
        :raises MiddlewareKindError: If any middleware is the wrong kind for the bus.
        """
        middleware.require(name, asynchronous=asynchronous)
        self._name = name
        self._asynchronous = asynchronous
        self._routes: dict[AnyEvent, _Route] = {}
        self._registering = threading.Lock()
        pairs = (*(_ASYNC_DEFAULTS if asynchronous else _SYNC_DEFAULTS), *dispatchers)
        self._dispatcher = _build_dispatcher((d.pattern, d.dispatcher) for d in pairs)
        self._middleware = middleware
        self._publish = _publisher(middleware.publish, envelope, asynchronous)
        self._plain = not (middleware.publish or middleware.handle) and envelope is None
        self._reserved = (
            _NONE_RESERVED if envelope is None else reserved_for(None, envelope.headers)
        )

    def emitter_for(self, event: AnyEvent) -> Callable[..., Any]:
        """
        Return what `bind` calls to emit the event: an emitter holding the event's route.

        The route is found by value once, here, and recorded. So a bound call reads the event's
        current handlers without looking it up, and an equal event handled or bound elsewhere
        shares the same route. Hooks the binding was given come in each call, as they do to
        `emit`; they were checked when it was bound.

        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        route, plain = self._route(event), self._plain
        publish_, reserved = self._publish, self._reserved

        def emit_routed(
            event: AnyEvent, payload: Any, *, hooks: Hooks[Any, Any] | None = None
        ) -> Any:
            if hooks is None and plain:
                message = from_fields(HandleCall, (event, None, payload, NO_HEADERS))
                return route.dispatcher(event, route.handlers, message)
            return publish_(route.send, reserved, event, payload, None, hooks)

        return emit_routed

    def _resolve(self, event: AnyEvent) -> Dispatcher[Any]:
        """Return the dispatcher for the event's pattern, or raise TypeError if there's none."""
        dispatcher = self._dispatcher(type(event))
        if dispatcher is _no_dispatcher:
            raise TypeError(
                _UNSUPPORTED_PATTERN.format(bus=self._name, pattern=type(event).__name__)
            )
        return dispatcher

    def _route(self, event: AnyEvent) -> _Route:
        """
        Return the event's route, recording it the first time.

        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        route = self._routes.get(event)
        if route is None:
            route = self._routes.setdefault(event, self._new_route(self._resolve(event)))
        return route

    def _new_route(self, dispatcher: Dispatcher[Any]) -> _Route:
        """Make a route with no handlers yet, and the `send` that delivers to them."""
        route = _Route(dispatcher, _NO_HANDLERS)
        route.send = _sender(route)
        return route

    def check_hooks(self, event: AnyEvent, hooks: Hooks[Any, Any]) -> None:
        """Raise TypeError for async hooks on a sync bus, which can't await them."""
        if hooks.has_async and not self._asynchronous:
            raise TypeError(_ASYNC_HOOKS_REJECTED.format(event.name))

    def register(
        self, config: AnyEvent, fn: Callable[..., Any], hooks: Hooks[Any, Any] | None
    ) -> Handler[AnyEvent, Any]:
        """Wrap fn and its hooks as a Handler, and add it to the event's handlers."""
        if not self._asynchronous and is_async_callable(fn):
            raise TypeError(_ASYNC_HANDLER_REJECTED)
        route = self._route(config)
        if hooks is not None:
            self.check_hooks(config, hooks)
        handler = wrap_handler(
            config, fn, config, hooks, self._middleware.handle, self._asynchronous
        )
        with self._registering:
            route.handlers = route.handlers.adding(handler)
        return handler

    def decorator(
        self, config: AnyEvent, hooks: Hooks[Any, Any] | None
    ) -> Callable[[Callable[..., Any]], Handler[AnyEvent, Any]]:
        """Return the decorator form of `handle`, registering what it's applied to."""

        def decorator(fn: Callable[..., Any]) -> Handler[AnyEvent, Any]:
            return self.register(config, fn, hooks)

        return decorator

    def emit(self, event: AnyEvent, payload: Any, hooks: Hooks[Any, Any] | None) -> Any:
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
                message = from_fields(HandleCall, (event, None, payload, NO_HEADERS))
                return dispatcher(event, _NO_HANDLERS, message)
            route = self._new_route(dispatcher)
        if hooks is not None:
            self.check_hooks(event, hooks)
        elif self._plain:
            message = from_fields(HandleCall, (event, None, payload, NO_HEADERS))
            return route.dispatcher(event, route.handlers, message)
        return self._publish(route.send, self._reserved, event, payload, None, hooks)

    def remove(self, handler: Handler[AnyEvent, Any]) -> None:
        """Remove a registered handler, ignoring one the bus doesn't have."""
        route = self._routes.get(handler.config)
        if route is not None:
            with self._registering:
                route.handlers = route.handlers.removing(handler)

    def handlers(self, event: AnyEvent) -> Handlers[AnyEvent, Any]:
        """Return the handlers registered for an event, empty if it has none."""
        route = self._routes.get(event)
        return _NO_HANDLERS if route is None else route.handlers


class DirectBus:
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
        {py:class}`EnvelopeConfig <stratae.events.envelope.EnvelopeConfig>` says. The bus
        refuses its `headers`' names as headers. `None`, the default, opens none, and reserves
        no envelope header names.
    :param dispatchers: {py:class}`Dispatch` pairs for more patterns, or replacing the
        defaults for `Request` and `PubSub`.
    :param middleware: Sync {py:class}`Middleware <stratae.events.middleware.Middleware>` run
        around every emit and handler call.
    :raises MiddlewareKindError: If any middleware isn't a sync function returning a value.
    """

    def __init__(
        self,
        *,
        envelope: EnvelopeConfig | None = None,
        dispatchers: Collection[Dispatch] = (),
        middleware: Middleware = Middleware(),
    ) -> None:
        """Create a bus, optionally with envelope tracking, more patterns and middleware."""
        self._core = _Core("DirectBus", False, envelope, dispatchers, middleware)

    def emit[Payload, Result](
        self,
        event: EventType[Payload, Result, Any, Any],
        payload: Payload,
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Result:
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
        return self._core.emit(event, payload, hooks)

    @overload
    def bind[**P, Payload, Result](
        self,
        event: PayloadEvent[Payload, Result, Any],
        *,
        factory: Callable[P, Payload],
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Callable[P, Result]: ...
    @overload
    def bind[Result](
        self,
        event: SignalEvent[Result, Any],
        *,
        factory: None = None,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> Callable[[], Result]: ...
    @overload
    def bind[Payload, Result](
        self,
        event: PayloadEvent[Payload, Result, Any],
        *,
        factory: None = None,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Callable[[Payload], Result]: ...

    def bind(
        self,
        event: AnyEvent,
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
            self._core.check_hooks(event, hooks)
        return bind(event, self._core.emitter_for(event), factory=factory, hooks=hooks)

    @overload
    def handle[Result](
        self,
        config: SignalEvent[Any, Result],
        fn: Callable[[], Result],
        *,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> SyncHandler[AnyEvent, Result]: ...

    @overload
    def handle[Result](
        self,
        config: SignalEvent[Any, Result],
        fn: None = None,
        *,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> Callable[[Callable[[], Result]], SyncHandler[AnyEvent, Result]]: ...

    @overload
    def handle[Payload, Result](
        self,
        config: PayloadEvent[Payload, Any, Result],
        fn: Callable[[Payload], Result],
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> SyncHandler[AnyEvent, Result]: ...

    @overload
    def handle[Payload, Result](
        self,
        config: PayloadEvent[Payload, Any, Result],
        fn: None = None,
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Callable[[Callable[[Payload], Result]], SyncHandler[AnyEvent, Result]]: ...

    def handle(
        self,
        config: AnyEvent,
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
            return self._core.register(config, fn, hooks)
        return self._core.decorator(config, hooks)

    def remove(self, handler: Handler[AnyEvent, Any]) -> None:
        """
        Remove a registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>` from `handle`.
            One this bus doesn't have is ignored.
        """
        self._core.remove(handler)

    def handlers(self, event: AnyEvent) -> Handlers[AnyEvent, Any]:
        """
        Return the handlers registered for an event, in no particular order.

        Use it to check registrations up front, such as that a request event has exactly one
        responder before anything emits it.

        :param event: The event to look up.
        :returns: The event's {py:class}`Handlers <stratae.events.handler.Handlers>`, empty if
            it has none. Later calls to `handle` or `remove` don't change it.
        """
        return self._core.handlers(event)


class AsyncDirectBus:
    """
    In-process, asynchronous event bus.

    Accepts sync and async handlers. A {py:class}`PubSub <stratae.events.patterns.PubSub>`
    event's handlers each run in their own copy of the context, so context variables one sets
    reach neither the others nor the emitter. Several async handlers run concurrently.
    {py:class}`Request <stratae.events.patterns.Request>` events resolve to their one
    responder's reply.

    :param envelope: Open an {py:class}`Envelope <stratae.events.envelope.Envelope>` for
        each emission, made as the
        {py:class}`EnvelopeConfig <stratae.events.envelope.EnvelopeConfig>` says. The bus
        refuses its `headers`' names as headers. `None`, the default, opens none, and reserves
        no envelope header names.
    :param dispatchers: {py:class}`Dispatch` pairs for more patterns, or replacing the
        defaults for `Request` and `PubSub`. Their dispatchers return awaitables.
    :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>` run
        around every emit and handler call. Each is an `async def`.
    :raises MiddlewareKindError: If any middleware isn't an `async def`.
    """

    def __init__(
        self,
        *,
        envelope: EnvelopeConfig | None = None,
        dispatchers: Collection[Dispatch] = (),
        middleware: Middleware = Middleware(),
    ) -> None:
        """Create a bus, optionally with envelope tracking, more patterns and middleware."""
        self._core = _Core("AsyncDirectBus", True, envelope, dispatchers, middleware)

    @markcoroutinefunction
    def emit[Payload, Result](
        self,
        event: EventType[Payload, Result, Any, Any],
        payload: Payload,
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Coroutine[Any, Any, Result]:
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
            dispatched = self._core.emit(event, payload, hooks)
        except Exception as error:
            return _await(_Failed(error))
        if dispatched.__class__ is CoroutineType:
            return dispatched
        return _await(dispatched)

    @overload
    def bind[**P, Payload, Result](
        self,
        event: PayloadEvent[Payload, Result, Any],
        *,
        factory: Callable[P, Payload] | Callable[P, Awaitable[Payload]],
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Callable[P, Awaitable[Result]]: ...
    @overload
    def bind[Result](
        self,
        event: SignalEvent[Result, Any],
        *,
        factory: None = None,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> Callable[[], Awaitable[Result]]: ...
    @overload
    def bind[Payload, Result](
        self,
        event: PayloadEvent[Payload, Result, Any],
        *,
        factory: None = None,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Callable[[Payload], Awaitable[Result]]: ...

    def bind(
        self,
        event: AnyEvent,
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
        return abind(event, self._core.emitter_for(event), factory=factory, hooks=hooks)

    @overload
    def handle[Result](
        self,
        config: SignalEvent[Any, Result],
        fn: Callable[[], Awaitable[Result]],
        *,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> AsyncHandler[AnyEvent, Result]: ...

    @overload
    def handle[Result](
        self,
        config: SignalEvent[Any, Result],
        fn: Callable[[], Result],
        *,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> Handler[AnyEvent, Result]: ...

    @overload
    def handle[Result](
        self,
        config: SignalEvent[Any, Result],
        fn: None = None,
        *,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> _AsyncSignalHandlerDecorator[Result]: ...

    @overload
    def handle[Payload, Result](
        self,
        config: PayloadEvent[Payload, Any, Result],
        fn: Callable[[Payload], Awaitable[Result]],
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> AsyncHandler[AnyEvent, Result]: ...

    @overload
    def handle[Payload, Result](
        self,
        config: PayloadEvent[Payload, Any, Result],
        fn: Callable[[Payload], Result],
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Handler[AnyEvent, Result]: ...

    @overload
    def handle[Payload, Result](
        self,
        config: PayloadEvent[Payload, Any, Result],
        fn: None = None,
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> _AsyncHandlerDecorator[Payload, Result]: ...

    def handle(
        self,
        config: AnyEvent,
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
            return self._core.register(config, fn, hooks)
        return self._core.decorator(config, hooks)

    def remove(self, handler: Handler[AnyEvent, Any]) -> None:
        """
        Remove a registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>` from `handle`.
            One this bus doesn't have is ignored.
        """
        self._core.remove(handler)

    def handlers(self, event: AnyEvent) -> Handlers[AnyEvent, Any]:
        """
        Return the handlers registered for an event, in no particular order.

        Use it to check registrations up front, such as that a request event has exactly one
        responder before anything emits it.

        :param event: The event to look up.
        :returns: The event's {py:class}`Handlers <stratae.events.handler.Handlers>`, empty if
            it has none. Later calls to `handle` or `remove` don't change it.
        """
        return self._core.handlers(event)
