"""
In-process event buses that dispatch straight to registered handlers, with no broker.

{py:class}`DirectBus` dispatches synchronously and rejects async handlers.
{py:class}`AsyncDirectBus` accepts both. Both run each
{py:class}`PubSub <stratae.events.patterns.PubSub>` handler in its own copy of the context, so
one handler's context variables never reach another, nor the emitter.

Register handlers with `handle`, keyed by event, and create emitters with `bind`. `bind` binds
the bus's `emitter` for the event, which {py:func}`bind <stratae.events.binding.bind>` can
bind too. A
{py:class}`Request <stratae.events.patterns.Request>` event needs exactly one responder, which
`handlers` can confirm before anything emits it.

Each bus delivers the patterns it has a dispatcher for, found by the event's class, so a
subclass of a supported pattern is delivered as that pattern. Add or replace a pattern with
`dispatchers=`, a list of {py:class}`Dispatch <stratae.events.dispatch.Dispatch>` pairs, fixed
once the bus exists. A dispatcher is checked against its pattern: pairing it with the wrong
pattern, or returning something other than what the pattern emits, is a type error.
Registering or binding an event of an unsupported pattern raises `TypeError`. With
`envelope_factory=EnvelopeFactory()`, each emission opens an
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
from collections.abc import Awaitable, Callable, Collection, Coroutine, Generator
from functools import partial
from inspect import markcoroutinefunction
from types import CoroutineType
from typing import Any, NoReturn, Protocol, overload

from stratae.callables import is_async_callable
from stratae.events.binding import bind
from stratae.events.by_pattern import ByPattern
from stratae.events.dispatch import ASYNC_DISPATCHERS, SYNC_DISPATCHERS, Dispatch, Dispatcher
from stratae.events.envelope import Envelope, EnvelopeFactory
from stratae.events.exceptions import DuplicateHandlerError
from stratae.events.handler import AsyncHandler, Handler, Handlers, SyncHandler
from stratae.events.handling import wrap_handler
from stratae.events.hooks import Hooks
from stratae.events.middleware import (
    NO_HEADERS,
    HandleCall,
    Middleware,
    PublishMiddleware,
    from_fields,
)
from stratae.events.patterns import NoPayload
from stratae.events.pipeline import (
    Outgoing,
    Publishing,
    Send,
    publish,
    publish_async,
    publish_enveloped,
    publish_enveloped_async,
)
from stratae.events.protocols import (
    AnyEvent,
    EmitCallable,
    EventType,
    PayloadEvent,
    SignalEvent,
    reserved_for,
)

_NONE_RESERVED: frozenset[str] = frozenset()
"""What a bus that opens no envelope reserves: no header names."""

_NO_HANDLERS: Handlers[AnyEvent, Any, Any] = Handlers()
"""The handlers of an event nothing handles."""

_ADD_A_DISPATCHER = "register one with dispatchers="
"""Why a bus refuses a pattern: it has no dispatcher for it, and can be given one."""


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
    ) -> AsyncHandler[AnyEvent, Payload, [Payload], Result]: ...
    @overload
    def __call__(
        self, fn: Callable[[Payload], Result]
    ) -> Handler[AnyEvent, Payload, [Payload], Result]: ...


class _AsyncSignalHandlerDecorator[Result](Protocol):
    """Decorator form of handle for a schema-less event, returning the Handler."""

    @overload
    def __call__(
        self, fn: Callable[[], Awaitable[Result]]
    ) -> AsyncHandler[AnyEvent, NoPayload, [], Result]: ...
    @overload
    def __call__(self, fn: Callable[[], Result]) -> Handler[AnyEvent, NoPayload, [], Result]: ...


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

    def __init__(self, dispatcher: Dispatcher[Any], handlers: Handlers[AnyEvent, Any, Any]) -> None:
        self.dispatcher = dispatcher
        self.handlers = handlers


type _Publish = Callable[
    [Send[None, Envelope | None], frozenset[str], AnyEvent, Any, None, Hooks[Any, Any] | None],
    Any,
]
"""Emits through the pipeline with a route's `send`: the entry point a bus binds once."""


def _publisher(
    middleware: tuple[PublishMiddleware, ...], factory: EnvelopeFactory | None, asynchronous: bool
) -> _Publish:
    """Return the pipeline's entry point for a bus, with its middleware and envelopes bound."""
    if factory is None:
        bare: Publishing[None, None] = Publishing(middleware, asynchronous=asynchronous)
        if asynchronous:
            return partial(publish_async, bare)
        return partial(publish, bare)
    enveloped: Publishing[None, Envelope] = Publishing(middleware, asynchronous=asynchronous)
    if asynchronous:
        return partial(publish_enveloped_async, enveloped, factory)
    return partial(publish_enveloped, enveloped, factory)


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

    Whether the bus is async decides its shared emit, how it delivers a message, and which
    handlers and hooks it refuses. All are chosen here, once. Its dispatchers are given whole.

    An emit called directly goes through `emit`, which looks the event up and refuses one the
    bus can't dispatch. A bound callable emits through `emitter`'s emitter instead, which
    refused the event when it was made, and holds its route. Either way, a plain emit, with
    no hooks, no middleware and no envelope, is delivered to its handlers directly. Any other
    goes through the shared emit, with the headers its publish middleware set, for handle
    middleware to read.
    """

    __slots__ = (
        "_asynchronous",
        "_dispatchers",
        "_middleware",
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
        factory: EnvelopeFactory | None,
        dispatchers: Collection[Dispatch],
        middleware: Middleware,
    ) -> None:
        """
        Create the core of a bus.

        :param name: The bus's class name, for error messages.
        :param asynchronous: Whether the bus runs handlers, hooks and middleware asynchronously.
        :param dispatchers: The bus's whole table of pattern and dispatcher pairs; later pairs
            win.
        :raises MiddlewareKindError: If any middleware is the wrong kind for the bus.
        """
        middleware.require(name, asynchronous=asynchronous)
        self._asynchronous = asynchronous
        self._routes: dict[AnyEvent, _Route] = {}
        self._registering = threading.Lock()
        self._dispatchers: Callable[[AnyEvent], Dispatcher[Any]] = ByPattern(
            name, ((d.pattern, d.dispatcher) for d in dispatchers), reason=_ADD_A_DISPATCHER
        ).__call__
        self._middleware = middleware
        self._publish = _publisher(middleware.publish, factory, asynchronous)
        self._plain = not (middleware.publish or middleware.handle) and factory is None
        self._reserved = _NONE_RESERVED if factory is None else reserved_for(None, factory.headers)

    def emitter(self, event: AnyEvent, /) -> Callable[..., Any]:
        """
        Return an emitter of the event that holds its route.

        The route is found by value once, here, and recorded. So each call reads the event's
        current handlers without looking it up, and an equal event handled or bound elsewhere
        shares the same route. Hooks come in each call, as they do to `emit`, and a sync bus
        refuses async ones there.

        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        route, plain = self._route(event), self._plain
        publish_, reserved = self._publish, self._reserved
        asynchronous = self._asynchronous

        def emit_routed(
            event: AnyEvent, payload: Any, /, *, hooks: Hooks[Any, Any] | None = None
        ) -> Any:
            if hooks is None:
                if plain:
                    message = from_fields(HandleCall, (event, None, payload, NO_HEADERS))
                    return route.dispatcher(event, route.handlers, message)
            elif hooks.has_async and not asynchronous:
                raise TypeError(_ASYNC_HOOKS_REJECTED.format(event.name))
            return publish_(route.send, reserved, event, payload, None, hooks)

        return markcoroutinefunction(emit_routed) if asynchronous else emit_routed

    def _route(self, event: AnyEvent) -> _Route:
        """
        Return the event's route, recording it the first time.

        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        route = self._routes.get(event)
        if route is None:
            route = self._routes.setdefault(event, self._new_route(self._dispatchers(event)))
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
    ) -> Handler[AnyEvent, Any, ..., Any]:
        """
        Wrap fn and its hooks as a Handler, and add it to the event's handlers.

        :raises DuplicateHandlerError: If the event already has an equal handler.
        """
        if not self._asynchronous and is_async_callable(fn):
            raise TypeError(_ASYNC_HANDLER_REJECTED)
        route = self._route(config)
        if hooks is not None:
            self.check_hooks(config, hooks)
        handler = wrap_handler(
            config, fn, config, hooks, self._middleware.handle, self._asynchronous
        )
        with self._registering:
            if handler in route.handlers:
                raise DuplicateHandlerError(config.name, fn)
            route.handlers = route.handlers.adding(handler)
        return handler

    def decorator(
        self, config: AnyEvent, hooks: Hooks[Any, Any] | None
    ) -> Callable[[Callable[..., Any]], Handler[AnyEvent, Any, ..., Any]]:
        """Return the decorator form of `handle`, registering what it's applied to."""

        def decorator(fn: Callable[..., Any]) -> Handler[AnyEvent, Any, ..., Any]:
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
            dispatcher = self._dispatchers(event)
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

    def remove(self, handler: Handler[AnyEvent, Any, ..., Any]) -> None:
        """Remove a registered handler, ignoring one the bus doesn't have."""
        route = self._routes.get(handler.config)
        if route is not None:
            with self._registering:
                route.handlers = route.handlers.removing(handler)

    def handlers(self, event: AnyEvent) -> Handlers[AnyEvent, Any, Any]:
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

    :param envelope_factory: Open an {py:class}`Envelope <stratae.events.envelope.Envelope>` for
        each emission, made by this
        {py:class}`EnvelopeFactory <stratae.events.envelope.EnvelopeFactory>`. The bus
        refuses its `headers`' names as headers. `None`, the default, opens none, and reserves
        no envelope header names.
    :param dispatchers: {py:class}`Dispatch <stratae.events.dispatch.Dispatch>` pairs for more
        patterns, or replacing the defaults for `Request` and `PubSub`,
        {py:data}`SYNC_DISPATCHERS <stratae.events.dispatch.SYNC_DISPATCHERS>`.
    :param middleware: Sync {py:class}`Middleware <stratae.events.middleware.Middleware>` run
        around every emit and handler call.
    :raises MiddlewareKindError: If any middleware isn't a sync function returning a value.
    """

    def __init__(
        self,
        *,
        envelope_factory: EnvelopeFactory | None = None,
        dispatchers: Collection[Dispatch] = (),
        middleware: Middleware = Middleware(),
    ) -> None:
        """Create a bus, optionally with envelope tracking, more patterns and middleware."""
        self._core = _Core(
            "DirectBus", False, envelope_factory, (*SYNC_DISPATCHERS, *dispatchers), middleware
        )

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

    def emitter[Payload, Result](
        self, event: EventType[Payload, Result, Any, Any], /
    ) -> EmitCallable[
        EventType[Payload, Result, Any, Any], Payload, Hooks[Payload, Result], Result
    ]:
        """
        Return an emitter of the event that holds its route.

        Binding it with {py:func}`bind <stratae.events.binding.bind>` gives what the bus's `bind`
        method does. Each call reads the event's handlers current then, without looking the
        event up.

        :param event: The event.
        :returns: An emitter taking the event, a payload and `hooks=`.
        :raises TypeError: If the bus has no dispatcher for the event's pattern, or, when it's
            called, if `hooks` includes an async hook.
        """
        return self._core.emitter(event)

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
        return bind(event, self.emitter(event), factory=factory, hooks=hooks)

    @overload
    def handle[Result](
        self,
        config: SignalEvent[Any, Result],
        fn: Callable[[], Result],
        *,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> SyncHandler[AnyEvent, NoPayload, [], Result]: ...

    @overload
    def handle[Result](
        self,
        config: SignalEvent[Any, Result],
        fn: None = None,
        *,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> Callable[[Callable[[], Result]], SyncHandler[AnyEvent, NoPayload, [], Result]]: ...

    @overload
    def handle[Payload, Result](
        self,
        config: PayloadEvent[Payload, Any, Result],
        fn: Callable[[Payload], Result],
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> SyncHandler[AnyEvent, Payload, [Payload], Result]: ...

    @overload
    def handle[Payload, Result](
        self,
        config: PayloadEvent[Payload, Any, Result],
        fn: None = None,
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Callable[
        [Callable[[Payload], Result]], SyncHandler[AnyEvent, Payload, [Payload], Result]
    ]: ...

    def handle(
        self,
        config: AnyEvent,
        fn: Callable[..., Any] | None = None,
        *,
        hooks: Hooks[Any, Any] | None = None,
    ) -> Any:
        """
        Register a sync handler for an event, as a decorator or direct call.

        A request event's handler returns the reply; a pub/sub handler's return is ignored. A
        function is registered once per event with the same hooks: registering it again
        raises, so a module imported twice can't run it twice. Empty hooks count as none.

        :param config: The event to handle.
        :param fn: The handler. If omitted, returns a decorator.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this handler.
        :returns: The {py:class}`Handler <stratae.events.handler.Handler>`, for `remove`.
        :raises DuplicateHandlerError: If the event already has `fn` with equal hooks.
        :raises TypeError: If `fn` is async, a hook is async, or `fn` or a hook is unhashable.
        """
        if fn is not None:
            return self._core.register(config, fn, hooks)
        return self._core.decorator(config, hooks)

    def remove(self, handler: Handler[AnyEvent, Any, ..., Any]) -> None:
        """
        Remove a registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>` from `handle`,
            or one equal to it. One this bus doesn't have is ignored.
        """
        self._core.remove(handler)

    def handlers(self, event: AnyEvent) -> Handlers[AnyEvent, Any, Any]:
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

    :param envelope_factory: Open an {py:class}`Envelope <stratae.events.envelope.Envelope>` for
        each emission, made by this
        {py:class}`EnvelopeFactory <stratae.events.envelope.EnvelopeFactory>`. The bus
        refuses its `headers`' names as headers. `None`, the default, opens none, and reserves
        no envelope header names.
    :param dispatchers: {py:class}`Dispatch <stratae.events.dispatch.Dispatch>` pairs for more
        patterns, or replacing the defaults for `Request` and `PubSub`,
        {py:data}`ASYNC_DISPATCHERS <stratae.events.dispatch.ASYNC_DISPATCHERS>`. Their
        dispatchers return awaitables.
    :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>` run
        around every emit and handler call. Each is an `async def`.
    :raises MiddlewareKindError: If any middleware isn't an `async def`.
    """

    def __init__(
        self,
        *,
        envelope_factory: EnvelopeFactory | None = None,
        dispatchers: Collection[Dispatch] = (),
        middleware: Middleware = Middleware(),
    ) -> None:
        """Create a bus, optionally with envelope tracking, more patterns and middleware."""
        self._core = _Core(
            "AsyncDirectBus",
            True,
            envelope_factory,
            (*ASYNC_DISPATCHERS, *dispatchers),
            middleware,
        )

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

    def emitter[Payload, Result](
        self, event: EventType[Payload, Result, Any, Any], /
    ) -> EmitCallable[
        EventType[Payload, Result, Any, Any], Payload, Hooks[Payload, Result], Awaitable[Result]
    ]:
        """
        Return an emitter of the event that holds its route.

        Binding it with {py:func}`bind <stratae.events.binding.bind>` gives what the bus's `bind`
        method does. Each call reads the event's handlers current then, without looking the
        event up.

        :param event: The event.
        :returns: An emitter taking the event, a payload and `hooks=`, returning an awaitable.
        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        return self._core.emitter(event)

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
        Bind an event to this bus. See {py:func}`bind <stratae.events.binding.bind>`.

        :param event: The event to bind.
        :param factory: A sync or async callable building the payload from the call's arguments.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit.
        :returns: An async callable that emits the event on this bus.
        :raises TypeError: If the bus has no dispatcher for the event's pattern.
        """
        return bind(event, self.emitter(event), factory=factory, hooks=hooks)

    @overload
    def handle[Result](
        self,
        config: SignalEvent[Any, Result],
        fn: Callable[[], Awaitable[Result]],
        *,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> AsyncHandler[AnyEvent, NoPayload, [], Result]: ...

    @overload
    def handle[Result](
        self,
        config: SignalEvent[Any, Result],
        fn: Callable[[], Result],
        *,
        hooks: Hooks[NoPayload, Result] | None = None,
    ) -> Handler[AnyEvent, NoPayload, [], Result]: ...

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
    ) -> AsyncHandler[AnyEvent, Payload, [Payload], Result]: ...

    @overload
    def handle[Payload, Result](
        self,
        config: PayloadEvent[Payload, Any, Result],
        fn: Callable[[Payload], Result],
        *,
        hooks: Hooks[Payload, Result] | None = None,
    ) -> Handler[AnyEvent, Payload, [Payload], Result]: ...

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

        A request event's handler returns the reply; a pub/sub handler's return is ignored. A
        function is registered once per event with the same hooks: registering it again
        raises, so a module imported twice can't run it twice. Empty hooks count as none.

        :param config: The event to handle.
        :param fn: The handler. If omitted, returns a decorator.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this handler.
        :returns: The {py:class}`Handler <stratae.events.handler.Handler>`, for `remove`.
        :raises DuplicateHandlerError: If the event already has `fn` with equal hooks.
        :raises TypeError: If `fn` or a hook is unhashable.
        """
        if fn is not None:
            return self._core.register(config, fn, hooks)
        return self._core.decorator(config, hooks)

    def remove(self, handler: Handler[AnyEvent, Any, ..., Any]) -> None:
        """
        Remove a registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>` from `handle`,
            or one equal to it. One this bus doesn't have is ignored.
        """
        self._core.remove(handler)

    def handlers(self, event: AnyEvent) -> Handlers[AnyEvent, Any, Any]:
        """
        Return the handlers registered for an event, in no particular order.

        Use it to check registrations up front, such as that a request event has exactly one
        responder before anything emits it.

        :param event: The event to look up.
        :returns: The event's {py:class}`Handlers <stratae.events.handler.Handlers>`, empty if
            it has none. Later calls to `handle` or `remove` don't change it.
        """
        return self._core.handlers(event)
