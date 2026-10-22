"""
In-process event buses that dispatch straight to registered handlers, with no broker.

{py:class}`DirectBus` dispatches synchronously and rejects async handlers.
{py:class}`AsyncDirectBus` accepts both, running
{py:class}`PubSub <stratae.events.event.PubSub>` handlers concurrently. Register handlers
with `handle`, keyed by {py:class}`Event <stratae.events.event.Event>`, and create
emitters with `bind`. A {py:class}`Request <stratae.events.event.Request>` event needs
exactly one responder. With `use_envelope=True`, each emission opens an
{py:class}`Envelope <stratae.events.envelope.Envelope>`.

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
from typing import Any, Awaitable, Callable, Literal, Protocol, overload

from stratae._callables import is_async_callable
from stratae.events.binding import AsyncBindMixin, BindMixin
from stratae.events.envelope import Envelope
from stratae.events.event import DispatchPattern, Event, NoPayload, is_request
from stratae.events.exceptions import MultipleRespondersError, NoResponderError
from stratae.events.handler import Handler

_AnyEvent = Event[Any, Any, Any]

_ASYNC_HANDLER_REJECTED = (
    "DirectBus dispatches synchronously and cannot await async handlers;"
    " register them on AsyncDirectBus instead"
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


class BaseDirectBus:
    """Handler registry and responder lookup shared by the direct buses."""

    def __init__(self) -> None:
        """Create an empty handler registry."""
        self._handlers: dict[_AnyEvent, set[Handler[Any, _AnyEvent, Any]]] = defaultdict(set)

    def _register(self, config: _AnyEvent, fn: Callable[..., Any]) -> Handler[Any, _AnyEvent, Any]:
        """Wrap fn as a Handler and store it in the registry under config."""

        def call_without_payload(_: Any) -> Any:
            return fn()

        call: Callable[..., Any] = call_without_payload if config.schema is NoPayload else fn
        handler: Handler[Any, _AnyEvent, Any] = Handler(call, config, is_async_callable(fn))
        self._handlers[config].add(handler)
        return handler

    def remove(self, handler: Handler[Any, _AnyEvent, Any]) -> None:
        """
        Remove a registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>` from `handle`.
        """
        self._handlers[handler.config].discard(handler)

    def _single_responder(self, event: _AnyEvent) -> Handler[Any, _AnyEvent, Any]:
        """Return event's sole responder, or raise if there isn't exactly one."""
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


class DirectBus(BaseDirectBus, BindMixin[None]):
    """
    In-process, synchronous event bus.

    {py:class}`PubSub <stratae.events.event.PubSub>` events fan out to every handler.
    {py:class}`Request <stratae.events.event.Request>` events return their one responder's
    reply. Async handlers are rejected with `TypeError`.

    :param use_envelope: Open an {py:class}`Envelope <stratae.events.envelope.Envelope>`
        for each emission.
    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Create a bus, optionally with envelope tracking."""
        super().__init__()
        self._dispatch: Callable[[Any, _AnyEvent], Any] = (
            self._dispatch_in_envelope if use_envelope else self._dispatch_plain
        )

    def emit[S, R, Signal: bool](
        self,
        event: Event[DispatchPattern[R, Any], S, Signal],
        config: None,  # noqa: S1172
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,  # noqa: S1172
    ) -> R:
        """
        Dispatch a payload to the event's handlers.

        Pub/sub handler failures are raised together as an {py:exc}`ExceptionGroup`. A
        responder's exception propagates directly.

        :param event: The {py:class}`Event <stratae.events.event.Event>` to dispatch.
        :param config: Unused.
        :param payload: The payload to dispatch.
        :param serializer: Unused.
        :returns: The responder's reply for a request event, otherwise `None`.
        :raises NoResponderError: If a request event has no responder.
        :raises MultipleRespondersError: If a request event has more than one responder.
        """
        return self._dispatch(payload, event)

    @overload
    def handle[R](
        self,
        config: Event[DispatchPattern[Any, R], NoPayload, Literal[True]],
        fn: Callable[[], R],
    ) -> Handler[[], _AnyEvent, R]: ...

    @overload
    def handle[R](
        self,
        config: Event[DispatchPattern[Any, R], NoPayload, Literal[True]],
        fn: None = None,
    ) -> Callable[[Callable[[], R]], Handler[[], _AnyEvent, R]]: ...

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S, Literal[False]],
        fn: Callable[[S], R],
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S, Literal[False]],
        fn: None = None,
    ) -> Callable[[Callable[[S], R]], Handler[[S], _AnyEvent, R]]: ...

    def handle(
        self,
        config: _AnyEvent,
        fn: Callable[..., Any] | None = None,
    ) -> Any:
        """
        Register a sync handler for an event, as a decorator or direct call.

        A request event's handler returns the reply; a pub/sub handler's return is ignored.

        :param config: The {py:class}`Event <stratae.events.event.Event>` to handle.
        :param fn: The handler. If omitted, returns a decorator.
        :returns: The {py:class}`Handler <stratae.events.handler.Handler>`, for `remove`.
        """
        if fn is not None:
            return self._register(config, fn)

        def decorator(f: Callable[..., Any]) -> Handler[..., _AnyEvent, Any]:
            return self._register(config, f)

        return decorator

    def _register(self, config: _AnyEvent, fn: Callable[..., Any]) -> Handler[Any, _AnyEvent, Any]:
        """Wrap fn as a Handler, raising TypeError if it's async."""
        if is_async_callable(fn):
            raise TypeError(_ASYNC_HANDLER_REJECTED)
        return super()._register(config, fn)

    def _dispatch_plain(self, payload: Any, event: _AnyEvent) -> Any:
        """Dispatch payload without an envelope."""
        if is_request(event):
            return self._dispatch_request(payload, event)
        self._dispatch_fanout(payload, event)
        return None

    def _dispatch_fanout(self, payload: Any, event: _AnyEvent) -> None:
        """Call every handler for event, gathering failures into one group."""
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
        """Call event's sole responder and return its reply."""
        return self._single_responder(event)(payload)

    def _dispatch_in_envelope(self, payload: Any, event: _AnyEvent) -> Any:
        """Dispatch payload inside a new envelope."""
        with Envelope.scope():
            return self._dispatch_plain(payload, event)


class AsyncDirectBus(BaseDirectBus, AsyncBindMixin[None]):
    """
    In-process, asynchronous event bus.

    Accepts sync and async handlers. {py:class}`PubSub <stratae.events.event.PubSub>`
    events fan out concurrently. {py:class}`Request <stratae.events.event.Request>` events
    resolve to their one responder's reply.

    :param use_envelope: Open an {py:class}`Envelope <stratae.events.envelope.Envelope>`
        for each emission.
    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Create a bus, optionally with envelope tracking."""
        super().__init__()
        self._use_envelope = use_envelope

    async def emit[S, R, Signal: bool](
        self,
        event: Event[DispatchPattern[R, Any], S, Signal],
        config: None,  # noqa: S1172
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,  # noqa: S1172
    ) -> R:
        """
        Dispatch a payload to the event's handlers.

        Pub/sub handler failures are raised together as an exception group. A responder's
        exception propagates directly.

        :param event: The {py:class}`Event <stratae.events.event.Event>` to dispatch.
        :param config: Unused.
        :param payload: The payload to dispatch.
        :param serializer: Unused.
        :returns: The responder's reply for a request event, otherwise `None`.
        :raises NoResponderError: If a request event has no responder.
        :raises MultipleRespondersError: If a request event has more than one responder.
        """
        if self._use_envelope:
            with Envelope.scope():
                return await self._dispatch(payload, event)
        return await self._dispatch(payload, event)

    @overload
    def handle[R](
        self,
        config: Event[DispatchPattern[Any, R], NoPayload, Literal[True]],
        fn: Callable[[], Awaitable[R]],
    ) -> Handler[[], _AnyEvent, Awaitable[R]]: ...

    @overload
    def handle[R](
        self,
        config: Event[DispatchPattern[Any, R], NoPayload, Literal[True]],
        fn: Callable[[], R],
    ) -> Handler[[], _AnyEvent, R]: ...

    @overload
    def handle[R](
        self,
        config: Event[DispatchPattern[Any, R], NoPayload, Literal[True]],
        fn: None = None,
    ) -> _AsyncSignalHandlerDecorator[R]: ...

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S, Literal[False]],
        fn: Callable[[S], Awaitable[R]],
    ) -> Handler[[S], _AnyEvent, Awaitable[R]]: ...

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S, Literal[False]],
        fn: Callable[[S], R],
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S, R](
        self,
        config: Event[DispatchPattern[Any, R], S, Literal[False]],
        fn: None = None,
    ) -> _AsyncHandlerDecorator[S, R]: ...

    def handle(
        self,
        config: _AnyEvent,
        fn: Callable[..., Any] | None = None,
    ) -> Any:
        """
        Register a sync or async handler for an event, as a decorator or direct call.

        A request event's handler returns the reply; a pub/sub handler's return is ignored.

        :param config: The {py:class}`Event <stratae.events.event.Event>` to handle.
        :param fn: The handler. If omitted, returns a decorator.
        :returns: The {py:class}`Handler <stratae.events.handler.Handler>`, for `remove`.
        """
        if fn is not None:
            return self._register(config, fn)

        def decorator(f: Callable[..., Any]) -> Handler[..., _AnyEvent, Any]:
            return self._register(config, f)

        return decorator

    async def _dispatch(self, payload: Any, event: _AnyEvent) -> Any:
        """Dispatch payload as a request or pub/sub fan-out."""
        if is_request(event):
            return await self._dispatch_request(payload, event)
        await self.dispatch(payload, config=event)
        return None

    async def _dispatch_request(self, payload: Any, event: _AnyEvent) -> Any:
        """Call event's sole responder, awaiting it if async."""
        responder = self._single_responder(event)
        if responder.is_async:
            return await responder(payload)
        return responder(payload)

    async def dispatch(self, payload: Any, *, config: _AnyEvent) -> None:
        """
        Run every handler for an event concurrently.

        :param payload: The payload to dispatch.
        :param config: The {py:class}`Event <stratae.events.event.Event>` to dispatch.
        :raises BaseExceptionGroup: If any handler raises. An `ExceptionGroup` unless one
            raised a `BaseException` such as `CancelledError`.
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
        exceptions = [r for r in results if isinstance(r, BaseException)]
        if exceptions:
            raise BaseExceptionGroup("handler errors", exceptions)
