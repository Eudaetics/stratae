"""Direct, in-process asynchronous event bus."""

import asyncio
from typing import Any, Awaitable, Callable, Protocol, overload

from stratae.events.adapters._base import AnyEventConfig, BaseDirectBus, HandlerDecorator
from stratae.events.bound import AsyncBoundEvent, abind
from stratae.events.envelope import Envelope
from stratae.events.event import EventConfig, PubSub, Request, is_request
from stratae.events.handler import Handler


class _AsyncResponderDecorator[S: Any, R](Protocol):
    """Decorator form of ``handle`` for request events: registers and returns the ``Handler``."""

    @overload
    def __call__(
        self, fn: Callable[[S], Awaitable[R]]
    ) -> Handler[[S], AnyEventConfig, Awaitable[R]]: ...
    @overload
    def __call__(self, fn: Callable[[S], R]) -> Handler[[S], AnyEventConfig, R]: ...


class AsyncDirectBus(BaseDirectBus):
    """
    In-process, asynchronous event bus with no routing config.

    Bind ``AsyncDirectBus.emit`` to an ``EventConfig`` via ``abind`` to create a
    callable that constructs payloads and dispatches them to registered handlers.
    Register handlers with ``handle``, using the same ``EventConfig`` as the
    routing key.  Sync and async handlers are both supported; all are dispatched
    concurrently via ``asyncio.gather``.  Each ``handle`` call is an independent
    registration; the same callable may be registered multiple times.

    Pub/sub events fan out to every registered handler and emit resolves to
    ``None``.  Request events dispatch to the registered responder, sync or
    async, and emit resolves to its reply; a request event must have exactly
    one responder at emit time, otherwise ``NoResponderError`` or
    ``MultipleRespondersError`` is raised.

    Args:
        use_envelope: When ``True``, each emission opens a scoped
                      ``Envelope`` for correlation tracking.  Defaults to
                      ``False`` for pure in-process dispatch with no envelope
                      overhead.

    Example::

        bus = AsyncDirectBus()

        order_placed = abind(bus.emit, event(PubSub)(OrderPlaced), config=None)

        @bus.handle(order_placed.event)
        async def on_order(payload: OrderPlaced) -> None: ...

        await order_placed(order_id=42)

        find_book = bus.bind(event(Request[Book])(BookQuery))

        @bus.handle(find_book.event)
        async def lookup(query: BookQuery) -> Book: ...

        book = await find_book(title="Dune")

    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        super().__init__()
        self._use_envelope = use_envelope

    @overload
    def bind[**P, S: Any, R](
        self, event: EventConfig[P, S, Request[R]]
    ) -> AsyncBoundEvent[P, S, Request[R], None, R]: ...

    @overload
    def bind[**P, S: Any](
        self, event: EventConfig[P, S, PubSub]
    ) -> AsyncBoundEvent[P, S, PubSub, None, None]: ...

    def bind(self, event: AnyEventConfig) -> AsyncBoundEvent[Any, Any, Any, None, Any]:
        """Return an ``AsyncBoundEvent`` pre-populated with this bus's emit and ``config=None``."""
        return abind(self.emit, event, config=None, serializer=None)

    @overload
    async def emit[**P, S: Any, R](
        self,
        payload: S,
        event: EventConfig[P, S, Request[R]],
        config: None = None,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R: ...

    @overload
    async def emit[**P, S: Any](
        self,
        payload: S,
        event: EventConfig[P, S, PubSub],
        config: None = None,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> None: ...

    async def emit(
        self,
        payload: Any,
        event: AnyEventConfig,
        config: None = None,
        *,
        serializer: Callable[..., Any] | None = None,
    ) -> Any:
        """
        Open a scoped envelope and dispatch the payload to registered handlers.

        Each emission runs inside its own ``Envelope``, or a child of the
        currently active one, enabling correlation across nested emissions.

        Pub/sub events fan out to every registered handler concurrently;
        handler exceptions are collected into an ``ExceptionGroup``.  Request
        events dispatch to exactly one responder, await its reply, and
        propagate its exceptions directly.

        Args:
            payload:    The constructed payload instance to dispatch.
            event:      The ``EventConfig`` used as the handler lookup key.
            config:     Unused; ``AsyncDirectBus`` requires no routing config.
            serializer: Unused; ``AsyncDirectBus`` requires no serializer.

        Returns:
            The responder's reply for request events; ``None`` for pub/sub.

        Raises:
            NoResponderError:        When a request event has no registered
                                     responder.
            MultipleRespondersError: When a request event has more than one
                                     registered responder.

        """
        if self._use_envelope:
            with Envelope.scope():
                return await self._dispatch(payload, event)
        return await self._dispatch(payload, event)

    @overload
    def handle[**P, S: Any, R](
        self,
        config: EventConfig[P, S, Request[R]],
        fn: Callable[[S], Awaitable[R]],
    ) -> Handler[[S], AnyEventConfig, Awaitable[R]]: ...

    @overload
    def handle[**P, S: Any, R](
        self,
        config: EventConfig[P, S, Request[R]],
        fn: Callable[[S], R],
    ) -> Handler[[S], AnyEventConfig, R]: ...

    @overload
    def handle[**P, S: Any, R](
        self,
        config: EventConfig[P, S, Request[R]],
        fn: None = None,
    ) -> _AsyncResponderDecorator[S, R]: ...

    @overload
    def handle[**P, S: Any, R](
        self,
        config: EventConfig[P, S, PubSub],
        fn: Callable[[S], R],
    ) -> Handler[[S], AnyEventConfig, R]: ...

    @overload
    def handle[**P, S: Any](
        self,
        config: EventConfig[P, S, PubSub],
        fn: None = None,
    ) -> HandlerDecorator[S]: ...

    def handle(
        self,
        config: AnyEventConfig,
        fn: Callable[..., Any] | None = None,
    ) -> Any:
        """
        Register a handler for a config, as a decorator or direct call.

        For request events the callable is the responder: it accepts the
        event's payload and must return the event's reply type, either
        directly or as an awaitable that resolves to it.  For pub/sub events
        the callable accepts the payload and its return value is ignored.

        Returns the ``Handler`` instance in both forms so callers can pass it
        to ``remove`` later.

        Args:
            config: The ``EventConfig`` used as the handler routing key.
            fn:     When supplied, registers ``fn`` directly and returns its
                    ``Handler``.  When omitted, returns a decorator that
                    registers and returns the ``Handler``.

        """
        if fn is not None:
            return self._register(config, fn)

        def decorator(f: Callable[..., Any]) -> Handler[..., AnyEventConfig, Any]:
            return self._register(config, f)

        return decorator

    async def _dispatch(self, payload: Any, event: AnyEventConfig) -> Any:
        if is_request(event):
            return await self._dispatch_request(payload, event)
        await self.dispatch(payload, config=event)
        return None

    async def _dispatch_request(self, payload: Any, event: AnyEventConfig) -> Any:
        responder = self._single_responder(event)
        if responder.is_async:
            return await responder(payload)
        return responder(payload)

    async def dispatch(self, payload: Any, *, config: AnyEventConfig) -> None:
        """
        Invoke every handler registered for the given ``EventConfig`` concurrently.

        Sync handlers are called directly; async handlers are awaited.  Both
        are dispatched via ``asyncio.gather``.

        Args:
            payload: The constructed payload instance to dispatch.
            config:  The ``EventConfig`` used as the handler lookup key.

        """

        async def _call(handler: Handler[Any, AnyEventConfig, Any]) -> None:
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
