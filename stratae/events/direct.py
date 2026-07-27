"""Direct, in-process synchronous and asynchronous event buses."""

import asyncio
from collections import defaultdict
from inspect import iscoroutinefunction
from typing import Any, Awaitable, Callable, Protocol, overload

from stratae.events.bound import (
    AsyncBoundEvent,
    AsyncFactoryBoundEvent,
    BoundEvent,
    FactoryBoundEvent,
    abind,
    bind,
)
from stratae.events.envelope import Envelope
from stratae.events.event import Event, PubSub, Request, is_request
from stratae.events.exceptions import MultipleRespondersError, NoResponderError
from stratae.events.handler import Handler

_AnyEvent = Event[Any, Any]

_ASYNC_HANDLER_REJECTED = (
    "DirectBus dispatches synchronously and cannot await async handlers;"
    " register them on AsyncDirectBus instead"
)


class _HandlerDecorator[S: Any](Protocol):
    """Decorator form of ``handle`` for pub/sub events: registers and returns the ``Handler``."""

    def __call__[R](self, fn: Callable[[S], R]) -> Handler[[S], _AnyEvent, R]:
        """Register ``fn`` as a handler and return its ``Handler``."""
        ...


class _AsyncResponderDecorator[S: Any, R](Protocol):
    """Decorator form of ``handle`` for request events: registers and returns the ``Handler``."""

    @overload
    def __call__(
        self, fn: Callable[[S], Awaitable[R]]
    ) -> Handler[[S], _AnyEvent, Awaitable[R]]: ...
    @overload
    def __call__(self, fn: Callable[[S], R]) -> Handler[[S], _AnyEvent, R]: ...


class BaseDirectBus:
    """
    Shared handler registry for the direct bus adapters.

    Holds the config-keyed handler registrations and the request responder
    resolution common to ``DirectBus`` and ``AsyncDirectBus``.  Subclasses
    own their emit/bind surfaces, their typed ``handle`` overloads, and
    dispatch semantics; registration delegates to ``_register``.
    """

    def __init__(self) -> None:
        """Initialise the handler registry."""
        self._handlers: dict[_AnyEvent, set[Handler[Any, _AnyEvent, Any]]] = defaultdict(set)

    def _register[**P, R](self, config: _AnyEvent, fn: Callable[P, R]) -> Handler[P, _AnyEvent, R]:
        handler: Handler[P, _AnyEvent, R] = Handler(fn, config)
        self._handlers[config].add(handler)
        return handler

    def remove(self, handler: Handler[Any, _AnyEvent, Any]) -> None:
        """
        Remove a previously registered handler.

        Args:
            handler: The ``Handler`` instance returned by ``handle``.

        """
        self._handlers[handler.config].discard(handler)

    def _single_responder(self, event: _AnyEvent) -> Handler[Any, _AnyEvent, Any]:
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

    Bind ``DirectBus.emit`` to an ``Event`` via ``bind`` to create a
    callable that dispatches payloads to registered handlers, optionally
    building them from a factory.  Register handlers with ``handle``, using
    the same ``Event`` as the routing key.  Each ``handle`` call is an
    independent registration; the same callable may be registered multiple
    times.

    Pub/sub events fan out to every registered handler and emit returns
    ``None``.  Request events block until the registered responder returns
    and emit returns its reply; a request event must have exactly one
    responder at emit time, otherwise ``NoResponderError`` or
    ``MultipleRespondersError`` is raised.  Handlers must be synchronous
    callables; registering an async handler raises ``TypeError``.

    Args:
        use_envelope: When ``True``, each emission opens a scoped
                      ``Envelope`` for correlation tracking.  Defaults to
                      ``False`` for pure in-process dispatch with no envelope
                      overhead.

    Example::

        bus = DirectBus()

        book_created = Event(Book, PubSub)
        create_book = bus.bind(book_created, factory=Book)

        @bus.handle(book_created)
        def save_book(book: Book) -> None: ...

        create_book(title="Dune", author="Herbert")

        book_requested = Event(BookQuery, Request[Book])
        find_book = bus.bind(book_requested, factory=BookQuery)

        @bus.handle(book_requested)
        def lookup(query: BookQuery) -> Book: ...

        book = find_book(title="Dune")

    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        super().__init__()
        self._dispatch: Callable[[Any, _AnyEvent], Any] = (
            self._dispatch_in_envelope if use_envelope else self._dispatch_plain
        )

    @overload
    def bind[**P, S: Any, R](
        self, event: Event[S, Request[R]], *, factory: Callable[P, S]
    ) -> FactoryBoundEvent[P, S, Request[R], None, R]: ...

    @overload
    def bind[**P, S: Any](
        self, event: Event[S, PubSub], *, factory: Callable[P, S]
    ) -> FactoryBoundEvent[P, S, PubSub, None, None]: ...

    @overload
    def bind[S: Any, R](
        self, event: Event[S, Request[R]]
    ) -> BoundEvent[S, Request[R], None, R]: ...

    @overload
    def bind[S: Any](self, event: Event[S, PubSub]) -> BoundEvent[S, PubSub, None, None]: ...

    def bind(
        self, event: _AnyEvent, *, factory: Callable[..., Any] | None = None
    ) -> FactoryBoundEvent[Any, Any, Any, None, Any] | BoundEvent[Any, Any, None, Any]:
        """Return a ``BoundEvent`` or ``FactoryBoundEvent`` for this bus, with ``config=None``."""
        return bind(self.emit, event, factory=factory, config=None, serializer=None)

    @overload
    def emit[S: Any, R](
        self,
        payload: S,
        event: Event[S, Request[R]],
        config: None = None,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R: ...

    @overload
    def emit[S: Any](
        self,
        payload: S,
        event: Event[S, PubSub],
        config: None = None,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> None: ...

    def emit(
        self,
        payload: Any,
        event: _AnyEvent,
        config: None = None,
        *,
        serializer: Callable[..., Any] | None = None,
    ) -> Any:
        """
        Dispatch the payload to registered handlers, opening an envelope scope if configured.

        Pub/sub events fan out to every registered handler; handler
        exceptions are collected into an ``ExceptionGroup``.  Request events
        dispatch to exactly one responder, block until it returns, and
        propagate its exceptions directly.

        Args:
            payload:    The payload instance to dispatch.
            event:      The ``Event`` used as the handler lookup key.
            config:     Unused; ``DirectBus`` requires no routing config.
            serializer: Unused; ``DirectBus`` requires no serializer.

        Returns:
            The responder's reply for request events; ``None`` for pub/sub.

        Raises:
            NoResponderError:        When a request event has no registered
                                     responder.
            MultipleRespondersError: When a request event has more than one
                                     registered responder.

        """
        return self._dispatch(payload, event)

    @overload
    def handle[S: Any, R](
        self,
        config: Event[S, Request[R]],
        fn: Callable[[S], R],
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S: Any, R](
        self,
        config: Event[S, Request[R]],
        fn: None = None,
    ) -> Callable[[Callable[[S], R]], Handler[[S], _AnyEvent, R]]: ...

    @overload
    def handle[S: Any, R](
        self,
        config: Event[S, PubSub],
        fn: Callable[[S], R],
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S: Any](
        self,
        config: Event[S, PubSub],
        fn: None = None,
    ) -> _HandlerDecorator[S]: ...

    def handle(
        self,
        config: _AnyEvent,
        fn: Callable[..., Any] | None = None,
    ) -> Any:
        """
        Register a handler for a config, as a decorator or direct call.

        For request events the callable is the responder: it accepts the
        event's payload and must return the event's reply type.  For pub/sub
        events the callable accepts the payload and its return value is
        ignored.  Async callables are rejected with ``TypeError`` on this
        synchronous bus; register them on ``AsyncDirectBus`` instead.

        Returns the ``Handler`` instance in both forms so callers can pass it
        to ``remove`` later.

        Args:
            config: The ``Event`` used as the handler routing key.
            fn:     When supplied, registers ``fn`` directly and returns its
                    ``Handler``.  When omitted, returns a decorator that
                    registers and returns the ``Handler``.

        """
        if fn is not None:
            return self._register(config, fn)

        def decorator(f: Callable[..., Any]) -> Handler[..., _AnyEvent, Any]:
            return self._register(config, f)

        return decorator

    def _register[**P, R](self, config: _AnyEvent, fn: Callable[P, R]) -> Handler[P, _AnyEvent, R]:
        if iscoroutinefunction(fn):
            raise TypeError(_ASYNC_HANDLER_REJECTED)
        return super()._register(config, fn)

    def _dispatch_plain(self, payload: Any, event: _AnyEvent) -> Any:
        if is_request(event):
            return self._dispatch_request(payload, event)
        self._dispatch_fanout(payload, event)
        return None

    def _dispatch_fanout(self, payload: Any, event: _AnyEvent) -> None:
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
        return self._single_responder(event)(payload)

    def _dispatch_in_envelope(self, payload: Any, event: _AnyEvent) -> Any:
        with Envelope.scope():
            return self._dispatch_plain(payload, event)


class AsyncDirectBus(BaseDirectBus):
    """
    In-process, asynchronous event bus with no routing config.

    Bind ``AsyncDirectBus.emit`` to an ``Event`` via ``abind`` to create a
    callable that dispatches payloads to registered handlers, optionally
    building them from a factory.  Register handlers with ``handle``, using
    the same ``Event`` as the routing key.  Sync and async handlers are both
    supported; all are dispatched concurrently via ``asyncio.gather``.  Each
    ``handle`` call is an independent registration; the same callable may be
    registered multiple times.

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
        order_placed = Event(OrderPlaced, PubSub)
        place_order = bus.bind(order_placed, factory=OrderPlaced)

        @bus.handle(order_placed)
        async def on_order(payload: OrderPlaced) -> None: ...

        await place_order(order_id=42)

        book_requested = Event(BookQuery, Request[Book])
        find_book = bus.bind(book_requested, factory=BookQuery)

        @bus.handle(book_requested)
        async def lookup(query: BookQuery) -> Book: ...

        book = await find_book(title="Dune")

    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        super().__init__()
        self._use_envelope = use_envelope

    @overload
    def bind[**P, S: Any, R](
        self,
        event: Event[S, Request[R]],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
    ) -> AsyncFactoryBoundEvent[P, S, Request[R], None, R]: ...

    @overload
    def bind[**P, S: Any](
        self,
        event: Event[S, PubSub],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
    ) -> AsyncFactoryBoundEvent[P, S, PubSub, None, None]: ...

    @overload
    def bind[S: Any, R](
        self, event: Event[S, Request[R]]
    ) -> AsyncBoundEvent[S, Request[R], None, R]: ...

    @overload
    def bind[S: Any](self, event: Event[S, PubSub]) -> AsyncBoundEvent[S, PubSub, None, None]: ...

    def bind(
        self, event: _AnyEvent, *, factory: Callable[..., Any] | None = None
    ) -> AsyncFactoryBoundEvent[Any, Any, Any, None, Any] | AsyncBoundEvent[Any, Any, None, Any]:
        """Return an ``AsyncBoundEvent`` or ``AsyncFactoryBoundEvent`` for this bus."""
        return abind(self.emit, event, factory=factory, config=None, serializer=None)

    @overload
    async def emit[S: Any, R](
        self,
        payload: S,
        event: Event[S, Request[R]],
        config: None = None,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R: ...

    @overload
    async def emit[S: Any](
        self,
        payload: S,
        event: Event[S, PubSub],
        config: None = None,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> None: ...

    async def emit(
        self,
        payload: Any,
        event: _AnyEvent,
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
            payload:    The payload instance to dispatch.
            event:      The ``Event`` used as the handler lookup key.
            config:     Unused; ``AsyncDirectBus`` requires no routing config.
            serializer: Unused; ``AsyncDirectBus`` requires no serializer.

        Returns:
            The responder's reply for request events; ``None`` for pub/sub.

        Raises:
            NoResponderError:        When a request event has no registered
                                     responder.
            MultipleRespondersError: When a request event has more than one
                                     responder.

        """
        if self._use_envelope:
            with Envelope.scope():
                return await self._dispatch(payload, event)
        return await self._dispatch(payload, event)

    @overload
    def handle[S: Any, R](
        self,
        config: Event[S, Request[R]],
        fn: Callable[[S], Awaitable[R]],
    ) -> Handler[[S], _AnyEvent, Awaitable[R]]: ...

    @overload
    def handle[S: Any, R](
        self,
        config: Event[S, Request[R]],
        fn: Callable[[S], R],
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S: Any, R](
        self,
        config: Event[S, Request[R]],
        fn: None = None,
    ) -> _AsyncResponderDecorator[S, R]: ...

    @overload
    def handle[S: Any, R](
        self,
        config: Event[S, PubSub],
        fn: Callable[[S], R],
    ) -> Handler[[S], _AnyEvent, R]: ...

    @overload
    def handle[S: Any](
        self,
        config: Event[S, PubSub],
        fn: None = None,
    ) -> _HandlerDecorator[S]: ...

    def handle(
        self,
        config: _AnyEvent,
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
            config: The ``Event`` used as the handler routing key.
            fn:     When supplied, registers ``fn`` directly and returns its
                    ``Handler``.  When omitted, returns a decorator that
                    registers and returns the ``Handler``.

        """
        if fn is not None:
            return self._register(config, fn)

        def decorator(f: Callable[..., Any]) -> Handler[..., _AnyEvent, Any]:
            return self._register(config, f)

        return decorator

    async def _dispatch(self, payload: Any, event: _AnyEvent) -> Any:
        if is_request(event):
            return await self._dispatch_request(payload, event)
        await self.dispatch(payload, config=event)
        return None

    async def _dispatch_request(self, payload: Any, event: _AnyEvent) -> Any:
        responder = self._single_responder(event)
        if responder.is_async:
            return await responder(payload)
        return responder(payload)

    async def dispatch(self, payload: Any, *, config: _AnyEvent) -> None:
        """
        Invoke every handler registered for the given ``Event`` concurrently.

        Sync handlers are called directly; async handlers are awaited.  Both
        are dispatched via ``asyncio.gather``.

        Args:
            payload: The payload instance to dispatch.
            config:  The ``Event`` used as the handler lookup key.

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
