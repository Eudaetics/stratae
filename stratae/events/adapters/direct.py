"""Direct, in-process synchronous event bus."""

from inspect import iscoroutinefunction
from typing import Any, Callable, overload

from stratae.events.adapters._base import AnyEventConfig, BaseDirectBus, HandlerDecorator
from stratae.events.bound import BoundEvent, bind
from stratae.events.envelope import Envelope
from stratae.events.event import EventConfig, PubSub, Request, is_request
from stratae.events.handler import Handler

_ASYNC_HANDLER_REJECTED = (
    "DirectBus dispatches synchronously and cannot await async handlers;"
    " register them on AsyncDirectBus instead"
)


class DirectBus(BaseDirectBus):
    """
    In-process, synchronous event bus with no routing config.

    Bind ``DirectBus.emit`` to an ``EventConfig`` via ``bind`` to create a
    callable that constructs payloads and dispatches them to registered
    handlers.  Register handlers with ``handle``, using the same
    ``EventConfig`` as the routing key.  Each ``handle`` call is an
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

        book_created = EventConfig(Book, PubSub)
        create_book = bus.bind(book_created)


        @bus.handle(book_created)
        def save_book(book: Book) -> None: ...


        create_book(title="Dune", author="Herbert")

        book_requested = EventConfig(BookQuery, Request[Book])
        find_book = bus.bind(book_requested)


        @bus.handle(book_requested)
        def lookup(query: BookQuery) -> Book: ...


        book = find_book(title="Dune")

    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        super().__init__()
        self._dispatch: Callable[[Any, AnyEventConfig], Any] = (
            self._dispatch_in_envelope if use_envelope else self._dispatch_plain
        )

    @overload
    def bind[**P, S: Any, R](
        self, event: EventConfig[P, S, Request[R]]
    ) -> BoundEvent[P, S, Request[R], None, R]: ...

    @overload
    def bind[**P, S: Any](
        self, event: EventConfig[P, S, PubSub]
    ) -> BoundEvent[P, S, PubSub, None, None]: ...

    def bind(self, event: AnyEventConfig) -> BoundEvent[Any, Any, Any, None, Any]:
        """Return a ``BoundEvent`` pre-populated with this bus's emit and ``config=None``."""
        return bind(self.emit, event, config=None, serializer=None)

    @overload
    def emit[**P, S: Any, R](
        self,
        payload: S,
        event: EventConfig[P, S, Request[R]],
        config: None = None,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R: ...

    @overload
    def emit[**P, S: Any](
        self,
        payload: S,
        event: EventConfig[P, S, PubSub],
        config: None = None,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> None: ...

    def emit(
        self,
        payload: Any,
        event: AnyEventConfig,
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
            payload:    The constructed payload instance to dispatch.
            event:      The ``EventConfig`` used as the handler lookup key.
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
    ) -> Callable[[Callable[[S], R]], Handler[[S], AnyEventConfig, R]]: ...

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
        event's payload and must return the event's reply type.  For pub/sub
        events the callable accepts the payload and its return value is
        ignored.  Async callables are rejected with ``TypeError`` on this
        synchronous bus; register them on ``AsyncDirectBus`` instead.

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

    def _register[**P, R](
        self, config: AnyEventConfig, fn: Callable[P, R]
    ) -> Handler[P, AnyEventConfig, R]:
        if iscoroutinefunction(fn):
            raise TypeError(_ASYNC_HANDLER_REJECTED)
        return super()._register(config, fn)

    def _dispatch_plain(self, payload: Any, event: AnyEventConfig) -> Any:
        if is_request(event):
            return self._dispatch_request(payload, event)
        self._dispatch_fanout(payload, event)
        return None

    def _dispatch_fanout(self, payload: Any, event: AnyEventConfig) -> None:
        exceptions: list[Exception] = []
        handlers = list(self._handlers.get(event, ()))
        for handler in handlers:
            try:
                handler(payload)
            except Exception as exc:
                exceptions.append(exc)
        if exceptions:
            raise ExceptionGroup("Handler Errors", exceptions)

    def _dispatch_request(self, payload: Any, event: AnyEventConfig) -> Any:
        return self._single_responder(event)(payload)

    def _dispatch_in_envelope(self, payload: Any, event: AnyEventConfig) -> Any:
        with Envelope.scope():
            return self._dispatch_plain(payload, event)
