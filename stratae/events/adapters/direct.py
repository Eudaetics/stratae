"""Direct, in-process synchronous event bus."""

from collections import defaultdict
from typing import Any, Callable, overload

from stratae.events.bound import BoundEvent, bind
from stratae.events.envelope import Envelope
from stratae.events.event import EventConfig, PubSub, Request, is_request
from stratae.events.exceptions import MultipleRespondersError, NoResponderError
from stratae.events.handler import Handler

_AnyEventConfig = EventConfig[Any, Any, Any]


class DirectBus:
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
    ``MultipleRespondersError`` is raised.

    Args:
        use_envelope: When ``True``, each emission opens a scoped
                      ``Envelope`` for correlation tracking.  Defaults to
                      ``False`` for pure in-process dispatch with no envelope
                      overhead.

    Example::

        bus = DirectBus()

        create_book = bind(bus.emit, event(PubSub)(Book), config=None)

        @bus.handle(create_book.event)
        def save_book(book: Book) -> None: ...

        create_book(title="Dune", author="Herbert")

        find_book = bus.bind(event(Request[Book])(BookQuery))

        @bus.handle(find_book.event)
        def lookup(query: BookQuery) -> Book: ...

        book = find_book(title="Dune")

    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        self._handlers: dict[_AnyEventConfig, set[Handler[Any, _AnyEventConfig, Any]]] = (
            defaultdict(set)
        )
        self._dispatch: Callable[[Any, _AnyEventConfig], Any] = (
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

    def bind(self, event: _AnyEventConfig) -> BoundEvent[Any, Any, Any, None, Any]:
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
        event: _AnyEventConfig,
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
    def handle[**P, R](
        self,
        config: _AnyEventConfig,
        fn: Callable[P, R],
    ) -> Handler[P, _AnyEventConfig, R]: ...

    @overload
    def handle[**P, R](
        self,
        config: _AnyEventConfig,
        fn: None = None,
    ) -> Callable[[Callable[P, R]], Handler[P, _AnyEventConfig, R]]: ...

    def handle[**P, R](
        self,
        config: _AnyEventConfig,
        fn: Callable[P, R] | None = None,
    ) -> (
        Handler[P, _AnyEventConfig, R] | Callable[[Callable[P, R]], Handler[P, _AnyEventConfig, R]]
    ):
        """
        Register a handler callable for a config, as a decorator or direct call.

        Returns the ``Handler`` instance in both forms so callers can pass it
        to ``remove`` later.

        Args:
            config: The ``EventConfig`` used as the handler routing key.
            fn:     When supplied, registers ``fn`` directly and returns its
                    ``Handler``.  When omitted, returns a decorator that
                    registers and returns the ``Handler``.

        """

        def decorator(f: Callable[P, R]) -> Handler[P, _AnyEventConfig, R]:
            handler: Handler[P, _AnyEventConfig, R] = Handler(f, config)
            self._handlers[config].add(handler)
            return handler

        if fn is not None:
            return decorator(fn)
        return decorator

    def remove(self, handler: Handler[Any, _AnyEventConfig, Any]) -> None:
        """
        Remove a previously registered handler.

        Args:
            handler: The ``Handler`` instance returned by ``handle``.

        """
        self._handlers[handler.config].discard(handler)

    def _dispatch_plain(self, payload: Any, event: _AnyEventConfig) -> Any:
        if is_request(event):
            return self._dispatch_request(payload, event)
        self._dispatch_fanout(payload, event)
        return None

    def _dispatch_fanout(self, payload: Any, event: _AnyEventConfig) -> None:
        exceptions: list[Exception] = []
        handlers = list(self._handlers.get(event, ()))
        for handler in handlers:
            try:
                handler(payload)
            except Exception as exc:
                exceptions.append(exc)
        if exceptions:
            raise ExceptionGroup("Handler Errors", exceptions)

    def _dispatch_request(self, payload: Any, event: _AnyEventConfig) -> Any:
        responders = self._handlers.get(event)
        if not responders:
            raise NoResponderError(f"no responder registered for request event '{event.name}'")
        if len(responders) > 1:
            raise MultipleRespondersError(
                f"request event '{event.name}' has {len(responders)} responders;"
                " exactly one is required"
            )
        (responder,) = responders
        return responder(payload)

    def _dispatch_in_envelope(self, payload: Any, event: _AnyEventConfig) -> Any:
        with Envelope.scope():
            return self._dispatch_plain(payload, event)
