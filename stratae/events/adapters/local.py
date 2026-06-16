"""Direct, in-process synchronous event bus."""

from collections import defaultdict
from typing import Any, Callable, overload

from stratae.events.bound import BoundEvent, bind
from stratae.events.envelope import Envelope
from stratae.events.event import EventConfig, EventType, Payload
from stratae.events.handler import Handler

_AnyEventConfig = EventConfig[Any, Any, Any]


class LocalBus:
    """
    In-process, synchronous event bus with no routing config.

    Bind ``LocalBus.emit`` to an ``EventConfig`` via ``bind`` to create a
    callable that constructs payloads and dispatches them to registered
    handlers.  Register handlers with ``handle``, using the same
    ``EventConfig`` as the routing key.  Each ``handle`` call is an
    independent registration; the same callable may be registered multiple
    times.

    Args:
        use_envelope: When ``True``, each emission opens a scoped
                      ``Envelope`` for correlation tracking.  Defaults to
                      ``False`` for pure in-process dispatch with no envelope
                      overhead.

    Example::

        bus = LocalBus()

        create_book = bind(bus.emit, event(PubSub)(Book), config=None)

        @bus.handle(create_book.event)
        def save_book(book: Book) -> None: ...

        create_book(title="Dune", author="Herbert")

    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        self._handlers: dict[_AnyEventConfig, set[Handler[Any, _AnyEventConfig, Any]]] = (
            defaultdict(set)
        )
        self._dispatch: Callable[[Payload, _AnyEventConfig, None], None] = (
            self._dispatch_in_envelope if use_envelope else self._dispatch_plain
        )

    def bind[**P, S: Payload, T: EventType](
        self, event: EventConfig[P, S, T]
    ) -> BoundEvent[P, S, T, None, None]:
        """Return a ``BoundEvent`` pre-populated with this bus's emit and ``config=None``."""
        return bind(self.emit, event, config=None)

    def emit(self, payload: Payload, event: _AnyEventConfig, _config: None = None) -> None:
        """
        Dispatch the payload to registered handlers, opening an envelope scope if configured.

        Args:
            payload:  The constructed ``Payload`` instance to dispatch.
            event:    The ``EventConfig`` used as the handler lookup key.
            _config:  Unused; ``LocalBus`` requires no routing config.

        """
        self._dispatch(payload, event, _config)

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

    def _dispatch_plain(self, payload: Payload, event: _AnyEventConfig, _config: None) -> None:
        exceptions: list[Exception] = []
        for handler in self._handlers.get(event, ()):
            try:
                handler(payload)
            except Exception as exc:
                exceptions.append(exc)
        if exceptions:
            raise ExceptionGroup("Handler Errors", exceptions)

    def _dispatch_in_envelope(
        self, payload: Payload, event: _AnyEventConfig, _config: None
    ) -> None:
        with Envelope.scope():
            self._dispatch_plain(payload, event, None)
