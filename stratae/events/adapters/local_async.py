"""Direct, in-process asynchronous event bus."""

import asyncio
from collections import defaultdict
from typing import Any, Callable, overload

from stratae.events.bound import AsyncBoundEvent, abind
from stratae.events.envelope import Envelope
from stratae.events.event import EventConfig, EventType, Payload
from stratae.events.handler import Handler

_AnyEventConfig = EventConfig[Any, Any, Any]


class AsyncLocalBus:
    """
    In-process, asynchronous event bus with no routing config.

    Bind ``AsyncLocalBus.emit`` to an ``EventConfig`` via ``abind`` to create a
    callable that constructs payloads and dispatches them to registered handlers.
    Register handlers with ``handle``, using the same ``EventConfig`` as the
    routing key.  Sync and async handlers are both supported; all are dispatched
    concurrently via ``asyncio.gather``.  Each ``handle`` call is an independent
    registration; the same callable may be registered multiple times.

    Args:
        use_envelope: When ``True``, each emission opens a scoped
                      ``Envelope`` for correlation tracking.  Defaults to
                      ``False`` for pure in-process dispatch with no envelope
                      overhead.

    Example::

        bus = AsyncLocalBus()

        order_placed = abind(bus.emit, event(PubSub)(OrderPlaced), config=None)

        @bus.handle(order_placed.event)
        async def on_order(payload: OrderPlaced) -> None: ...

        await order_placed(order_id=42)

    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        self._use_envelope = use_envelope
        self._handlers: dict[_AnyEventConfig, set[Handler[Any, _AnyEventConfig, Any]]] = (
            defaultdict(set)
        )

    def bind[**P, S: Payload, T: EventType](
        self, event: EventConfig[P, S, T]
    ) -> AsyncBoundEvent[P, S, T, None, None]:
        """Return an ``AsyncBoundEvent`` pre-populated with this bus's emit and ``config=None``."""
        return abind(self.emit, event, config=None)

    async def emit(self, payload: Payload, event: _AnyEventConfig, _config: None) -> None:
        """
        Open a scoped envelope and dispatch the payload to registered handlers.

        Each emission runs inside its own ``Envelope``, or a child of the
        currently active one, enabling correlation across nested emissions.

        Args:
            payload:  The constructed ``Payload`` instance to dispatch.
            event:    The ``EventConfig`` used as the handler lookup key.
            _config:  Unused; ``AsyncLocalBus`` requires no routing config.

        """
        if self._use_envelope:
            with Envelope.scope():
                await self.dispatch(payload, config=event)
        else:
            await self.dispatch(payload, config=event)

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

    async def dispatch(self, payload: Payload, *, config: _AnyEventConfig) -> None:
        """
        Invoke every handler registered for the given ``EventConfig`` concurrently.

        Sync handlers are called directly; async handlers are awaited.  Both
        are dispatched via ``asyncio.gather``.

        Args:
            payload: The constructed ``Payload`` instance to dispatch.
            config:  The ``EventConfig`` used as the handler lookup key.

        """

        async def _call(handler: Handler[Any, _AnyEventConfig, Any]) -> None:
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
