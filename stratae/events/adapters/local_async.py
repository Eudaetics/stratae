"""Direct, in-process asynchronous event bus."""

import asyncio
from typing import Any

from stratae.events.bound import AsyncBoundEvent
from stratae.events.envelope import Envelope
from stratae.events.event import Payload
from stratae.events.handler import Handler
from stratae.events.mixins.publish import AsyncBasicPublisher
from stratae.events.mixins.subscribe import AsyncSubscriber


class AsyncLocalBus(AsyncBasicPublisher[None], AsyncSubscriber[AsyncBoundEvent[Any, None, None]]):
    """
    In-process, asynchronous event bus with no routing config.

    The ``AsyncBoundEvent`` returned by ``publish`` serves as both the emit handle and
    the subscription key.  Pass it as ``config`` to ``subscribe`` to register a
    handler; await it to emit an event.  Sync and async handlers are both
    supported; all are dispatched concurrently via ``asyncio.gather``.  Each
    call to ``subscribe`` is an independent registration; the same callable may
    be subscribed multiple times.

    Args:
        use_envelope: When ``True``, each emission opens a scoped
                      ``Envelope`` for correlation tracking.  Defaults to
                      ``False`` for pure in-process dispatch with no envelope
                      overhead.

    Example::

        bus = AsyncLocalBus()

        order_placed = bus.publish(OrderPlaced)

        @bus.subscribe(order_placed)
        async def on_order(payload: OrderPlaced) -> None: ...

        await order_placed(order_id=42)

    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        super().__init__()
        self._use_envelope = use_envelope

    async def emit_publish[**P](
        self, payload: Payload, event: AsyncBoundEvent[P, None, None]
    ) -> None:
        """
        Open a scoped envelope and dispatch the payload to all registered handlers.

        Each emission runs inside its own ``Envelope``, or a child of the
        currently active one, enabling correlation across nested emissions.

        Args:
            payload: The constructed ``Payload`` instance to dispatch.
            event:   The ``AsyncBoundEvent`` used as the handler lookup key.

        """
        if self._use_envelope:
            with Envelope.scope():
                await self.handle_subscribe(payload, config=event)
        else:
            await self.handle_subscribe(payload, config=event)

    async def handle_subscribe[**P](
        self, payload: Payload, *, config: AsyncBoundEvent[P, None, None]
    ) -> None:
        """
        Invoke every handler registered for the given bound event concurrently.

        Sync handlers are called directly; async handlers are awaited.  Both
        are dispatched via ``asyncio.gather``.

        Args:
            payload: The constructed ``Payload`` instance to dispatch.
            config:  The ``AsyncBoundEvent`` used as the handler lookup key.

        """

        async def _call(
            handler: Handler[[Payload], AsyncBoundEvent[P, None, None], Any],
        ) -> None:
            if handler.is_async:
                await handler(payload)
            else:
                handler(payload)

        results = await asyncio.gather(
            *(_call(h) for h in self.get_handlers(config)), return_exceptions=True
        )
        exceptions = [r for r in results if isinstance(r, Exception)]
        if exceptions:
            raise ExceptionGroup("handler errors", exceptions)
