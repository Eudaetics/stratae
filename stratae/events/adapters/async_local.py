"""Direct, in-process asynchronous event bus."""

import asyncio
from typing import Any, Awaitable

from stratae.events.envelope import scoped_envelope
from stratae.events.event import BoundEvent, EventSchema
from stratae.events.handler import Handler
from stratae.events.mixins.publish import AsyncPublisher
from stratae.events.mixins.subscribe import AsyncSubscriber


class AsyncLocalBus(AsyncPublisher[None, None], AsyncSubscriber[BoundEvent[Any, Any, Any]]):
    """
    In-process, asynchronous event bus with no routing metadata.

    Dispatches every event emitted on a channel to all handlers registered
    on that channel.  Sync and async handlers are both supported; all are
    dispatched concurrently via ``asyncio.gather``.  Each call to
    ``subscribe`` is an independent registration; the same callable may be
    subscribed multiple times.

    Example::

        bus = AsyncLocalBus()

        emit_order = bus.publish(OrderPlaced)

        @bus.subscribe(config=emit_order)
        async def on_order(payload: OrderPlaced) -> None: ...

        await emit_order(order_id=42)

        bus.unsubscribe(on_order)
    """

    async def emit_publish[**P](
        self, payload: EventSchema, event: BoundEvent[P, Any, Awaitable[None]]
    ) -> None:
        """
        Open a scoped envelope and dispatch the payload to all handlers on the channel.

        Each emission runs inside its own ``EventEnvelope``, or a child of the
        currently active one, enabling correlation across nested emissions.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            event:   The event being emitted.

        """
        with scoped_envelope():
            await self.handle_subscribe(payload, config=event)

    async def handle_subscribe(
        self, payload: EventSchema, *, config: BoundEvent[Any, None, Awaitable[None]]
    ) -> None:
        """
        Invoke every handler registered on the channel concurrently.

        Sync handlers are called directly; async handlers are awaited.  Both
        are dispatched via ``asyncio.gather`` so all run concurrently.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            config:    Unused; present to satisfy the ``AsyncSubscriber`` interface.

        """

        async def _call(handler: Handler[Any, BoundEvent[Any, Any, Any], Any]) -> None:
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
