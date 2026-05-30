"""Direct, in-process asynchronous event bus."""

import asyncio
from typing import Any

from stratae.events.channel import Channel
from stratae.events.envelope import scoped_envelope
from stratae.events.event import EventSchema
from stratae.events.handler import Handler
from stratae.events.mixins.publish import AsyncPublisher
from stratae.events.mixins.subscribe import AsyncSubscriber


class AsyncLocalBus(AsyncPublisher[None, None], AsyncSubscriber[None]):
    """
    In-process, asynchronous event bus with no routing metadata.

    Dispatches every event emitted on a channel to all handlers registered
    on that channel.  Sync and async handlers are both supported; all are
    dispatched concurrently via ``asyncio.gather``.  Each call to
    ``subscribe`` is an independent registration; the same callable may be
    subscribed multiple times.

    Example::

        bus = AsyncLocalBus()
        orders = Channel("orders")

        handle = bus.subscribe(orders, on_order)

        emit_order = bus.publish(orders, OrderPlaced)
        await emit_order(order_id=42)

        bus.unsubscribe(orders, handle)
    """

    async def emit_publish(
        self, channel: Channel, payload: EventSchema, *, meta: None = None
    ) -> None:
        """
        Open a scoped envelope and dispatch the payload to all handlers on the channel.

        Each emission runs inside its own ``EventEnvelope``, or a child of the
        currently active one, enabling correlation across nested emissions.

        Args:
            channel: The channel the event was emitted on.
            payload: The constructed ``EventSchema`` instance to dispatch.
            meta:    Unused; present to satisfy the ``AsyncPublisher`` interface.

        """
        with scoped_envelope():
            await self.handle_subscribe(channel, payload, meta=meta)

    async def handle_subscribe(
        self, channel: Channel, payload: EventSchema, *, meta: None = None
    ) -> None:
        """
        Invoke every handler registered on the channel concurrently.

        Sync handlers are called directly; async handlers are awaited.  Both
        are dispatched via ``asyncio.gather`` so all run concurrently.

        Args:
            channel: The channel the event arrived on.
            payload: The constructed ``EventSchema`` instance to dispatch.
            meta:    Unused; present to satisfy the ``AsyncSubscriber`` interface.

        """

        async def _call(handler: Handler[Any, None, Any]) -> None:
            if handler.is_async:
                await handler(payload)
            else:
                handler(payload)

        results = await asyncio.gather(
            *(_call(h) for h in self.get_handlers(channel)), return_exceptions=True
        )
        exceptions = [r for r in results if isinstance(r, Exception)]
        if exceptions:
            raise ExceptionGroup("handler errors", exceptions)
