"""Direct, in-process synchronous event bus."""

from stratae.events.channel import Channel
from stratae.events.envelope import scoped_envelope
from stratae.events.event import EventSchema
from stratae.events.mixins.publish import Publisher
from stratae.events.mixins.subscribe import Subscriber


class LocalBus(Publisher[None, None], Subscriber[None]):
    """
    In-process, synchronous event bus with no routing metadata.

    Dispatches every event emitted on a channel to all handlers registered
    on that channel.  Each call to ``subscribe`` is an independent
    registration; the same callable may be subscribed multiple times.

    Example::

        bus = LocalBus()
        orders = Channel("orders")

        handle = bus.subscribe(orders, on_order)

        emit_order = bus.publish(orders, OrderPlaced)
        emit_order(order_id=42)

        bus.unsubscribe(orders, handle)
    """

    def emit_publish(self, channel: Channel, payload: EventSchema, *, meta: None) -> None:
        """
        Open a scoped envelope and dispatch the payload to all handlers on the channel.

        Each emission runs inside its own ``EventEnvelope``, or a child of the
        currently active one, enabling correlation across nested emissions.

        Args:
            channel: The channel the event was emitted on.
            payload: The constructed ``EventSchema`` instance to dispatch.
            meta:    Unused; present to satisfy the ``Publisher`` interface.

        """
        with scoped_envelope():
            self.handle_subscribe(channel, payload, meta=meta)

    def handle_subscribe(self, channel: Channel, payload: EventSchema, *, meta: None) -> None:
        """
        Invoke every handler registered on the channel with the payload.

        Args:
            channel: The channel the event arrived on.
            payload: The constructed ``EventSchema`` instance to dispatch.
            meta:    Unused; present to satisfy the ``Subscriber`` interface.

        """
        for handler in self.get_handlers(channel):
            handler(payload)
