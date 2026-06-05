"""RabbitMQ async publish adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aiormq import connect

from stratae.events.event import AsyncBoundEvent, EventSchema
from stratae.events.mixins.publish import AsyncPublisher

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel, AbstractConnection


class RabbitMQConfig:
    """Routing config for a RabbitMQ publish binding."""

    def __init__(self, exchange: str, routing_key: str) -> None:
        """
        Bind routing config for a single publish target.

        Args:
            exchange:    The RabbitMQ exchange to publish to.
            routing_key: The routing key for the message.

        """
        self.exchange = exchange
        self.routing_key = routing_key


class RabbitMQPublisher(AsyncPublisher[RabbitMQConfig, None]):
    """
    Async RabbitMQ publish adapter.

    Manages a single connection and channel for the lifetime of the context.
    Use as an async context manager::

        async with RabbitMQPublisher("amqp://guest:guest@localhost/") as publisher:
            emit_order = publisher.publish(
                            OrderPlaced, config=RabbitMQConfig("events", "order.placed")
                        )
            await emit_order(order_id=42)

    """

    def __init__(self, url: str) -> None:
        """
        Initialise the publisher with a RabbitMQ connection URL.

        Args:
            url: AMQP connection URL, e.g. ``"amqp://guest:guest@localhost/"``.

        """
        super().__init__()
        self._url = url
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None

    async def __aenter__(self) -> RabbitMQPublisher:
        """
        Open the AMQP connection and allocate a channel.

        Returns:
            ``self``, ready to publish.

        """
        self._connection = await connect(self._url)
        self._channel = await self._connection.channel()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close the AMQP connection, if one was opened."""
        if self._connection is not None:
            await self._connection.close()

    async def emit_publish[**P](
        self, payload: EventSchema, event: AsyncBoundEvent[P, RabbitMQConfig, None]
    ) -> None:
        """
        Publish a serialized event payload to RabbitMQ.

        Args:
            payload: The constructed ``EventSchema`` instance to publish.
            event:   The bound event carrying the RabbitMQ routing config.

        """
        ...
