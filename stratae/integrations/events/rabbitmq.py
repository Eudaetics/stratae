"""RabbitMQ async publish adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from aiormq import connect

from stratae.events.bound import AsyncBoundEvent, abind
from stratae.events.event import EventConfig, PubSub
from stratae.events.exceptions import NotConnectedError
from stratae.serde import pack

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel, AbstractConnection

_NOT_CONNECTED = "publisher is not connected; open it with 'async with' before emitting"


class RabbitMQConfig:
    """Routing config for a RabbitMQ publish binding."""

    __slots__ = ("exchange", "routing_key")

    def __init__(self, exchange: str, routing_key: str) -> None:
        """
        Bind routing config for a single publish target.

        Args:
            exchange:    The RabbitMQ exchange to publish to.
            routing_key: The routing key for the message.

        """
        self.exchange = exchange
        self.routing_key = routing_key


class RabbitMQPublisher:
    """
    Async RabbitMQ publish adapter for pub/sub events.

    Manages a single connection and channel for the lifetime of the context.
    Payloads are serialized with the binding's serializer when one is given,
    falling back to ``stratae.serde.pack``.

    Example::

        async with RabbitMQPublisher("amqp://guest:guest@localhost/") as publisher:
            order_placed = publisher.bind(
                event(PubSub)(OrderPlaced),
                config=RabbitMQConfig("events", "order.placed"),
            )
            await order_placed(order_id=42)

    """

    def __init__(self, url: str) -> None:
        """
        Initialise the publisher with a RabbitMQ connection URL.

        Args:
            url: AMQP connection URL, e.g. ``"amqp://guest:guest@localhost/"``.

        """
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
            self._connection = None
            self._channel = None

    def bind[**P, S: Any](
        self,
        event: EventConfig[P, S, PubSub],
        *,
        config: RabbitMQConfig,
        serializer: Callable[[S], bytes] | None = None,
    ) -> AsyncBoundEvent[P, S, PubSub, RabbitMQConfig, None]:
        """
        Return an ``AsyncBoundEvent`` publishing through this adapter.

        Args:
            event:      The ``EventConfig`` whose factory constructs the payload.
            config:     The exchange and routing key to publish to.
            serializer: Encodes the payload to bytes before publishing.
                        Defaults to ``stratae.serde.pack``.

        """
        return abind(self.emit, event, config=config, serializer=serializer)

    async def emit[**P, S: Any](
        self,
        payload: S,
        event: EventConfig[P, S, PubSub],
        config: RabbitMQConfig,
        *,
        serializer: Callable[[S], bytes] | None = None,
    ) -> None:
        """
        Serialize the payload and publish it to the configured exchange.

        Args:
            payload:    The constructed payload instance to publish.
            event:      The ``EventConfig`` being emitted; carried for
                        adapter-uniform emit signatures, not used for routing.
            config:     The exchange and routing key to publish to.
            serializer: Encodes the payload to bytes before publishing.
                        Defaults to ``stratae.serde.pack``.

        Raises:
            NotConnectedError: When the publisher's connection is not open.

        """
        if self._channel is None:
            raise NotConnectedError(_NOT_CONNECTED)
        body = serializer(payload) if serializer is not None else pack(payload)
        await self._channel.basic_publish(
            body, exchange=config.exchange, routing_key=config.routing_key
        )
