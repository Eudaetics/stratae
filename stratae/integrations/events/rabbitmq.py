"""RabbitMQ async publish and consume adapters."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Protocol, overload

from aiormq import connect

from stratae.events.bound import AsyncBoundEvent, abind
from stratae.events.event import EventConfig, PubSub
from stratae.events.exceptions import NotConnectedError
from stratae.events.handler import Handler
from stratae.serde import Unpacker, pack, unpack

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel, AbstractConnection, DeliveredMessage

_log = logging.getLogger(__name__)

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


class RabbitMQConsumeConfig:
    """Routing config for a RabbitMQ consume binding."""

    __slots__ = ("queue",)

    def __init__(self, queue: str) -> None:
        """
        Bind routing config for a single consume source.

        Args:
            queue: The RabbitMQ queue to consume from.

        """
        self.queue = queue


class _Registration:
    """Pairs a registered handler with its deserializer and live consumer tag."""

    __slots__ = ("consumer_tag", "deserializer", "event", "handler")

    def __init__(
        self,
        event: EventConfig[Any, Any, PubSub],
        handler: Handler[[Any], RabbitMQConsumeConfig, Any],
        deserializer: Callable[[bytes], Any] | None,
    ) -> None:
        self.event = event
        self.handler = handler
        self.deserializer = deserializer
        self.consumer_tag: str | None = None


class _ConsumeDecorator[S: Any](Protocol):
    """Decorator form of ``RabbitMQConsumer.handle``: registers and returns the ``Handler``."""

    def __call__[R](self, fn: Callable[[S], R]) -> Handler[[S], RabbitMQConsumeConfig, R]: ...


class RabbitMQConsumer:
    """
    Async RabbitMQ consume adapter for pub/sub events.

    Register handlers with ``handle`` at any time: registrations made before
    entering the context start consuming when the connection opens, and
    registrations made while connected start consuming immediately.  Each
    registration is its own AMQP consumer: two handlers on the same queue
    compete for messages round-robin rather than fanning out.  Fan-out
    topology belongs to the broker — one queue per consumer, each bound to
    the same exchange.

    Message bodies are decoded with the consumer's type-directed
    ``deserializer`` (default ``stratae.serde.unpack``) against each event's
    ``payload_type``, so plain classes, dataclasses, msgspec structs, and
    the like each take their registered decode path; a per-registration
    ``deserializer`` overrides it.  Sync and async handlers are both
    supported.  A message is acked after its handler returns.  When
    deserialization or handling raises, the message is nacked without
    requeue and the exception is logged — this keeps poison messages from
    redelivering forever and shields the channel from handler errors, which
    would otherwise escape into aiormq's channel machinery and close it.

    Example::

        order_placed = EventConfig(OrderPlaced, PubSub)
        consumer = RabbitMQConsumer("amqp://guest:guest@localhost/")

        @consumer.handle(order_placed, config=RabbitMQConsumeConfig("orders"))
        def on_order(payload: OrderPlaced) -> None: ...

        async with consumer:
            ...  # consumes in the background until the context exits

    """

    def __init__(self, url: str, deserializer: Unpacker = unpack) -> None:
        """
        Initialise the consumer with a RabbitMQ connection URL.

        Args:
            url:          AMQP connection URL, e.g. ``"amqp://guest:guest@localhost/"``.
            deserializer: Type-directed deserializer decoding message bodies
                          into each event's ``payload_type``.  Defaults to
                          ``stratae.serde.unpack``.

        """
        self._url = url
        self._deserializer = deserializer
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._registrations: dict[Handler[[Any], RabbitMQConsumeConfig, Any], _Registration] = {}
        self._starting: set[asyncio.Task[None]] = set()

    async def __aenter__(self) -> RabbitMQConsumer:
        """
        Open the AMQP connection and start consuming for every registration.

        Returns:
            ``self``, consuming in the background.

        """
        self._connection = await connect(self._url)
        channel = await self._connection.channel()
        self._channel = channel
        for registration in self._registrations.values():
            await self._start(channel, registration)
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close the AMQP connection, if one was opened."""
        for task in tuple(self._starting):
            task.cancel()
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None
        for registration in self._registrations.values():
            registration.consumer_tag = None

    @overload
    def handle[**P, S: Any, R](
        self,
        event: EventConfig[P, S, PubSub],
        fn: Callable[[S], R],
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Callable[[bytes], S] | None = None,
    ) -> Handler[[S], RabbitMQConsumeConfig, R]: ...

    @overload
    def handle[**P, S: Any](
        self,
        event: EventConfig[P, S, PubSub],
        fn: None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Callable[[bytes], S] | None = None,
    ) -> _ConsumeDecorator[S]: ...

    def handle(
        self,
        event: EventConfig[Any, Any, PubSub],
        fn: Callable[[Any], Any] | None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Callable[[bytes], Any] | None = None,
    ) -> Handler[[Any], RabbitMQConsumeConfig, Any] | _ConsumeDecorator[Any]:
        """
        Register a handler consuming a queue for an event, as a decorator or direct call.

        When the consumer is already connected, consumption starts
        immediately; otherwise it starts when the context is entered.
        Returns the ``Handler`` instance in both forms so callers can pass
        it to ``remove`` later.

        Args:
            event:        The ``EventConfig`` whose payload type the handler receives.
            fn:           When supplied, registers ``fn`` directly and returns its
                          ``Handler``.  When omitted, returns a decorator that
                          registers and returns the ``Handler``.
            config:       The queue to consume from.
            deserializer: Decodes a message body into the event's payload.
                          Overrides the consumer's type-directed deserializer
                          for this registration only.

        """
        if fn is not None:
            return self._register(event, fn, config, deserializer)

        def decorator(fn: Callable[[Any], Any]) -> Handler[[Any], RabbitMQConsumeConfig, Any]:
            return self._register(event, fn, config, deserializer)

        return decorator

    async def remove(self, handler: Handler[[Any], RabbitMQConsumeConfig, Any]) -> None:
        """
        Cancel and remove a previously registered handler.

        Cancels the live AMQP consumer when one is running; safe to call
        whether or not the consumer is connected.

        Args:
            handler: The ``Handler`` instance returned by ``handle``.

        """
        registration = self._registrations.pop(handler, None)
        if registration is None:
            return
        if registration.consumer_tag is not None and self._channel is not None:
            await self._channel.basic_cancel(registration.consumer_tag)
            registration.consumer_tag = None

    def _register(
        self,
        event: EventConfig[Any, Any, PubSub],
        fn: Callable[[Any], Any],
        config: RabbitMQConsumeConfig,
        deserializer: Callable[[bytes], Any] | None,
    ) -> Handler[[Any], RabbitMQConsumeConfig, Any]:
        handler: Handler[[Any], RabbitMQConsumeConfig, Any] = Handler(fn, config)
        registration = _Registration(event, handler, deserializer)
        self._registrations[handler] = registration
        channel = self._channel
        if channel is not None:
            task = asyncio.get_running_loop().create_task(self._start(channel, registration))
            self._starting.add(task)
            task.add_done_callback(self._discard_start)
        return handler

    async def _start(self, channel: AbstractChannel, registration: _Registration) -> None:
        ok = await channel.basic_consume(
            registration.handler.config.queue, self._on_message(registration)
        )
        registration.consumer_tag = ok.consumer_tag
        if registration.handler not in self._registrations and ok.consumer_tag is not None:
            registration.consumer_tag = None
            await channel.basic_cancel(ok.consumer_tag)

    def _discard_start(self, task: asyncio.Task[None]) -> None:
        self._starting.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            _log.error("failed to start AMQP consumer", exc_info=exc)

    def _deserialize(self, registration: _Registration, body: bytes) -> Any:
        if registration.deserializer is not None:
            return registration.deserializer(body)
        return self._deserializer(registration.event.payload_type, body)

    def _on_message(
        self, registration: _Registration
    ) -> Callable[[DeliveredMessage], Coroutine[Any, Any, None]]:
        async def on_message(message: DeliveredMessage) -> None:
            try:
                payload = self._deserialize(registration, message.body)
                if registration.handler.is_async:
                    await registration.handler(payload)
                else:
                    registration.handler(payload)
            except Exception:
                _log.exception(
                    "handler for event '%s' failed; message nacked without requeue",
                    registration.event.name,
                )
                if message.delivery_tag is not None:
                    await message.channel.basic_nack(message.delivery_tag, requeue=False)
                return
            if message.delivery_tag is not None:
                await message.channel.basic_ack(message.delivery_tag)

        return on_message
