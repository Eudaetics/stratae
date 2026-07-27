"""RabbitMQ async publish and consume adapters."""

from __future__ import annotations

import asyncio
import logging
from copy import copy
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Coroutine, Iterable, Protocol, overload

from aiormq import connect
from pamqp.commands import Basic

from stratae.events.bound import AsyncBoundEvent, AsyncFactoryBoundEvent, abind
from stratae.events.envelope import (
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    TIMESTAMP_HEADER,
    Envelope,
)
from stratae.events.event import Event, PubSub
from stratae.events.exceptions import NotConnectedError
from stratae.events.handler import Handler
from stratae.serde import Unpacker, pack, unpack_json

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel, AbstractConnection, DeliveredMessage

_log = logging.getLogger(__name__)

_NOT_CONNECTED = "publisher is not connected; open it with 'async with' before emitting"

_NO_CONSUME_TARGET = "consume config requires a queue, an exchange, or both"


def _stamp(properties: Basic.Properties | None) -> Basic.Properties:
    current = Envelope.current()
    envelope = current.child() if current is not None else Envelope()
    stamped = copy(properties) if properties is not None else Basic.Properties()
    stamped.headers = (stamped.headers or {}) | envelope.to_headers()
    stamped.message_id = str(envelope.message_id)
    stamped.correlation_id = str(envelope.correlation_id)
    stamped.timestamp = envelope.timestamp
    return stamped


def _envelope_from(message: DeliveredMessage) -> Envelope | None:
    properties = message.header.properties
    native = {
        MESSAGE_ID_HEADER: properties.message_id,
        CORRELATION_ID_HEADER: properties.correlation_id,
        TIMESTAMP_HEADER: properties.timestamp and properties.timestamp.isoformat(),
    }
    headers = native | dict(properties.headers or {})
    if (headers.get(MESSAGE_ID_HEADER) is None) != (headers.get(CORRELATION_ID_HEADER) is None):
        _log.info("partial envelope headers; minting the missing id: %r", headers)
    try:
        return Envelope.from_headers(headers)
    except ValueError:
        _log.warning("unparseable envelope headers; scoping a fresh envelope: %r", headers)
        return None


class RabbitMQConfig:
    """Routing config for a RabbitMQ publish binding."""

    __slots__ = ("exchange", "exchange_type", "properties", "routing_key")

    def __init__(
        self,
        exchange: str,
        routing_key: str,
        *,
        exchange_type: str | None = None,
        properties: Basic.Properties | None = None,
    ) -> None:
        """
        Bind routing config for a single publish target.

        Args:
            exchange:      The RabbitMQ exchange to publish to.
            routing_key:   The routing key for the message.
            exchange_type: When given, the publisher declares the exchange
                           with this type before first publishing to it.
            properties:    AMQP message properties published with every
                           message, e.g. ``Basic.Properties(delivery_mode=2)``
                           for persistent messages.

        """
        self.exchange = exchange
        self.routing_key = routing_key
        self.exchange_type = exchange_type
        self.properties = properties


class RabbitMQPublisher:
    """
    Async RabbitMQ publish adapter for pub/sub events.

    Manages a single connection and channel for the lifetime of the context.
    Payloads are serialized with the binding's serializer when one is given,
    falling back to the adapter-wide ``serializer`` (default
    ``stratae.serde.pack``).  A config carrying an ``exchange_type`` has its
    exchange declared before its first publish.

    Example::

        async with RabbitMQPublisher("amqp://guest:guest@localhost/") as publisher:
            order_placed = publisher.bind(
                Event(OrderPlaced, PubSub),
                factory=OrderPlaced,
                config=RabbitMQConfig("events", "order.placed"),
            )
            await order_placed(order_id=42)

    """

    def __init__(self, url: str, serializer: Callable[[Any], bytes] = pack) -> None:
        """
        Initialise the publisher with a RabbitMQ connection URL.

        Args:
            url:        AMQP connection URL, e.g. ``"amqp://guest:guest@localhost/"``.
            serializer: Encodes payloads to bytes for every binding that does
                        not supply its own.  Defaults to ``stratae.serde.pack``.

        """
        self._url = url
        self._serializer = serializer
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._declared: set[str] = set()

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
            self._declared.clear()

    @overload
    def bind[**P, S: Any](
        self,
        event: Event[S, PubSub],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
        config: RabbitMQConfig,
        serializer: Callable[[S], bytes] | None = None,
    ) -> AsyncFactoryBoundEvent[P, S, PubSub, RabbitMQConfig, None]: ...

    @overload
    def bind[S: Any](
        self,
        event: Event[S, PubSub],
        *,
        config: RabbitMQConfig,
        serializer: Callable[[S], bytes] | None = None,
    ) -> AsyncBoundEvent[S, PubSub, RabbitMQConfig, None]: ...

    def bind(
        self,
        event: Event[Any, PubSub],
        *,
        factory: Callable[..., Any] | None = None,
        config: RabbitMQConfig,
        serializer: Callable[[Any], bytes] | None = None,
    ) -> (
        AsyncFactoryBoundEvent[Any, Any, PubSub, RabbitMQConfig, None]
        | AsyncBoundEvent[Any, PubSub, RabbitMQConfig, None]
    ):
        """
        Return an ``AsyncBoundEvent`` or ``AsyncFactoryBoundEvent`` publishing through this adapter.

        Args:
            event:      The ``Event`` this binding publishes.
            factory:    Builds the payload from the bound call's arguments.
                        Omit it to pass an already-built payload straight
                        through instead.
            config:     The exchange and routing key to publish to.
            serializer: Encodes the payload to bytes before publishing.
                        Overrides the adapter's ``serializer`` for this
                        binding only.

        """
        return abind(self.emit, event, factory=factory, config=config, serializer=serializer)

    async def emit[S: Any](
        self,
        payload: S,
        event: Event[S, PubSub],
        config: RabbitMQConfig,
        *,
        serializer: Callable[[S], bytes] | None = None,
    ) -> None:
        """
        Serialize the payload and publish it to the configured exchange.

        Args:
            payload:    The payload instance to publish.
            event:      The ``Event`` being emitted; carried for
                        adapter-uniform emit signatures, not used for routing.
            config:     The exchange and routing key to publish to.
            serializer: Encodes the payload to bytes before publishing.
                        Overrides the adapter's ``serializer`` for this
                        call only.

        Raises:
            NotConnectedError: When the publisher's connection is not open.

        """
        if self._channel is None:
            raise NotConnectedError(_NOT_CONNECTED)
        if config.exchange_type is not None and config.exchange not in self._declared:
            await self._channel.exchange_declare(
                config.exchange, exchange_type=config.exchange_type
            )
            self._declared.add(config.exchange)
        body = serializer(payload) if serializer is not None else self._serializer(payload)
        await self._channel.basic_publish(
            body,
            exchange=config.exchange,
            routing_key=config.routing_key,
            properties=_stamp(config.properties),
        )


class RabbitMQConsumeConfig:
    """Routing config for a RabbitMQ consume binding."""

    __slots__ = (
        "arguments",
        "auto_delete",
        "binding_keys",
        "durable",
        "exchange",
        "exchange_type",
        "exclusive",
        "queue",
    )

    def __init__(
        self,
        queue: str | None = None,
        *,
        exchange: str | None = None,
        binding_key: str | Iterable[str] = "",
        exchange_type: str = "fanout",
        durable: bool | None = None,
        exclusive: bool | None = None,
        auto_delete: bool | None = None,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """
        Bind routing config for a single consume source.

        A ``queue`` alone is declared durable and consumed, competing with
        that queue's other consumers.  Supplying an ``exchange`` switches the
        registration into subscriber mode: the adapter declares the exchange
        and a queue, binds them once per ``binding_key``, and consumes the
        declared queue — server-named, exclusive, and auto-deleted when
        ``queue`` is omitted, durable when it is named.  The ``durable``,
        ``exclusive``, and ``auto_delete`` flags override those inferred
        defaults when given.

        Args:
            queue:         The RabbitMQ queue to consume from.  When omitted,
                           a private server-named queue is declared instead.
            exchange:      The exchange to bind the queue to; enables
                           subscriber mode.
            binding_key:   The routing key pattern for the binding — one, or
                           an iterable to bind the queue several times.
            exchange_type: The type the exchange is declared with.
            durable:       Overrides the inferred queue durability.
            exclusive:     Overrides the inferred queue exclusivity.
            auto_delete:   Overrides the inferred queue auto-delete flag.
            arguments:     Optional arguments for the queue declaration,
                           e.g. message TTL or a dead-letter exchange.

        Raises:
            ValueError: When neither ``queue`` nor ``exchange`` is given.

        """
        if queue is None and exchange is None:
            raise ValueError(_NO_CONSUME_TARGET)
        self.queue = queue
        self.exchange = exchange
        self.binding_keys: tuple[str, ...] = (
            (binding_key,) if isinstance(binding_key, str) else tuple(binding_key)
        )
        self.exchange_type = exchange_type
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments


class _Registration:
    """Pairs a registered handler with its deserializer and live consumer tag."""

    __slots__ = ("consumer_tag", "deserializer", "event", "handler")

    def __init__(
        self,
        event: Event[Any, PubSub],
        handler: Handler[[Any], RabbitMQConsumeConfig, Any],
        deserializer: Unpacker | None,
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
    comes from topology: a config with an ``exchange`` declares and binds
    its own queue — a private server-named one when unnamed, a durable one
    when named — so each such registration sees every message the exchange
    routes to it.

    Message bodies are decoded with the consumer's ``deserializer`` — an
    ``Unpacker`` called as ``deserializer(body, type=schema)`` with each
    event's ``schema``.  The default, ``stratae.serde.unpack_json``,
    decodes JSON and constructs the type from keyword arguments; pass a
    compatible decoder such as ``msgspec.json.decode`` to swap the format
    adapter-wide, and a per-registration ``deserializer`` overrides it for
    one queue.  Sync and async handlers are both
    supported.  A message is acked after its handler returns.  When
    deserialization or handling raises, the message is nacked without
    requeue and the exception is logged — this keeps poison messages from
    redelivering forever and shields the channel from handler errors, which
    would otherwise escape into aiormq's channel machinery and close it.

    Example::

        order_placed = Event(OrderPlaced, PubSub)
        consumer = RabbitMQConsumer("amqp://guest:guest@localhost/")

        @consumer.handle(order_placed, config=RabbitMQConsumeConfig("orders"))
        def on_order(payload: OrderPlaced) -> None: ...

        async with consumer:
            ...  # consumes in the background until the context exits

    """

    def __init__(
        self,
        url: str,
        deserializer: Unpacker = unpack_json,
        prefetch_count: int | None = None,
    ) -> None:
        """
        Initialise the consumer with a RabbitMQ connection URL.

        Args:
            url:            AMQP connection URL, e.g. ``"amqp://guest:guest@localhost/"``.
            deserializer:   Decodes message bodies into each event's
                            ``schema``, called as
                            ``deserializer(body, type=schema)``.
                            Defaults to ``stratae.serde.unpack_json``.
            prefetch_count: Channel QoS: the number of unacked messages the
                            broker delivers ahead.  ``1`` gives fair dispatch
                            between competing consumers; ``None`` sets no
                            QoS, matching the broker's unlimited default.

        """
        self._url = url
        self._deserializer = deserializer
        self._prefetch_count = prefetch_count
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
        if self._prefetch_count is not None:
            await channel.basic_qos(prefetch_count=self._prefetch_count)
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
    def handle[S: Any, R](
        self,
        event: Event[S, PubSub],
        fn: Callable[[S], R],
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Unpacker | None = None,
    ) -> Handler[[S], RabbitMQConsumeConfig, R]: ...

    @overload
    def handle[S: Any](
        self,
        event: Event[S, PubSub],
        fn: None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Unpacker | None = None,
    ) -> _ConsumeDecorator[S]: ...

    def handle(
        self,
        event: Event[Any, PubSub],
        fn: Callable[[Any], Any] | None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Unpacker | None = None,
    ) -> Handler[[Any], RabbitMQConsumeConfig, Any] | _ConsumeDecorator[Any]:
        """
        Register a handler consuming a queue for an event, as a decorator or direct call.

        When the consumer is already connected, consumption starts
        immediately; otherwise it starts when the context is entered.
        Returns the ``Handler`` instance in both forms so callers can pass
        it to ``remove`` later.

        Args:
            event:        The ``Event`` whose schema the handler receives.
            fn:           When supplied, registers ``fn`` directly and returns its
                          ``Handler``.  When omitted, returns a decorator that
                          registers and returns the ``Handler``.
            config:       The queue to consume from.
            deserializer: Decodes a message body into the event's payload.
                          Overrides the consumer's ``deserializer`` for this
                          registration only.

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
        event: Event[Any, PubSub],
        fn: Callable[[Any], Any],
        config: RabbitMQConsumeConfig,
        deserializer: Unpacker | None,
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
        queue = await self._declare(channel, registration.handler.config)
        ok = await channel.basic_consume(queue, self._on_message(registration))
        registration.consumer_tag = ok.consumer_tag
        if registration.handler not in self._registrations and ok.consumer_tag is not None:
            registration.consumer_tag = None
            await channel.basic_cancel(ok.consumer_tag)

    def _queue_declare_setup(self, channel: AbstractChannel, config: RabbitMQConsumeConfig):
        named = config.queue is not None
        return channel.queue_declare(
            config.queue or "",
            durable=named if config.durable is None else config.durable,
            exclusive=(not named) if config.exclusive is None else config.exclusive,
            auto_delete=(not named) if config.auto_delete is None else config.auto_delete,
            arguments=config.arguments,
        )

    async def _declare(self, channel: AbstractChannel, config: RabbitMQConsumeConfig) -> str:
        declared = await self._queue_declare_setup(channel, config)
        queue = config.queue or declared.queue or ""
        if config.exchange is None:
            return queue
        await channel.exchange_declare(config.exchange, exchange_type=config.exchange_type)
        for binding_key in config.binding_keys:
            await channel.queue_bind(queue, config.exchange, routing_key=binding_key)
        return queue

    def _discard_start(self, task: asyncio.Task[None]) -> None:
        self._starting.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            _log.error("failed to start AMQP consumer", exc_info=exc)

    def _deserialize(self, registration: _Registration, body: bytes) -> Any:
        unpacker = registration.deserializer or self._deserializer
        return unpacker(body, type=registration.event.schema)

    def _on_message(
        self, registration: _Registration
    ) -> Callable[[DeliveredMessage], Coroutine[Any, Any, None]]:
        async def on_message(message: DeliveredMessage) -> None:
            try:
                payload = self._deserialize(registration, message.body)
                with Envelope.scope(_envelope_from(message)):
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
