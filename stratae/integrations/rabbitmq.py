"""
RabbitMQ async publish and consume adapters.

{py:class}`RabbitMQPublisher` and {py:class}`RabbitMQConsumer` are async
adapters implementing the {py:class}`Producer <stratae.events.protocols.Producer>`
and {py:class}`Consumer <stratae.events.protocols.Consumer>` protocols over
`aiormq`. Bind an {py:class}`EventConfig <stratae.events.event.EventConfig>`
to a publisher with {py:func}`RabbitMQPublisher.bind`, using a
{py:class}`RabbitMQConfig` to name the exchange and routing key. Register
handlers on a consumer with {py:func}`RabbitMQConsumer.handle`, using a
{py:class}`RabbitMQConsumeConfig` to name the queue or exchange to consume
from. Both adapters open their connection in `async with` and raise
{py:exc}`NotConnectedError <stratae.events.exceptions.NotConnectedError>`
when used before that.

Every publish carries an {py:class}`Envelope <stratae.events.envelope.Envelope>`
in its message headers, a child of whichever envelope is active when it's
sent. Every delivered message reopens that envelope as the active one for
its handler. Correlation ids propagate across a chain of publishes and
handlers this way, without extra plumbing.

```{rubric} Example:
```
<!--- skip: next -->
```{code-block} python
:caption: Publishing a pub/sub event to a RabbitMQ exchange

import asyncio
from stratae.events import PubSub, event
from stratae.integrations.rabbitmq import RabbitMQConfig, RabbitMQPublisher

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

    def to_dict(self) -> dict[str, int]:
        return {"order_id": self.order_id}

order_placed = event(OrderPlaced, PubSub)

async def main() -> None:
    async with RabbitMQPublisher("amqp://guest:guest@localhost/") as publisher:
        place_order = publisher.bind(
            order_placed, config=RabbitMQConfig("events", "order.placed")
        )
        await place_order(order_id=42)

asyncio.run(main())
```

See {py:class}`RabbitMQPublisher` and {py:class}`RabbitMQConsumer` for
additional examples.

"""

from __future__ import annotations

import asyncio
import logging
from copy import copy
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Iterable, Protocol, overload

from aiormq import connect
from pamqp.commands import Basic

from stratae.events import (
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    TIMESTAMP_HEADER,
    AsyncBoundEvent,
    Envelope,
    EventConfig,
    Handler,
    PubSub,
    abind,
)
from stratae.events.exceptions import NotConnectedError
from stratae.serde import Unpacker, pack, unpack_json

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel, AbstractConnection, DeliveredMessage

_log = logging.getLogger(__name__)

_NOT_CONNECTED = "publisher is not connected; open it with 'async with' before emitting"

_NO_CONSUME_TARGET = "consume config requires a queue, an exchange, or both"


def _stamp(properties: Basic.Properties | None) -> Basic.Properties:
    """Copy properties, stamping it with the current envelope's headers, ids, and timestamp."""
    current = Envelope.current()
    envelope = current.child() if current is not None else Envelope()
    stamped = copy(properties) if properties is not None else Basic.Properties()
    stamped.headers = (stamped.headers or {}) | envelope.to_headers()
    stamped.message_id = str(envelope.message_id)
    stamped.correlation_id = str(envelope.correlation_id)
    stamped.timestamp = envelope.timestamp
    return stamped


def _envelope_from(message: DeliveredMessage) -> Envelope | None:
    """Reconstruct the Envelope carried by a delivered message's properties and headers."""
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
    """
    Routing config for a RabbitMQ publish binding.

    Pairs the exchange and routing key a {py:class}`RabbitMQPublisher`
    binding publishes to. Supplying `exchange_type` declares the exchange
    before the binding's first publish; leave it unset when the exchange is
    already declared elsewhere.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Declaring routing config for a persistent, topic-routed binding

    from pamqp.commands import Basic
    from stratae.integrations.rabbitmq import RabbitMQConfig

    config = RabbitMQConfig(
        "orders",
        "order.placed",
        exchange_type="topic",
        properties=Basic.Properties(delivery_mode=2),
    )

    assert config.exchange == "orders"
    assert config.routing_key == "order.placed"
    assert config.exchange_type == "topic"
    assert config.properties.delivery_mode == 2
    ```

    """

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

        :param exchange: The RabbitMQ exchange to publish to.
        :param routing_key: The routing key for the message.
        :param exchange_type: When given, the publisher declares the exchange
            with this type before its first publish to it.
        :param properties: AMQP message properties published with every
            message, e.g. `Basic.Properties(delivery_mode=2)` for persistent
            messages.

        """
        self.exchange = exchange
        self.routing_key = routing_key
        self.exchange_type = exchange_type
        self.properties = properties


class RabbitMQPublisher:
    """
    Async RabbitMQ publish adapter for pub/sub events.

    Manages a single connection and channel for the lifetime of the `async
    with` block. Payloads are serialized with the binding's `serializer`
    when one is given, falling back to the adapter-wide `serializer`
    ({py:func}`stratae.serde.pack` by default). A config carrying an
    `exchange_type` has its exchange declared before its first publish.
    Bind an {py:class}`EventConfig <stratae.events.event.EventConfig>` to
    this adapter with {py:func}`RabbitMQPublisher.bind`, which returns an
    {py:class}`AsyncBoundEvent <stratae.events.bound.AsyncBoundEvent>`.

    ```{rubric} Example:
    ```
    <!--- skip: next -->
    ```{code-block} python
    :caption: Binding and publishing a pub/sub event to a declared exchange

    import asyncio
    from stratae.events import PubSub, event
    from stratae.integrations.rabbitmq import RabbitMQConfig, RabbitMQPublisher

    class OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

        def to_dict(self) -> dict[str, int]:
            return {"order_id": self.order_id}

    order_placed = event(OrderPlaced, PubSub)

    async def main() -> None:
        async with RabbitMQPublisher("amqp://guest:guest@localhost/") as publisher:
            place_order = publisher.bind(
                order_placed,
                config=RabbitMQConfig("events", "order.placed", exchange_type="topic"),
            )
            await place_order(order_id=42)

    asyncio.run(main())
    ```

    """

    def __init__(self, url: str, serializer: Callable[[Any], bytes] = pack) -> None:
        """
        Initialise the publisher with a RabbitMQ connection URL.

        :param url: AMQP connection URL, e.g. `"amqp://guest:guest@localhost/"`.
        :param serializer: Encodes payloads to bytes for every binding that
            does not supply its own. Defaults to {py:func}`stratae.serde.pack`.

        """
        self._url = url
        self._serializer = serializer
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._declared: set[str] = set()

    async def __aenter__(self) -> RabbitMQPublisher:
        """
        Open the AMQP connection and allocate a channel.

        :returns: `self`, ready to publish.

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

    def bind[**P, S: Any](
        self,
        event: EventConfig[P, S, PubSub],
        *,
        config: RabbitMQConfig,
        serializer: Callable[[S], bytes] | None = None,
    ) -> AsyncBoundEvent[P, S, PubSub, RabbitMQConfig, None]:
        """
        Return an AsyncBoundEvent publishing through this adapter.

        :param event: The {py:class}`EventConfig <stratae.events.event.EventConfig>`
            whose factory constructs the payload.
        :param config: The exchange and routing key to publish to.
        :param serializer: Encodes the payload to bytes before publishing.
            Overrides the adapter's `serializer` for this binding only.
        :returns: An {py:class}`AsyncBoundEvent <stratae.events.bound.AsyncBoundEvent>`
            wrapping this adapter's `emit` and `event`.

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

        :param payload: The constructed payload instance to publish.
        :param event: The {py:class}`EventConfig <stratae.events.event.EventConfig>`
            being emitted; carried for adapter-uniform emit signatures, not
            used for routing.
        :param config: The exchange and routing key to publish to.
        :param serializer: Encodes the payload to bytes before publishing.
            Overrides the adapter's `serializer` for this call only.
        :raises NotConnectedError: When the publisher's connection is not open.

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
    """
    Routing config for a RabbitMQ consume binding.

    A `queue` alone is declared durable and consumed. It competes with that
    queue's other consumers. Supplying an `exchange` switches the
    registration into subscriber mode instead: the adapter declares the
    exchange and a queue, binds them once per binding key, then consumes the
    declared queue. That queue is server-named, exclusive, and auto-deleted
    when `queue` is omitted, or durable when `queue` is named. `durable`,
    `exclusive`, and `auto_delete` override those inferred defaults when
    given.

    ```{rubric} Examples:
    ```
    ```{code-block} python
    :caption: Declaring subscriber-mode config bound to a topic exchange

    from stratae.integrations.rabbitmq import RabbitMQConsumeConfig

    config = RabbitMQConsumeConfig(exchange="events", binding_key="order.*", exchange_type="topic")

    assert config.queue is None
    assert config.exchange == "events"
    assert config.binding_keys == ("order.*",)
    ```

    ```{code-block} python
    :caption: Omitting both a queue and an exchange is rejected

    import pytest
    from stratae.integrations.rabbitmq import RabbitMQConsumeConfig

    with pytest.raises(ValueError, match="queue"):
        RabbitMQConsumeConfig()
    ```

    """

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

        :param queue: The RabbitMQ queue to consume from. When omitted, a
            private server-named queue is declared instead.
        :param exchange: The exchange to bind the queue to; enables
            subscriber mode.
        :param binding_key: The routing key pattern for the binding.
        :param exchange_type: The type the exchange is declared with.
        :param durable: Overrides the inferred queue durability.
        :param exclusive: Overrides the inferred queue exclusivity.
        :param auto_delete: Overrides the inferred queue auto-delete flag.
        :param arguments: Optional arguments for the queue declaration,
            e.g. message TTL or a dead-letter exchange.
        :raises ValueError: When neither `queue` nor `exchange` is given.

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
        event: EventConfig[Any, Any, PubSub],
        handler: Handler[[Any], RabbitMQConsumeConfig, Any],
        deserializer: Unpacker | None,
    ) -> None:
        self.event = event
        self.handler = handler
        self.deserializer = deserializer
        self.consumer_tag: str | None = None


class _ConsumeDecorator[S: Any](Protocol):
    """Decorator form of `handle`: registers and returns the resulting Handler."""

    def __call__[R](self, fn: Callable[[S], R]) -> Handler[[S], RabbitMQConsumeConfig, R]: ...


class RabbitMQConsumer:
    """
    Async RabbitMQ consume adapter for pub/sub events.

    Register handlers with `handle` at any time. Registrations made before
    entering the context start consuming when the connection opens.
    Registrations made while connected start consuming immediately. Each
    registration is its own AMQP consumer, so two handlers on the same
    queue compete for messages round-robin rather than fanning out. Fan-out
    comes from topology instead: a config with an `exchange` declares and
    binds its own queue, a private server-named one when unnamed or a
    durable one when named, so each such registration sees every message
    the exchange routes to it.

    Message bodies are decoded with the consumer's `deserializer`, an
    {py:class}`Unpacker <stratae.serde.Unpacker>` called as
    `deserializer(body, type=payload_type)` with each event's `payload_type`.
    The default, {py:func}`stratae.serde.unpack_json`, decodes JSON and
    constructs the type from keyword arguments. Pass a compatible decoder
    such as `msgspec.json.decode` to swap the format adapter-wide. A
    per-registration `deserializer` overrides it for one queue. Sync and
    async handlers are both supported. A message is acked after its handler
    returns. When deserialization or handling raises, the message is nacked
    without requeue and the exception is logged. This keeps poison messages
    from redelivering forever, and it shields the channel from handler
    errors that would otherwise escape into aiormq's channel machinery and
    close it.

    ```{rubric} Example:
    ```
    <!--- skip: next -->
    ```{code-block} python
    :caption: Registering a handler and consuming a queue

    import asyncio
    from stratae.events import PubSub, event
    from stratae.integrations.rabbitmq import RabbitMQConsumeConfig, RabbitMQConsumer

    class OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    order_placed = event(OrderPlaced, PubSub)
    consumer = RabbitMQConsumer("amqp://guest:guest@localhost/")

    @consumer.handle(order_placed, config=RabbitMQConsumeConfig("orders"))
    def on_order(payload: OrderPlaced) -> None:
        print(f"order {payload.order_id} placed")

    async def main() -> None:
        async with consumer:
            ...  # consumes in the background until the context exits

    asyncio.run(main())
    ```

    """

    def __init__(
        self,
        url: str,
        deserializer: Unpacker = unpack_json,
        prefetch_count: int | None = None,
    ) -> None:
        """
        Initialise the consumer with a RabbitMQ connection URL.

        :param url: AMQP connection URL, e.g. `"amqp://guest:guest@localhost/"`.
        :param deserializer: Decodes message bodies into each event's
            `payload_type`, called as `deserializer(body, type=payload_type)`.
            Defaults to {py:func}`stratae.serde.unpack_json`.
        :param prefetch_count: Channel QoS: the number of unacked messages
            the broker delivers ahead. `1` gives fair dispatch between
            competing consumers; `None` sets no QoS, matching the broker's
            unlimited default.

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

        :returns: `self`, consuming in the background.

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
    def handle[**P, S: Any, R](
        self,
        event: EventConfig[P, S, PubSub],
        fn: Callable[[S], R],
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Unpacker | None = None,
    ) -> Handler[[S], RabbitMQConsumeConfig, R]: ...

    @overload
    def handle[**P, S: Any](
        self,
        event: EventConfig[P, S, PubSub],
        fn: None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Unpacker | None = None,
    ) -> _ConsumeDecorator[S]: ...

    def handle(
        self,
        event: EventConfig[Any, Any, PubSub],
        fn: Callable[[Any], Any] | None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Unpacker | None = None,
    ) -> Handler[[Any], RabbitMQConsumeConfig, Any] | _ConsumeDecorator[Any]:
        """
        Register a handler consuming a queue for an event, as a decorator or direct call.

        When the consumer is already connected, consumption starts
        immediately. Otherwise it starts when the context is entered.
        Returns the {py:class}`Handler <stratae.events.handler.Handler>`
        instance in both forms so callers can pass it to `remove` later.

        :param event: The {py:class}`EventConfig <stratae.events.event.EventConfig>`
            whose payload type the handler receives.
        :param fn: When supplied, registers `fn` directly and returns its
            `Handler`. When omitted, returns a decorator that registers and
            returns the `Handler`.
        :param config: The queue to consume from.
        :param deserializer: Decodes a message body into the event's payload.
            Overrides the consumer's `deserializer` for this registration
            only.

        """
        if fn is not None:
            return self._register(event, fn, config, deserializer)

        def decorator(fn: Callable[[Any], Any]) -> Handler[[Any], RabbitMQConsumeConfig, Any]:
            return self._register(event, fn, config, deserializer)

        return decorator

    async def remove(self, handler: Handler[[Any], RabbitMQConsumeConfig, Any]) -> None:
        """
        Cancel and remove a previously registered handler.

        Cancels the live AMQP consumer when one is running. Safe to call
        whether or not the consumer is connected.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>`
            instance returned by `handle`.

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
        deserializer: Unpacker | None,
    ) -> Handler[[Any], RabbitMQConsumeConfig, Any]:
        """Wrap fn as a Handler and start consuming immediately if the connection is open."""
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
        """Declare the registration's queue, start consuming, and cancel it if removed mid-start."""
        queue = await self._declare(channel, registration.handler.config)
        ok = await channel.basic_consume(queue, self._on_message(registration))
        registration.consumer_tag = ok.consumer_tag
        if registration.handler not in self._registrations and ok.consumer_tag is not None:
            registration.consumer_tag = None
            await channel.basic_cancel(ok.consumer_tag)

    def _queue_declare_setup(self, channel: AbstractChannel, config: RabbitMQConsumeConfig):
        """Return the queue_declare call for config, inferring flags from whether it's named."""
        named = config.queue is not None
        return channel.queue_declare(
            config.queue or "",
            durable=named if config.durable is None else config.durable,
            exclusive=(not named) if config.exclusive is None else config.exclusive,
            auto_delete=(not named) if config.auto_delete is None else config.auto_delete,
            arguments=config.arguments,
        )

    async def _declare(self, channel: AbstractChannel, config: RabbitMQConsumeConfig) -> str:
        """Declare config's queue and, if it carries an exchange, declare and bind it too."""
        declared = await self._queue_declare_setup(channel, config)
        queue = config.queue or declared.queue or ""
        if config.exchange is None:
            return queue
        await channel.exchange_declare(config.exchange, exchange_type=config.exchange_type)
        for binding_key in config.binding_keys:
            await channel.queue_bind(queue, config.exchange, routing_key=binding_key)
        return queue

    def _discard_start(self, task: asyncio.Task[None]) -> None:
        """Drop a finished start task from the pending set, logging any non-cancellation failure."""
        self._starting.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            _log.error("failed to start AMQP consumer", exc_info=exc)

    def _deserialize(self, registration: _Registration, body: bytes) -> Any:
        """Decode a message body with the registration's deserializer, or the consumer's default."""
        unpacker = registration.deserializer or self._deserializer
        return unpacker(body, type=registration.event.payload_type)

    def _on_message(
        self, registration: _Registration
    ) -> Callable[[DeliveredMessage], Coroutine[Any, Any, None]]:
        """Return the delivery callback that decodes, dispatches, and acks or nacks a message."""

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
