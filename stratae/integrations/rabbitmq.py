"""
RabbitMQ async publish and consume adapters.

{py:class}`RabbitMQPublisher` and {py:class}`RabbitMQConsumer` implement the
{py:class}`Producer <stratae.events.protocols.Producer>` and
{py:class}`Consumer <stratae.events.protocols.Consumer>` protocols over `aiormq`. Publish
with {py:func}`RabbitMQPublisher.bind` and a {py:class}`RabbitMQConfig`; consume with
{py:func}`RabbitMQConsumer.handle` and a {py:class}`RabbitMQConsumeConfig`. Both connect in
`async with`.

Each message carries its {py:class}`Envelope <stratae.events.envelope.Envelope>` in its
headers, so correlation ids propagate across publishes and handlers.
{py:class}`Hooks <stratae.events.hooks.Hooks>` passed to `bind` or `emit` run around each
publish, and those passed to `handle` around each delivery's handler, before its ack or nack.

````{example} Publishing an event to RabbitMQ, then consuming it back
<!--- skip: next -->
```{code-block} python
import asyncio
from stratae.events import PubSub
from stratae.integrations.rabbitmq import (
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQConsumer,
    RabbitMQPublisher,
)
from stratae.serde import deserialize, encode

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

@encode.register
def encode_order_placed(order: OrderPlaced) -> dict[str, int]:
    return {"order_id": order.order_id}

@deserialize.register(OrderPlaced)
def decode_order_placed(value: dict[str, int]) -> OrderPlaced:
    return OrderPlaced(**value)

order_placed_event = PubSub(OrderPlaced)

consumer = RabbitMQConsumer("amqp://guest:guest@localhost/")

@consumer.handle(
    order_placed_event,
    config=RabbitMQConsumeConfig(
        exchange="events",
        binding_key="order.placed",
        exchange_type="topic",
        exchange_durable=True,
    ),
)
def on_order_placed(order: OrderPlaced) -> None:
    print(f"received order {order.order_id}")

async def main() -> None:
    async with (
        consumer,  # declares the queue before anything is published
        RabbitMQPublisher("amqp://guest:guest@localhost/") as publisher,
    ):
        place_order = publisher.bind(
            order_placed_event,
            factory=OrderPlaced,
            config=RabbitMQConfig("events", "order.placed"),
        )
        await place_order(order_id=42)

        await asyncio.sleep(0.05)  # wait for delivery

asyncio.run(main())
```
```{output}
received order 42
```
````
"""

from __future__ import annotations

import asyncio
import logging
from copy import copy
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Iterable,
    Literal,
    Protocol,
    cast,
    overload,
)

from aiormq import connect
from pamqp.commands import Basic

from stratae.events import (
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    TIMESTAMP_HEADER,
    AnyEvent,
    Envelope,
    EventType,
    Handler,
    NoPayload,
    PubSub,
)
from stratae.events.binding import AsyncBindMixin
from stratae.events.exceptions import AlreadyConnectedError, NotConnectedError
from stratae.events.handler import invoke
from stratae.events.hooks import Hooks
from stratae.events.middleware import Middleware
from stratae.events.pipeline import run_emit_async, wrap_handler
from stratae.serde import Deserializer, Serializer, deserialize, serialize

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel, AbstractConnection, DeliveredMessage

_log = logging.getLogger(__name__)

_NOT_CONNECTED = "publisher is not connected; open it with 'async with' before emitting"

_ALREADY_CONNECTED = "{} is already connected; exit it before entering it again"

_REQUEST_REPLY_NOT_IMPLEMENTED = (
    "RabbitMQ publishes PubSub events only; request/reply is not yet implemented"
)

_PUBSUB_ONLY = "RabbitMQConsumer handles PubSub events only, got {!r}"

_NO_CONSUME_TARGET = "consume config requires a queue, an exchange, or both"


def _stamp(properties: Basic.Properties | None, envelope: Envelope) -> Basic.Properties:
    """Copy properties, stamped with the message's envelope headers, ids, and timestamp."""
    stamped = copy(properties) if properties is not None else Basic.Properties()
    stamped.headers = (stamped.headers or {}) | envelope.to_headers()
    stamped.message_id = str(envelope.message_id)
    stamped.correlation_id = str(envelope.correlation_id)
    stamped.timestamp = envelope.timestamp
    return stamped


def _envelope_from(message: DeliveredMessage) -> Envelope | None:
    """Rebuild the Envelope carried by a delivered message."""
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
    """Exchange and routing key for a {py:class}`RabbitMQPublisher` binding."""

    __slots__ = ("exchange", "exchange_durable", "exchange_type", "properties", "routing_key")

    def __init__(
        self,
        exchange: str,
        routing_key: str,
        *,
        exchange_type: str | None = None,
        exchange_durable: bool = False,
        properties: Basic.Properties | None = None,
    ) -> None:
        """
        Create a publish target.

        :param exchange: The exchange to publish to.
        :param routing_key: The message's routing key.
        :param exchange_type: If given, declare the exchange with this type before the first
            publish. Leave unset when it's declared elsewhere.
        :param exchange_durable: Whether a declared exchange survives a broker restart. Must
            match any existing declaration.
        :param properties: AMQP properties sent with every message.
        """
        self.exchange = exchange
        self.routing_key = routing_key
        self.exchange_type = exchange_type
        self.exchange_durable = exchange_durable
        self.properties = properties


class RabbitMQPublisher(AsyncBindMixin[RabbitMQConfig]):
    """
    Async RabbitMQ publisher for pub/sub events.

    Holds one connection and channel for the `async with` block. Bind events with `bind`
    from {py:class}`AsyncBindMixin <stratae.events.binding.AsyncBindMixin>`. It implements
    {py:class}`PubSub <stratae.events.patterns.PubSub>` and its subclasses only: binding or
    emitting any other event raises `NotImplementedError`.
    """

    def __init__(
        self,
        url: str,
        serializer: Serializer[object] = serialize,
        *,
        middleware: Middleware = Middleware(),
    ) -> None:
        """
        Create a publisher.

        :param url: AMQP connection URL, e.g. `"amqp://guest:guest@localhost/"`.
        :param serializer: Default payload serializer. Defaults to
            {py:func}`stratae.serde.serialize`.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `publish` half runs around every publish, inside the message's
            envelope; its `handle` half is a consumer's, so one `Middleware` can be shared with
            a consumer.
        :raises TypeError: If any `publish` middleware is a class that isn't a context manager.
        """
        middleware.require(type(self).__name__, asynchronous=True, handle=False)
        self._publish_middleware = middleware.publish
        self._url = url
        self._serializer = serializer
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._declared: set[str] = set()

    async def __aenter__(self) -> RabbitMQPublisher:
        """
        Open the connection and channel.

        :returns: `self`.
        :raises AlreadyConnectedError: If already connected.
        """
        if self._connection is not None:
            raise AlreadyConnectedError(_ALREADY_CONNECTED.format("publisher"))
        self._connection = await connect(self._url)
        try:
            self._channel = await self._connection.channel()
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        """Close the AMQP connection, if one was opened."""
        connection = self._connection
        self._connection = None
        self._channel = None
        self._declared.clear()
        if connection is not None:
            await connection.close()

    async def emit[S, R](
        self,
        event: EventType[S, R, Any, Any],
        config: RabbitMQConfig,
        payload: S,
        *,
        serializer: Serializer[S] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> R:
        """
        Serialize a payload and publish it.

        :param event: The event being emitted.
        :param config: The exchange and routing key.
        :param payload: The payload to publish.
        :param serializer: Overrides the publisher's serializer for this call.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around the publish,
            inside the message's envelope, so they see the ids it's sent with.
        :raises NotConnectedError: If the publisher isn't connected.
        :raises NotImplementedError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        """
        self._check_event(event)
        # The message's own envelope, current while middleware and hooks run, so they see it.
        with Envelope.scope() as envelope:
            if hooks is None and not self._publish_middleware:
                await self._publish(event, config, payload, serializer, envelope)
            else:
                await run_emit_async(
                    hooks,
                    self._publish_middleware,
                    event,
                    config,
                    payload,
                    self._publish,
                    event,
                    config,
                    payload,
                    serializer,
                    envelope,
                )
        return cast(R, None)

    async def _publish(
        self,
        event: AnyEvent,
        config: RabbitMQConfig,
        payload: Any,
        serializer: Serializer[Any] | None,
        envelope: Envelope,
    ) -> None:
        """Serialize a payload and publish it to config's exchange, stamped with `envelope`."""
        if self._channel is None:
            raise NotConnectedError(_NOT_CONNECTED)
        await self._declare_exchange(self._channel, config)
        body = self._encode(event, payload, serializer)
        await self._channel.basic_publish(
            body,
            exchange=config.exchange,
            routing_key=config.routing_key,
            properties=_stamp(config.properties, envelope),
        )

    def _check_event(
        self,
        event: AnyEvent,
        hooks: Hooks[Any, Any] | None = None,  # noqa: S1172
    ) -> None:
        """Raise NotImplementedError for an event that isn't PubSub; any hooks can run."""
        if not isinstance(event, PubSub):
            raise NotImplementedError(_REQUEST_REPLY_NOT_IMPLEMENTED)

    async def _declare_exchange(self, channel: AbstractChannel, config: RabbitMQConfig) -> None:
        """Declare config's exchange once, if it carries a type."""
        if config.exchange_type is None or config.exchange in self._declared:
            return
        await channel.exchange_declare(
            config.exchange, exchange_type=config.exchange_type, durable=config.exchange_durable
        )
        self._declared.add(config.exchange)

    def _encode(self, event: AnyEvent, payload: Any, serializer: Serializer[Any] | None) -> bytes:
        """Serialize payload, or return an empty body for a schema-less event."""
        if event.is_signal:
            return b""
        return serializer(payload) if serializer is not None else self._serializer(payload)


class RabbitMQConsumeConfig:
    """
    Queue or exchange for a {py:class}`RabbitMQConsumer` registration.

    A `queue` alone is consumed competitively. An `exchange` declares and binds a queue
    of its own: private and auto-deleted if `queue` is omitted, durable if named.
    """

    __slots__ = (
        "arguments",
        "auto_delete",
        "binding_keys",
        "durable",
        "exchange",
        "exchange_durable",
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
        exchange_durable: bool = False,
        durable: bool | None = None,
        exclusive: bool | None = None,
        auto_delete: bool | None = None,
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """
        Create a consume source.

        :param queue: The queue to consume. If omitted, a private server-named queue is used.
        :param exchange: An exchange to bind the queue to.
        :param binding_key: The binding's routing key pattern.
        :param exchange_type: The exchange's declared type.
        :param exchange_durable: Whether the exchange survives a broker restart. Must match any
            existing declaration.
        :param durable: Overrides the inferred queue durability.
        :param exclusive: Overrides the inferred queue exclusivity.
        :param auto_delete: Overrides the inferred queue auto-delete flag.
        :param arguments: Queue declaration arguments, e.g. a message TTL.
        :raises ValueError: If neither `queue` nor `exchange` is given.
        """
        if queue is None and exchange is None:
            raise ValueError(_NO_CONSUME_TARGET)
        self.queue = queue
        self.exchange = exchange
        self.binding_keys: tuple[str, ...] = (
            (binding_key,) if isinstance(binding_key, str) else tuple(binding_key)
        )
        self.exchange_type = exchange_type
        self.exchange_durable = exchange_durable
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments


class _Registration:
    """Pairs a registered handler with its deserializer and live consumer tag."""

    __slots__ = ("consumer_tag", "deserializer", "event", "handler")

    def __init__(
        self,
        event: PubSub[Any, Any],
        handler: Handler[[Any], RabbitMQConsumeConfig, Any],
        deserializer: Deserializer | None,
    ) -> None:
        self.event = event
        self.handler = handler
        self.deserializer = deserializer
        self.consumer_tag: str | None = None


class _ConsumeDecorator[S: Any](Protocol):
    """Decorator form of `handle`, returning the Handler."""

    def __call__[R](self, fn: Callable[[S], R]) -> Handler[[S], RabbitMQConsumeConfig, R]: ...


class _SignalConsumeDecorator(Protocol):
    """Decorator form of `handle` for a schema-less event, returning the Handler."""

    def __call__[R](self, fn: Callable[[], R]) -> Handler[[], RabbitMQConsumeConfig, R]: ...


class RabbitMQConsumer:
    """
    Async RabbitMQ consumer for pub/sub events.

    Each registration is its own AMQP consumer, so handlers on one queue compete. Use an
    `exchange` config to fan out. Bodies are decoded with `deserializer(body, type=schema)`,
    defaulting to {py:obj}`stratae.serde.deserialize`.

    A message is acked once its handler, its hooks and its middleware return. If decoding, the
    handler, a `before` hook or a middleware raises, it's logged and nacked without requeue.
    Observing hooks in a slot run concurrently, so the ack waits for the slowest of them, not
    their sum; one that raises is logged and the message is still acked. Sync handlers run on
    the event loop and block other deliveries.

    Exiting the context shuts down gracefully: it cancels every AMQP consumer so no new
    messages arrive, waits up to `drain_timeout` for running handlers to finish and ack, then
    closes the connection. A handler still running at the timeout is cancelled by the close,
    and the broker redelivers its unacked message.
    """

    def __init__(
        self,
        url: str,
        deserializer: Deserializer = deserialize,
        prefetch_count: int | None = None,
        drain_timeout: float | None = 30.0,
        *,
        middleware: Middleware = Middleware(),
    ) -> None:
        """
        Create a consumer.

        :param url: AMQP connection URL, e.g. `"amqp://guest:guest@localhost/"`.
        :param deserializer: Default body decoder. Defaults to
            {py:obj}`stratae.serde.deserialize`.
        :param prefetch_count: Unacked messages delivered ahead; `None` for no limit.
        :param drain_timeout: Seconds exiting waits for running handlers before closing;
            `None` to wait for them indefinitely.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `handle` half runs around every handler call, inside the message's
            envelope; a message that fails to decode never reaches it. Its `publish` half is a
            publisher's, so one `Middleware` can be shared with a publisher.
        :raises TypeError: If any `handle` middleware is a class that isn't a context manager.
        """
        middleware.require(type(self).__name__, asynchronous=True, publish=False)
        self._handle_middleware = middleware.handle
        self._url = url
        self._deserializer = deserializer
        self._prefetch_count = prefetch_count
        self._drain_timeout = drain_timeout
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._registrations: dict[Handler[[Any], RabbitMQConsumeConfig, Any], _Registration] = {}
        self._starting: set[asyncio.Task[None]] = set()
        self._in_flight: set[asyncio.Task[Any]] = set()

    async def __aenter__(self) -> RabbitMQConsumer:
        """
        Open the connection and start consuming for every registration.

        :returns: `self`.
        :raises AlreadyConnectedError: If already connected.
        """
        if self._connection is not None:
            raise AlreadyConnectedError(_ALREADY_CONNECTED.format("consumer"))
        self._connection = await connect(self._url)
        try:
            channel = await self._connection.channel()
            if self._prefetch_count is not None:
                await channel.basic_qos(prefetch_count=self._prefetch_count)
            self._channel = channel
            for registration in self._registrations.values():
                await self._start(channel, registration)
        except BaseException:
            await self.__aexit__(None, None, None)
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop consuming, wait for running handlers, and close the connection, if opened."""
        for task in tuple(self._starting):
            task.cancel()
        channel = self._channel
        try:
            if channel is not None:
                await self._drain(channel)
        finally:
            await self._close()

    async def _drain(self, channel: AbstractChannel) -> None:
        """Cancel every AMQP consumer, then wait up to the drain timeout for running handlers."""
        try:
            for registration in self._registrations.values():
                if registration.consumer_tag is not None:
                    await channel.basic_cancel(registration.consumer_tag)
        except Exception:
            _log.warning("failed to cancel AMQP consumers on exit", exc_info=True)
        # A handler exiting its own consumer would otherwise wait on itself.
        running = self._in_flight - {asyncio.current_task()}
        if not running:
            return
        _, pending = await asyncio.wait(running, timeout=self._drain_timeout)
        if pending:
            _log.warning(
                "%d handler(s) still running after the drain timeout; their messages will be"
                " redelivered",
                len(pending),
            )

    async def _close(self) -> None:
        """Clear the connection state and close the connection, if one was opened."""
        connection = self._connection
        # Cleared before closing, so a failed close leaves no stale state.
        self._connection = None
        self._channel = None
        for registration in self._registrations.values():
            registration.consumer_tag = None
        if connection is not None:
            await connection.close()

    @overload
    def handle[R](
        self,
        event: PubSub[NoPayload, Literal[True]],
        fn: Callable[[], R],
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[NoPayload, object] | None = None,
    ) -> Handler[[], RabbitMQConsumeConfig, R]: ...

    @overload
    def handle(
        self,
        event: PubSub[NoPayload, Literal[True]],
        fn: None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[NoPayload, object] | None = None,
    ) -> _SignalConsumeDecorator: ...

    @overload
    def handle[S, R](
        self,
        event: PubSub[S, Literal[False]],
        fn: Callable[[S], R],
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[S, object] | None = None,
    ) -> Handler[[S], RabbitMQConsumeConfig, R]: ...

    @overload
    def handle[S](
        self,
        event: PubSub[S, Literal[False]],
        fn: None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[S, object] | None = None,
    ) -> _ConsumeDecorator[S]: ...

    def handle(
        self,
        event: AnyEvent,
        fn: Callable[..., Any] | None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[Any, Any] | None = None,
    ) -> (
        Handler[Any, RabbitMQConsumeConfig, Any] | _ConsumeDecorator[Any] | _SignalConsumeDecorator
    ):
        """
        Register a handler for an event, as a decorator or direct call.

        Consumption starts now if connected, otherwise on entering the context.

        :param event: The {py:class}`PubSub <stratae.events.patterns.PubSub>` event to handle.
        :param fn: The handler. If omitted, returns a decorator.
        :param config: The queue or exchange to consume from.
        :param deserializer: Overrides the consumer's deserializer for this registration.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this handler on
            each delivery. A message that fails to decode never reaches the handler, so its
            hooks don't see it.
        :returns: The {py:class}`Handler <stratae.events.handler.Handler>`, for `remove`.
        :raises TypeError: If the event isn't {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        """
        if not isinstance(event, PubSub):
            raise TypeError(_PUBSUB_ONLY.format(event))
        if fn is not None:
            return self._register(event, fn, config, deserializer, hooks)

        def decorator(fn: Callable[..., Any]) -> Handler[Any, RabbitMQConsumeConfig, Any]:
            return self._register(event, fn, config, deserializer, hooks)

        return decorator

    async def remove(self, handler: Handler[[Any], RabbitMQConsumeConfig, Any]) -> None:
        """
        Cancel and remove a registered handler.

        :param handler: The {py:class}`Handler <stratae.events.handler.Handler>` from `handle`.
        """
        registration = self._registrations.pop(handler, None)
        if registration is None:
            return
        if registration.consumer_tag is not None and self._channel is not None:
            await self._channel.basic_cancel(registration.consumer_tag)
            registration.consumer_tag = None

    def _register(
        self,
        event: PubSub[Any, Any],
        fn: Callable[..., Any],
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None,
        hooks: Hooks[Any, Any] | None,
    ) -> Handler[[Any], RabbitMQConsumeConfig, Any]:
        """Wrap fn and its hooks as a Handler, consuming immediately if connected."""
        call, is_async = wrap_handler(
            event, config, fn, hooks, self._handle_middleware, asynchronous=True
        )
        handler: Handler[[Any], RabbitMQConsumeConfig, Any] = Handler(call, config, is_async)
        registration = _Registration(event, handler, deserializer)
        self._registrations[handler] = registration
        channel = self._channel
        if channel is not None:
            task = asyncio.get_running_loop().create_task(self._start(channel, registration))
            self._starting.add(task)
            task.add_done_callback(self._discard_start)
        return handler

    async def _start(self, channel: AbstractChannel, registration: _Registration) -> None:
        """Declare the registration's queue and start consuming, unless removed mid-start."""
        queue = await self._declare(channel, registration.handler.config)
        ok = await channel.basic_consume(queue, self._on_message(registration))
        registration.consumer_tag = ok.consumer_tag
        if registration.handler not in self._registrations and ok.consumer_tag is not None:
            registration.consumer_tag = None
            await channel.basic_cancel(ok.consumer_tag)

    def _queue_declare_setup(self, channel: AbstractChannel, config: RabbitMQConsumeConfig):
        """Return the queue_declare call for config, inferring its flags."""
        named = config.queue is not None
        return channel.queue_declare(
            config.queue or "",
            durable=named if config.durable is None else config.durable,
            exclusive=(not named) if config.exclusive is None else config.exclusive,
            auto_delete=(not named) if config.auto_delete is None else config.auto_delete,
            arguments=config.arguments,
        )

    async def _declare(self, channel: AbstractChannel, config: RabbitMQConsumeConfig) -> str:
        """Declare config's queue, and its exchange and bindings if any."""
        declared = await self._queue_declare_setup(channel, config)
        queue = config.queue or declared.queue or ""
        if config.exchange is None:
            return queue
        await channel.exchange_declare(
            config.exchange,
            exchange_type=config.exchange_type,
            durable=config.exchange_durable,
        )
        for binding_key in config.binding_keys:
            await channel.queue_bind(queue, config.exchange, routing_key=binding_key)
        return queue

    def _discard_start(self, task: asyncio.Task[None]) -> None:
        """Drop a finished start task, logging any failure."""
        self._starting.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            _log.error("failed to start AMQP consumer", exc_info=exc)

    def _deserialize(self, registration: _Registration, body: bytes) -> Any:
        """Decode a body with the registration's deserializer or the default."""
        if registration.event.is_signal:
            return NoPayload()
        deserializer = registration.deserializer or self._deserializer
        return deserializer(body, type=registration.event.schema)

    def _on_message(
        self, registration: _Registration
    ) -> Callable[[DeliveredMessage], Coroutine[Any, Any, None]]:
        """Return the delivery callback, tracked as in flight until it acks or nacks."""

        async def on_message(message: DeliveredMessage) -> None:
            task = cast(asyncio.Task[Any], asyncio.current_task())
            self._in_flight.add(task)
            try:
                await self._deliver(registration, message)
            finally:
                self._in_flight.discard(task)

        return on_message

    async def _deliver(self, registration: _Registration, message: DeliveredMessage) -> None:
        """Decode and dispatch a message, then ack it, or nack it if either step raises."""
        try:
            payload = self._deserialize(registration, message.body)
            with Envelope.scope(_envelope_from(message)):
                await invoke(registration.handler, payload)
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
