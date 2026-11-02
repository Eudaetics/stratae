"""
RabbitMQ async publish and consume adapters.

{py:class}`RabbitMQPublisher` and {py:class}`RabbitMQConsumer` implement the
{py:class}`Producer <stratae.events.protocols.Producer>` and
{py:class}`Consumer <stratae.events.protocols.Consumer>` protocols over `aiormq`. Publish
with {py:func}`RabbitMQPublisher.bind` and a {py:class}`RabbitMQConfig`; consume with
{py:func}`RabbitMQConsumer.handle` and a {py:class}`RabbitMQConsumeConfig`. Both connect in
`async with`. A consumer declares its queues when it starts; give a publisher the same
consume configs as `topology` to declare them when it connects instead, so messages published
before any consumer has run aren't dropped.

Each message carries its {py:class}`Envelope <stratae.events.envelope.Envelope>` in its
headers, so correlation ids propagate across publishes and handlers. Each publish gets a
new envelope, a child of the active one, unless `emit` is given one: re-publishing a message
with the envelope it was first sent with, as an outbox relay or a job retry would, keeps its
message id, so consumers can deduplicate on it. Redelivery keeps it too.
{py:class}`Hooks <stratae.events.hooks.Hooks>` passed to `bind` or `emit` run around each
publish, and those passed to `handle` around each delivery's handler, before its ack or nack.

Give the consumer a `scope` to run each message's work in its own
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`, as
{py:func}`scoped_route <stratae.integrations.starlette.scoped_route>` does per request. It's
activated inside the message's envelope, around its middleware, hooks and handler, and the
message is acked only once it has closed, so a commit in its cleanup lands before the ack.

Give a registration a {py:class}`RabbitMQFailurePolicy` to retry a failed message after
growing delays, then move it to a dead-letter queue, or drop it. Retries wait in TTL retry
queues, so they need no broker plugin, and every attempt keeps the message's envelope.

````{example} Retrying a failed message three times, then dead-lettering it
```{code-block} python
from stratae.integrations.rabbitmq import RabbitMQConsumeConfig, RabbitMQFailurePolicy

policy = RabbitMQFailurePolicy(retry_delays=(1, 10, 60))
config = RabbitMQConsumeConfig("orders", on_failure=policy)

for attempt in range(1, 5):
    print(attempt, policy.retry_queue("orders", attempt))
print("then", policy.dead_letter_target("orders"))
```
```{output}
1 orders.retry.1000
2 orders.retry.10000
3 orders.retry.60000
4 None
then orders.dead-letter
```
````

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
from aiormq.exceptions import PublishError
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
from stratae.events.exceptions import AlreadyConnectedError, NotConnectedError, UnroutableError
from stratae.events.handler import invoke
from stratae.events.hooks import Hooks
from stratae.events.middleware import Middleware
from stratae.events.pipeline import run_emit_async, wrap_handler
from stratae.lifecycle import AsyncScope
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

_CHANNEL_OPTIONS: dict[str, Any] = {"publisher_confirms": True, "on_return_raises": True}
"""Every channel waits for the broker's confirm on publish, and raises for a returned message."""

_UNROUTABLE = "message to exchange {!r} with routing key {!r} reached no queue"

_NO_CONSUME_TARGET = "consume config requires a queue, an exchange, or both"

_FAILURE_POLICY_NEEDS_QUEUE = (
    "a failure policy needs a named queue to route retries and dead letters back to"
)

_TOPOLOGY_NEEDS_QUEUE = (
    "a publisher's topology declares queues for consumers, so each config needs a named queue"
)

_NON_POSITIVE_DELAY = "retry delays must be positive, got {!r}"

_DROP_WITH_QUEUE = "dead_letter_queue is set, but dead_letter=False drops failed messages"

ATTEMPTS_HEADER = "x-attempts"
"""Header counting how many times a retried or dead-lettered message has failed."""

FAILURE_HEADER = "x-failure"
"""Header naming the exception a retried or dead-lettered message last failed with."""


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


def _attempts(message: DeliveredMessage) -> int:
    """Return how many times a delivered message has already failed."""
    attempts = (message.header.properties.headers or {}).get(ATTEMPTS_HEADER)
    return attempts if isinstance(attempts, int) and attempts > 0 else 0


def _failed(message: DeliveredMessage, attempts: int, exc: Exception) -> Basic.Properties:
    """Copy a failed message's properties, stamped with its attempts and failure."""
    properties = copy(message.header.properties)
    properties.headers = dict(properties.headers or {}) | {
        ATTEMPTS_HEADER: attempts,
        FAILURE_HEADER: f"{type(exc).__name__}: {exc}",
    }
    return properties


class RabbitMQConfig:
    """Exchange and routing key for a {py:class}`RabbitMQPublisher` binding."""

    __slots__ = (
        "exchange",
        "exchange_durable",
        "exchange_type",
        "mandatory",
        "properties",
        "routing_key",
    )

    def __init__(
        self,
        exchange: str,
        routing_key: str,
        *,
        exchange_type: str | None = None,
        exchange_durable: bool = False,
        properties: Basic.Properties | None = None,
        mandatory: bool = False,
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
        :param mandatory: Whether a message must reach a queue. If it matches none, `emit`
            raises {py:exc}`UnroutableError <stratae.events.exceptions.UnroutableError>`;
            otherwise the broker drops it, and `emit` still succeeds.
        """
        self.exchange = exchange
        self.routing_key = routing_key
        self.exchange_type = exchange_type
        self.exchange_durable = exchange_durable
        self.properties = properties
        self.mandatory = mandatory


class RabbitMQPublisher(AsyncBindMixin[RabbitMQConfig]):
    """
    Async RabbitMQ publisher for pub/sub events.

    Holds one connection and channel for the `async with` block. Bind events with `bind`
    from {py:class}`AsyncBindMixin <stratae.events.binding.AsyncBindMixin>`. It implements
    {py:class}`PubSub <stratae.events.patterns.PubSub>` and its subclasses only: binding or
    emitting any other event raises `NotImplementedError`.

    Publishing uses publisher confirms: `emit` returns only once the broker has taken
    responsibility for the message, and raises if the broker refuses it or the channel closes
    first. A message that reaches an exchange but matches no queue is still confirmed, and
    dropped; set `mandatory` on its {py:class}`RabbitMQConfig` to raise
    {py:exc}`UnroutableError <stratae.events.exceptions.UnroutableError>` instead.

    A message routed to no queue is dropped by the broker, so one published before its
    consumers have ever run is lost. Pass the consumers' {py:class}`RabbitMQConsumeConfig`s as
    `topology` to declare their queues, exchanges, bindings and failure queues when the
    publisher connects, exactly as the consumers will, so the broker keeps the messages until
    they start.
    """

    def __init__(
        self,
        url: str,
        serializer: Serializer[object] = serialize,
        *,
        topology: Iterable[RabbitMQConsumeConfig] = (),
        middleware: Middleware = Middleware(),
    ) -> None:
        """
        Create a publisher.

        :param url: AMQP connection URL, e.g. `"amqp://guest:guest@localhost/"`.
        :param serializer: Default payload serializer. Defaults to
            {py:func}`stratae.serde.serialize`.
        :param topology: Consume configs whose queues, exchanges, bindings and failure queues
            are declared on entering the context, before anything is published. Each needs a
            named `queue`.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `publish` half runs around every publish, inside the message's
            envelope; its `handle` half is a consumer's, so one `Middleware` can be shared with
            a consumer.
        :raises TypeError: If any `publish` middleware is a class that isn't a context manager.
        :raises ValueError: If a `topology` config has no named `queue`.
        """
        middleware.require(type(self).__name__, asynchronous=True, handle=False)
        self._topology = tuple(topology)
        for config in self._topology:
            if config.queue is None:
                raise ValueError(_TOPOLOGY_NEEDS_QUEUE)
        self._publish_middleware = middleware.publish
        self._url = url
        self._serializer = serializer
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._declared: set[str] = set()

    async def __aenter__(self) -> RabbitMQPublisher:
        """
        Open the connection and channel, and declare the topology.

        :returns: `self`.
        :raises AlreadyConnectedError: If already connected.
        :raises aiormq.exceptions.ChannelPreconditionFailed: If a topology declaration
            conflicts with one the broker already holds. The publisher is left disconnected.
        """
        if self._connection is not None:
            raise AlreadyConnectedError(_ALREADY_CONNECTED.format("publisher"))
        self._connection = await connect(self._url)
        try:
            channel = await self._connection.channel(**_CHANNEL_OPTIONS)
            for config in self._topology:
                await _declare(channel, config)
            self._channel = channel
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
        envelope: Envelope | None = None,
    ) -> R:
        """
        Serialize a payload and publish it.

        :param event: The event being emitted.
        :param config: The exchange and routing key.
        :param payload: The payload to publish.
        :param serializer: Overrides the publisher's serializer for this call.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around the publish,
            inside the message's envelope, so they see the ids it's sent with.
        :param envelope: Sends the message with this envelope's ids and timestamp, unchanged,
            instead of a child of the active one. Pass the envelope a message was first sent
            with to re-publish it, so consumers can deduplicate on its message id.
        :raises NotConnectedError: If the publisher isn't connected.
        :raises NotImplementedError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises UnroutableError: If `config.mandatory` is set and the message reached no queue.
        :raises aiormq.exceptions.DeliveryError: If the broker refuses the message.
        :raises aiormq.exceptions.ChannelClosed: If the channel closes before the broker
            confirms, e.g. because the exchange doesn't exist.
        """
        self._check_event(event)
        # The message's own envelope, current while middleware and hooks run, so they see it.
        with Envelope.scope(envelope) as sent:
            if hooks is None and not self._publish_middleware:
                await self._publish(event, config, payload, serializer, sent)
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
                    sent,
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
        try:
            await self._channel.basic_publish(
                body,
                exchange=config.exchange,
                routing_key=config.routing_key,
                properties=_stamp(config.properties, envelope),
                mandatory=config.mandatory,
            )
        except PublishError as exc:
            raise UnroutableError(_UNROUTABLE.format(config.exchange, config.routing_key)) from exc

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


class RabbitMQFailurePolicy:
    """
    What a {py:class}`RabbitMQConsumer` does with a message whose handling fails.

    A failed message is retried once per entry in `retry_delays`, each attempt waiting that
    many seconds, then dead-lettered or dropped. With no delays, it's dead-lettered or dropped
    on its first failure. A message that fails to decode would fail every attempt, so it skips
    the retries.

    Each delay gets a retry queue, `<queue>.retry.<milliseconds>`, whose message TTL routes
    the message back to the consumed queue once it expires. A dead-lettered message goes to
    `dead_letter_queue`, `<queue>.dead-letter` by default. Retries and dead letters keep the
    message's body, properties and envelope, and add {py:data}`ATTEMPTS_HEADER` and
    {py:data}`FAILURE_HEADER`. The consumer declares these queues when it starts consuming.

    Each failed message is published to its next queue before it's acked, so a crash between
    the two delivers it twice, never zero times.
    """

    __slots__ = ("dead_letter", "dead_letter_queue", "retry_delays")

    def __init__(
        self,
        retry_delays: Iterable[float] = (),
        *,
        dead_letter: bool = True,
        dead_letter_queue: str | None = None,
    ) -> None:
        """
        Create a failure policy.

        :param retry_delays: Seconds to wait before each retry, in order, e.g. `(1, 10, 60)`
            for three retries with a growing backoff.
        :param dead_letter: Whether a message is dead-lettered once its retries run out, or
            acked and dropped.
        :param dead_letter_queue: The dead-letter queue's name; defaults to
            `<queue>.dead-letter`.
        :raises ValueError: If a delay isn't positive, or `dead_letter_queue` is given with
            `dead_letter=False`.
        """
        self.retry_delays = tuple(retry_delays)
        for delay in self.retry_delays:
            if delay <= 0:
                raise ValueError(_NON_POSITIVE_DELAY.format(delay))
        if not dead_letter and dead_letter_queue is not None:
            raise ValueError(_DROP_WITH_QUEUE)
        self.dead_letter = dead_letter
        self.dead_letter_queue = dead_letter_queue

    def retry_queue(self, queue: str, attempt: int) -> str | None:
        """
        Return the retry queue for a message's `attempt`-th failure, or `None` past the last.

        :param queue: The consumed queue.
        :param attempt: How many times the message has failed, counting this failure.
        """
        if attempt > len(self.retry_delays):
            return None
        return _retry_queue(queue, self.retry_delays[attempt - 1])

    def dead_letter_target(self, queue: str) -> str | None:
        """
        Return the dead-letter queue for messages consumed from `queue`, or `None` to drop.

        :param queue: The consumed queue.
        """
        if not self.dead_letter:
            return None
        return self.dead_letter_queue or f"{queue}.dead-letter"


def _milliseconds(seconds: float) -> int:
    """Return a delay in whole milliseconds, as a queue's message TTL takes it."""
    return max(1, round(seconds * 1000))


def _retry_queue(queue: str, delay: float) -> str:
    """Return the name of `queue`'s retry queue for `delay` seconds."""
    return f"{queue}.retry.{_milliseconds(delay)}"


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
        "on_failure",
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
        on_failure: RabbitMQFailurePolicy | None = None,
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
        :param on_failure: A {py:class}`RabbitMQFailurePolicy` retrying, dead-lettering or
            dropping messages whose handling fails. Without one, a failed message is nacked
            without requeue.
        :raises ValueError: If neither `queue` nor `exchange` is given, or `on_failure` is
            given without a `queue`.
        """
        if queue is None and exchange is None:
            raise ValueError(_NO_CONSUME_TARGET)
        if on_failure is not None and queue is None:
            raise ValueError(_FAILURE_POLICY_NEEDS_QUEUE)
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
        self.on_failure = on_failure


def _declare_queue(channel: AbstractChannel, config: RabbitMQConsumeConfig):
    """Return the queue_declare call for config, inferring its flags."""
    named = config.queue is not None
    return channel.queue_declare(
        config.queue or "",
        durable=named if config.durable is None else config.durable,
        exclusive=(not named) if config.exclusive is None else config.exclusive,
        auto_delete=(not named) if config.auto_delete is None else config.auto_delete,
        arguments=config.arguments,
    )


async def _declare(channel: AbstractChannel, config: RabbitMQConsumeConfig) -> str:
    """Declare config's queue, its failure queues, and its exchange and bindings if any."""
    declared = await _declare_queue(channel, config)
    queue = config.queue or declared.queue or ""
    if config.on_failure is not None:
        await _declare_failure_queues(channel, queue, config.on_failure)
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


async def _declare_failure_queues(
    channel: AbstractChannel, queue: str, policy: RabbitMQFailurePolicy
) -> None:
    """Declare a policy's retry queues, each expiring back into `queue`, and dead letters."""
    for delay in dict.fromkeys(policy.retry_delays):
        await channel.queue_declare(
            _retry_queue(queue, delay),
            durable=True,
            arguments={
                "x-message-ttl": _milliseconds(delay),
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue,
            },
        )
    dead_letter = policy.dead_letter_target(queue)
    if dead_letter is not None:
        await channel.queue_declare(dead_letter, durable=True)


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

    A message is acked once its handler, its hooks and its middleware return, and its scope, if
    any, has closed. If decoding, the handler, a `before` hook, a middleware or closing the
    scope raises, it's logged and nacked without requeue, or, if its registration has a
    {py:class}`RabbitMQFailurePolicy`, retried, dead-lettered or dropped under it. A failure
    inside the scope closes it with that error first, so its resources roll back. A delivery
    cancelled at shutdown isn't a failure: it stays unacked. Observing hooks in a slot run
    concurrently, so the ack waits for the slowest of them, not their sum; one that raises is
    logged and the message is still acked. Sync handlers run on the event loop and block other
    deliveries.

    Exiting the context shuts down gracefully: it cancels every AMQP consumer so no new
    messages arrive, waits up to `drain_timeout` for running handlers to finish and ack, then
    closes the connection. A handler still running at the timeout is cancelled by the close,
    its scope closes with the cancellation, and the broker redelivers its unacked message.
    """

    def __init__(
        self,
        url: str,
        deserializer: Deserializer = deserialize,
        prefetch_count: int | None = None,
        drain_timeout: float | None = 30.0,
        *,
        scope: AsyncScope | None = None,
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
        :param scope: Activated for each message, around its middleware, hooks and handler,
            and closed before the ack or nack. A message that fails to decode never activates
            it.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `handle` half runs around every handler call, inside the message's
            envelope; a message that fails to decode never reaches it. Its `publish` half is a
            publisher's, so one `Middleware` can be shared with a publisher.
        :raises TypeError: If any `handle` middleware is a class that isn't a context manager.
        """
        middleware.require(type(self).__name__, asynchronous=True, publish=False)
        self._handle_middleware = middleware.handle
        self._scope = scope
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
            channel = await self._connection.channel(**_CHANNEL_OPTIONS)
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
        queue = await _declare(channel, registration.handler.config)
        ok = await channel.basic_consume(queue, self._on_message(registration))
        registration.consumer_tag = ok.consumer_tag
        if registration.handler not in self._registrations and ok.consumer_tag is not None:
            registration.consumer_tag = None
            await channel.basic_cancel(ok.consumer_tag)

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
        """Decode a message and dispatch it in its scope, then ack it, or settle its failure."""
        try:
            payload = self._deserialize(registration, message.body)
        except Exception as exc:
            await self._fail(registration, message, exc, retry=False)
            return
        try:
            with Envelope.scope(_envelope_from(message)):
                if self._scope is None:
                    await invoke(registration.handler, payload)
                else:
                    async with self._scope:
                        await invoke(registration.handler, payload)
        except Exception as exc:
            await self._fail(registration, message, exc, retry=True)
            return
        if message.delivery_tag is not None:
            await message.channel.basic_ack(message.delivery_tag)

    async def _fail(
        self,
        registration: _Registration,
        message: DeliveredMessage,
        exc: Exception,
        *,
        retry: bool,
    ) -> None:
        """Nack a failed message, or retry, dead-letter or drop it under its failure policy."""
        policy = registration.handler.config.on_failure
        name = registration.event.name
        if policy is None:
            _log.error(
                "handler for event '%s' failed; message nacked without requeue", name, exc_info=exc
            )
            if message.delivery_tag is not None:
                await message.channel.basic_nack(message.delivery_tag, requeue=False)
            return
        queue = cast(str, registration.handler.config.queue)
        attempts = _attempts(message) + 1
        target = policy.retry_queue(queue, attempts) if retry else None
        if target is not None:
            _log.warning(
                "handler for event '%s' failed; retrying via '%s'", name, target, exc_info=exc
            )
        else:
            target = policy.dead_letter_target(queue)
            _log.error(
                "handler for event '%s' failed after %d attempt(s); message %s",
                name,
                attempts,
                f"dead-lettered to '{target}'" if target is not None else "dropped",
                exc_info=exc,
            )
        if message.delivery_tag is None:
            return
        if target is not None:
            await message.channel.basic_publish(
                message.body,
                exchange="",
                routing_key=target,
                properties=_failed(message, attempts, exc),
            )
        await message.channel.basic_ack(message.delivery_tag)
