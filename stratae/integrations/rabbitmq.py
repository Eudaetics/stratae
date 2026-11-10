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

Both report their {py:class}`ConnectionState` as `state`, e.g. for a health check. Their
`wait_for_state` waits until they reach a given state, e.g. to hold off work until
reconnected. If the connection or channel drops, they reconnect with the backoff of a
{py:class}`RabbitMQReconnect`, then declare their topology again, or resume every
registration. While reconnecting, `emit`
raises {py:exc}`NotConnectedError <stratae.events.exceptions.NotConnectedError>` at once, so
the caller decides whether to retry; nothing is buffered in memory. A message whose handler
was running when the connection dropped is cancelled unacked, and the broker redelivers it.

Each message carries its {py:class}`Envelope <stratae.events.envelope.Envelope>` in its
headers, so correlation ids propagate across publishes and handlers. Each publish gets a
new envelope, a child of the active one, unless `emit` is given one: re-publishing a message
with the envelope it was first sent with, as an outbox relay or a job retry would, keeps its
message id, so consumers can deduplicate on it. Redelivery keeps it too.
Headers that publish middleware adds are sent with the message, and handle middleware reads
a delivered message's text headers. `emit` can be given headers as well, so a message can be
re-published with the ones it was first sent with.
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
import msgspec
from stratae.events import PubSub
from stratae.integrations.rabbitmq import (
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQConsumer,
    RabbitMQPublisher,
)

class OrderPlaced(msgspec.Struct):
    order_id: int

order_placed_event = PubSub(OrderPlaced)

consumer = RabbitMQConsumer(
    "amqp://guest:guest@localhost/", deserializer=msgspec.json.decode
)

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
        RabbitMQPublisher(
            "amqp://guest:guest@localhost/", serializer=msgspec.json.encode
        ) as publisher,
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
from collections import defaultdict
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Iterator, Mapping
from contextlib import suppress
from copy import copy
from dataclasses import KW_ONLY, InitVar, dataclass, field
from enum import StrEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Protocol,
    cast,
    overload,
)
from uuid import UUID

from aiormq import connect
from aiormq.exceptions import PublishError
from pamqp.commands import Basic, Queue

from stratae.events import (
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    TIMESTAMP_HEADER,
    AnyEvent,
    Deserializer,
    Destination,
    Envelope,
    EnvelopeConfig,
    Handler,
    NoPayload,
    PubSub,
    Serializer,
)
from stratae.events.binding import AsyncPubSubBindMixin
from stratae.events.envelope import ENVELOPE_HEADERS
from stratae.events.exceptions import AlreadyConnectedError, NotConnectedError, UnroutableError
from stratae.events.handler import invoke
from stratae.events.hooks import Hooks
from stratae.events.middleware import MessageHeaders, Middleware
from stratae.events.pipeline import receiving, run_emit_async, wrap_handler
from stratae.lifecycle import AsyncScope

if TYPE_CHECKING:
    from aiormq.abc import AbstractChannel, AbstractConnection, DeliveredMessage

_log = logging.getLogger(__name__)

_NOT_CONNECTED = "publisher is not connected; open it with 'async with' before emitting"

_RECONNECTING = "publisher lost its broker connection and is reconnecting; retry the publish"

_BAD_RECONNECT = "reconnect needs 0 < initial_delay <= max_delay and backoff >= 1"

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

_RESERVED = ENVELOPE_HEADERS | {ATTEMPTS_HEADER, FAILURE_HEADER}

_SYSTEM = "rabbitmq"
"""The messaging system RabbitMQ destinations name, as OpenTelemetry's conventions spell it."""

_ROUTING_KEY = "messaging.rabbitmq.destination.routing_key"
"""The conventions' attribute for a published message's routing key."""
"""The headers the adapter sets itself, which middleware and `emit` can't."""


def _stamp(
    properties: Basic.Properties | None, envelope: Envelope, headers: Mapping[str, str]
) -> Basic.Properties:
    """Copy properties, stamped with the message's headers, envelope ids, and timestamp."""
    stamped = copy(properties) if properties is not None else Basic.Properties()
    stamped.headers = (stamped.headers or {}) | dict(headers) | envelope.to_headers()
    stamped.message_id = str(envelope.message_id)
    stamped.correlation_id = str(envelope.correlation_id)
    stamped.timestamp = envelope.timestamp
    return stamped


def _envelope_from(message: DeliveredMessage, new_id: Callable[[], UUID]) -> Envelope | None:
    """Rebuild the Envelope carried by a delivered message, making any missing id with `new_id`."""
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
        return Envelope.from_headers(headers, new_id)
    except ValueError:
        _log.warning("unparseable envelope headers; scoping a fresh envelope: %r", headers)
        return None


def _headers_from(message: DeliveredMessage) -> dict[str, str]:
    """Return a delivered message's text headers, for handle middleware."""
    headers = message.header.properties.headers or {}
    return {name: value for name, value in headers.items() if isinstance(value, str)}


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


@dataclass(frozen=True, slots=True)
class RabbitMQConfig:
    """
    Exchange and routing key for a {py:class}`RabbitMQPublisher` binding.

    An immutable value: configs with the same fields are equal.
    """

    exchange: str
    """The exchange to publish to."""
    routing_key: str
    """The message's routing key."""
    _: KW_ONLY
    exchange_type: str | None = None
    """If given, declare the exchange with this type before the first publish. Leave unset
    when it's declared elsewhere."""
    exchange_durable: bool = False
    """Whether a declared exchange survives a broker restart. Must match any existing
    declaration."""
    properties: Basic.Properties | None = None
    """AMQP properties sent with every message."""
    mandatory: bool = False
    """Whether a message must reach a queue. If it matches none, `emit` raises
    {py:exc}`UnroutableError <stratae.events.exceptions.UnroutableError>`; otherwise the broker
    drops it, and `emit` still succeeds."""
    messaging_destination: Destination = field(init=False, repr=False, compare=False)
    """
    Where messages published with this config go, for tracing.

    Named `exchange:routing_key`, leaving out an empty part, or `amq.default` when both are
    empty. The routing key is also its own attribute.
    """

    def __post_init__(self) -> None:
        """Work out the destination once, since the config can't change."""
        exchange, key = self.exchange, self.routing_key
        name = ":".join(part for part in (exchange, key) if part) or "amq.default"
        attributes = {_ROUTING_KEY: key} if key else {}
        destination = Destination(_SYSTEM, name, attributes=attributes)
        object.__setattr__(self, "messaging_destination", destination)


class ConnectionState(StrEnum):
    """Where a {py:class}`RabbitMQPublisher`'s or {py:class}`RabbitMQConsumer`'s connection is."""

    CONNECTED = "connected"
    """Connected, with an open channel."""

    RECONNECTING = "reconnecting"
    """The connection dropped unexpectedly, and is being reopened."""

    CLOSED = "closed"
    """Not connected: never entered, exited, or dropped with reconnecting turned off."""


class RabbitMQReconnect:
    """
    How a {py:class}`RabbitMQPublisher` or {py:class}`RabbitMQConsumer` reconnects.

    After its connection or channel closes unexpectedly, it waits `initial_delay` seconds and
    tries again, multiplying the wait by `backoff` after each failed attempt, up to
    `max_delay`, until it reconnects or its context exits.
    """

    __slots__ = ("backoff", "initial_delay", "max_delay")

    def __init__(
        self, initial_delay: float = 0.5, max_delay: float = 30.0, backoff: float = 2.0
    ) -> None:
        """
        Create a reconnect policy.

        :param initial_delay: Seconds before the first attempt.
        :param max_delay: The longest wait between attempts.
        :param backoff: What each failed attempt multiplies the wait by.
        :raises ValueError: Unless `0 < initial_delay <= max_delay` and `backoff >= 1`.
        """
        if not 0 < initial_delay <= max_delay or backoff < 1:
            raise ValueError(_BAD_RECONNECT)
        self.initial_delay: float = initial_delay
        self.max_delay: float = max_delay
        self.backoff: float = backoff

    def delays(self) -> Iterator[float]:
        """Yield the wait before each reconnect attempt, without end."""
        delay = self.initial_delay
        while True:
            yield delay
            delay = min(delay * self.backoff, self.max_delay)


def _closing(channel: Any) -> asyncio.Future[Any]:
    """Return a channel's closing future, typed: aiormq leaves its result unknown."""
    return channel.closing


class _Link:
    """A connection and its channel, reopened with backoff after an unexpected close."""

    def __init__(
        self,
        name: str,
        url: str,
        reconnect: RabbitMQReconnect | None,
        on_open: Callable[[AbstractChannel], Awaitable[None]],
        on_lost: Callable[[], None],
    ) -> None:
        self._name = name
        self._url = url
        self._reconnect = reconnect
        self._on_open = on_open
        self._on_lost = on_lost
        self._connection: AbstractConnection | None = None
        self._watch: asyncio.Future[Any] | None = None
        self._reconnecting: asyncio.Task[None] | None = None
        self.channel: AbstractChannel | None = None
        self._state = ConnectionState.CLOSED
        self._waiting: dict[ConnectionState, set[asyncio.Event]] = defaultdict(set)

    @property
    def state(self) -> ConnectionState:
        """Where the connection is."""
        return self._state

    @state.setter
    def state(self, state: ConnectionState) -> None:
        """Move to `state`, waking whoever waits for it."""
        self._state = state
        for reached in self._waiting.pop(state, ()):
            reached.set()

    async def wait_for(self, state: ConnectionState) -> None:
        """Return once the connection is in `state`, now or next."""
        if self._state is state:
            return
        reached = asyncio.Event()
        waiting = self._waiting[state]
        waiting.add(reached)
        try:
            await reached.wait()
        finally:
            waiting.discard(reached)

    async def open(self) -> None:
        """Connect and run `on_open`, raising if already open or if either step fails."""
        if self.state is not ConnectionState.CLOSED:
            raise AlreadyConnectedError(_ALREADY_CONNECTED.format(self._name))
        await self._open()

    async def _open(self) -> None:
        """Connect, open a channel and run `on_open`, closing the connection if any step fails."""
        connection = await connect(self._url)
        try:
            channel = await connection.channel(**_CHANNEL_OPTIONS)
            await self._on_open(channel)
        except BaseException:
            await connection.close()
            raise
        self._connection, self.channel = connection, channel
        self.state = ConnectionState.CONNECTED
        self._watch = _closing(channel)
        self._watch.add_done_callback(self._on_closed)

    def _on_closed(self, closing: asyncio.Future[Any]) -> None:
        """Start reconnecting when the channel closes, unless it was closed on purpose."""
        if closing.cancelled() or self.state is not ConnectionState.CONNECTED:
            return
        stale, reason = cast("AbstractConnection", self._connection), closing.exception()
        self._connection, self.channel, self._watch = None, None, None
        self._on_lost()
        if self._reconnect is None:
            _log.error("%s lost its broker connection: %s", self._name, reason)
            self.state = ConnectionState.CLOSED
        else:
            _log.warning("%s lost its broker connection (%s); reconnecting", self._name, reason)
            self.state = ConnectionState.RECONNECTING
        self._reconnecting = asyncio.get_running_loop().create_task(self._recover(stale))

    async def _recover(self, stale: AbstractConnection) -> None:
        """Close what's left of the old connection, then reconnect with backoff if enabled."""
        with suppress(Exception):
            await stale.close()
        if self._reconnect is None:
            return
        delays = self._reconnect.delays()
        while True:
            await asyncio.sleep(next(delays))
            try:
                await self._open()
            except Exception:
                _log.warning("%s failed to reconnect", self._name, exc_info=True)
                continue
            _log.info("%s reconnected to the broker", self._name)
            return

    async def close(self) -> None:
        """Stop any reconnecting and close the connection, if open."""
        self.state = ConnectionState.CLOSED
        recovering, self._reconnecting = self._reconnecting, None
        if recovering is not None:
            recovering.cancel()
            with suppress(asyncio.CancelledError):
                await recovering
        if self._watch is not None:
            self._watch.cancel()
        connection = self._connection
        # Cleared before closing, so a failed close leaves no stale state.
        self._connection, self.channel, self._watch = None, None, None
        if connection is not None:
            await connection.close()


class RabbitMQPublisher(AsyncPubSubBindMixin[RabbitMQConfig]):
    """
    Async RabbitMQ publisher for pub/sub events.

    Holds one connection and channel for the `async with` block. Bind events with `bind`
    from {py:class}`AsyncPubSubBindMixin <stratae.events.binding.AsyncPubSubBindMixin>`. It
    implements {py:class}`PubSub <stratae.events.patterns.PubSub>` and its subclasses only, so
    it matches {py:class}`PubSubProducer <stratae.events.protocols.PubSubProducer>`. Binding or
    emitting any other event is a type error, and raises `NotImplementedError`.

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
        serializer: Serializer[object],
        *,
        topology: Iterable[RabbitMQConsumeConfig] = (),
        reconnect: RabbitMQReconnect | None = RabbitMQReconnect(),
        middleware: Middleware = Middleware(),
        envelope: EnvelopeConfig = EnvelopeConfig(),
    ) -> None:
        """
        Create a publisher.

        :param url: AMQP connection URL, e.g. `"amqp://guest:guest@localhost/"`.
        :param serializer: Default payload serializer, such as `msgspec.json.encode`. A
            binding's own serializer overrides it.
        :param topology: Consume configs whose queues, exchanges, bindings and failure queues
            are declared on entering the context, before anything is published. Each needs a
            named `queue`. They're declared again after each reconnect.
        :param reconnect: How to reconnect after the connection or channel closes unexpectedly;
            `None` to stay closed instead.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `publish` half runs around every publish, inside the message's
            envelope; its `handle` half is a consumer's, so one `Middleware` can be shared with
            a consumer.
        :param envelope: How the envelope of a message published without a given one is made.
            Its `new_id` makes random ids by default.
        :raises TypeError: If any `publish` middleware is a class that isn't a context manager.
        :raises ValueError: If a `topology` config has no named `queue`.
        """
        middleware.require(type(self).__name__, asynchronous=True, handle=False)
        self._new_id = envelope.new_id
        self._topology = tuple(topology)
        for config in self._topology:
            if config.queue is None:
                raise ValueError(_TOPOLOGY_NEEDS_QUEUE)
        self._publish_middleware = middleware.publish
        self._serializer = serializer
        self._declared: set[str] = set()
        self._link = _Link(
            "publisher", url, reconnect, self._declare_topology, self._declared.clear
        )

    @property
    def state(self) -> ConnectionState:
        """The publisher's {py:class}`ConnectionState`, e.g. for a health check."""
        return self._link.state

    async def wait_for_state(self, state: ConnectionState) -> None:
        """
        Wait until the publisher's connection is in `state`.

        Returns at once if it's already there. Otherwise returns when it next gets there, even
        if it has moved on by the time the caller resumes. Bound the wait with
        `asyncio.timeout`.

        :param state: The {py:class}`ConnectionState` to wait for.
        """
        await self._link.wait_for(state)

    async def __aenter__(self) -> RabbitMQPublisher:
        """
        Open the connection and channel, and declare the topology.

        :returns: `self`.
        :raises AlreadyConnectedError: If already connected.
        :raises aiormq.exceptions.ChannelPreconditionFailed: If a topology declaration
            conflicts with one the broker already holds. The publisher is left disconnected.
        """
        await self._link.open()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop reconnecting and close the AMQP connection, if one was opened."""
        self._declared.clear()
        await self._link.close()

    async def _declare_topology(self, channel: AbstractChannel) -> None:
        """Declare every topology config on a newly opened channel."""
        for config in self._topology:
            await _declare(channel, config)

    async def emit[S](
        self,
        event: PubSub[S, Any],
        config: RabbitMQConfig,
        payload: S,
        *,
        serializer: Serializer[S] | None = None,
        hooks: Hooks[S, None] | None = None,
        envelope: Envelope | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """
        Serialize a payload and publish it.

        :param event: The PubSub event being emitted.
        :param config: The exchange and routing key.
        :param payload: The payload to publish.
        :param serializer: Overrides the publisher's serializer for this call.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around the publish,
            inside the message's envelope, so they see the ids it's sent with.
        :param envelope: Sends the message with this envelope's ids and timestamp, unchanged,
            instead of a child of the active one. Pass the envelope a message was first sent
            with to re-publish it, so consumers can deduplicate on its message id.
        :param headers: Headers to send the message with, which publish middleware sees and
            can add to. Pass a message's headers to re-publish it with them.
        :raises ValueError: If a header is one the publisher sets itself: an envelope header,
            {py:data}`ATTEMPTS_HEADER` or {py:data}`FAILURE_HEADER`.
        :raises NotConnectedError: If the publisher isn't connected.
        :raises NotImplementedError: If the event isn't
            {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises UnroutableError: If `config.mandatory` is set and the message reached no queue.
        :raises aiormq.exceptions.DeliveryError: If the broker refuses the message.
        :raises aiormq.exceptions.ChannelClosed: If the channel closes before the broker
            confirms, e.g. because the exchange doesn't exist.
        """
        self._prepare_event(event)
        stamped = MessageHeaders(headers or {}, reserved=_RESERVED)
        # The message's own envelope, current while middleware and hooks run, so they see it.
        with Envelope.scope(envelope, new_id=self._new_id) as sent:
            if hooks is None and not self._publish_middleware:
                await self._publish(event, config, payload, serializer, sent, stamped)
            else:
                await run_emit_async(
                    hooks,
                    self._publish_middleware,
                    event,
                    config,
                    payload,
                    stamped,
                    self._publish,
                    event,
                    config,
                    payload,
                    serializer,
                    sent,
                    stamped,
                )

    async def _publish(
        self,
        event: AnyEvent,
        config: RabbitMQConfig,
        payload: Any,
        serializer: Serializer[Any] | None,
        envelope: Envelope,
        headers: MessageHeaders,
    ) -> None:
        """Serialize a payload and publish it to config's exchange, with its headers."""
        channel = self._link.channel
        if channel is None:
            reconnecting = self._link.state is ConnectionState.RECONNECTING
            raise NotConnectedError(_RECONNECTING if reconnecting else _NOT_CONNECTED)
        await self._declare_exchange(channel, config)
        body = self._encode(event, payload, serializer)
        try:
            await channel.basic_publish(
                body,
                exchange=config.exchange,
                routing_key=config.routing_key,
                properties=_stamp(config.properties, envelope, headers),
                mandatory=config.mandatory,
            )
        except PublishError as exc:
            raise UnroutableError(_UNROUTABLE.format(config.exchange, config.routing_key)) from exc

    def _prepare_event(self, event: AnyEvent) -> None:
        """Raise NotImplementedError for an event that isn't PubSub."""
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
        self.retry_delays: tuple[float, ...] = tuple(retry_delays)
        for delay in self.retry_delays:
            if delay <= 0:
                raise ValueError(_NON_POSITIVE_DELAY.format(delay))
        if not dead_letter and dead_letter_queue is not None:
            raise ValueError(_DROP_WITH_QUEUE)
        self.dead_letter: bool = dead_letter
        self.dead_letter_queue: str | None = dead_letter_queue

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


@dataclass(frozen=True, slots=True)
class RabbitMQConsumeConfig:
    """
    Queue or exchange for a {py:class}`RabbitMQConsumer` registration.

    A `queue` alone is consumed competitively. An `exchange` declares and binds a queue
    of its own: private and auto-deleted if `queue` is omitted, durable if named. An immutable
    value: configs with the same fields are equal.

    :raises ValueError: If neither `queue` nor `exchange` is given, or `on_failure` is given
        without a `queue`.
    """

    queue: str | None = None
    """The queue to consume. If omitted, a private server-named queue is used."""
    _: KW_ONLY
    exchange: str | None = None
    """An exchange to bind the queue to."""
    binding_key: InitVar[str | Iterable[str]] = ""
    """The binding's routing key pattern, or several. Kept as `binding_keys`."""
    exchange_type: str = "fanout"
    """The exchange's declared type."""
    exchange_durable: bool = False
    """Whether the exchange survives a broker restart. Must match any existing declaration."""
    durable: bool | None = None
    """Overrides the inferred queue durability."""
    exclusive: bool | None = None
    """Overrides the inferred queue exclusivity."""
    auto_delete: bool | None = None
    """Overrides the inferred queue auto-delete flag."""
    arguments: dict[str, Any] | None = None
    """Queue declaration arguments, e.g. a message TTL."""
    on_failure: RabbitMQFailurePolicy | None = None
    """
    A {py:class}`RabbitMQFailurePolicy` retrying, dead-lettering or dropping messages whose
    handling fails. Without one, a failed message is nacked without requeue.
    """
    binding_keys: tuple[str, ...] = field(init=False)
    """The binding's routing key patterns, from `binding_key`."""
    messaging_destination: Destination = field(init=False, repr=False, compare=False)
    """
    Where messages handled with this config come from, for tracing.

    Named `exchange:queue`, leaving out a part that isn't given. Without a `queue`, the server
    names a private one, so the destination is anonymous.
    """

    def __post_init__(self, binding_key: str | Iterable[str]) -> None:
        """Check the config, and keep its binding keys and destination."""
        if self.queue is None and self.exchange is None:
            raise ValueError(_NO_CONSUME_TARGET)
        if self.on_failure is not None and self.queue is None:
            raise ValueError(_FAILURE_POLICY_NEEDS_QUEUE)
        keys = (binding_key,) if isinstance(binding_key, str) else tuple(binding_key)
        object.__setattr__(self, "binding_keys", keys)
        queue = self.queue
        name = ":".join(part for part in (self.exchange, queue) if part)
        destination = Destination(_SYSTEM, name, anonymous=queue is None)
        object.__setattr__(self, "messaging_destination", destination)


async def _declare_queue(
    channel: AbstractChannel, config: RabbitMQConsumeConfig
) -> Queue.DeclareOk:
    """Declare config's queue, inferring its flags."""
    named = config.queue is not None
    return await channel.queue_declare(
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


type _Returns = Awaitable[None] | None
"""What a pub/sub handler returns: `None`, or an awaitable resolving to it."""


class _ConsumeDecorator[S: Any](Protocol):
    """Decorator form of `handle`, returning the Handler."""

    def __call__[R: _Returns](
        self, fn: Callable[[S], R]
    ) -> Handler[[S], RabbitMQConsumeConfig, R]: ...


class _SignalConsumeDecorator(Protocol):
    """Decorator form of `handle` for a schema-less event, returning the Handler."""

    def __call__[R: _Returns](
        self, fn: Callable[[], R]
    ) -> Handler[[], RabbitMQConsumeConfig, R]: ...


class RabbitMQConsumer:
    """
    Async RabbitMQ consumer for pub/sub events.

    Each registration is its own AMQP consumer, so handlers on one queue compete. Use an
    `exchange` config to fan out. Bodies are decoded with `deserializer(body, type=schema)`.

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
    Handlers running when the connection drops are cancelled the same way, and their messages
    redelivered once the consumer reconnects.
    """

    def __init__(
        self,
        url: str,
        deserializer: Deserializer,
        prefetch_count: int | None = None,
        drain_timeout: float | None = 30.0,
        *,
        scope: AsyncScope | None = None,
        reconnect: RabbitMQReconnect | None = RabbitMQReconnect(),
        middleware: Middleware = Middleware(),
        envelope: EnvelopeConfig = EnvelopeConfig(),
    ) -> None:
        """
        Create a consumer.

        :param url: AMQP connection URL, e.g. `"amqp://guest:guest@localhost/"`.
        :param deserializer: Default body decoder, such as `msgspec.json.decode`. A
            registration's own deserializer overrides it.
        :param prefetch_count: Unacked messages delivered ahead; `None` for no limit.
        :param drain_timeout: Seconds exiting waits for running handlers before closing;
            `None` to wait for them indefinitely.
        :param scope: Activated for each message, around its middleware, hooks and handler,
            and closed before the ack or nack. A message that fails to decode never activates
            it.
        :param reconnect: How to reconnect after the connection or channel closes unexpectedly,
            resuming every registration; `None` to stay closed instead.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `handle` half runs around every handler call, inside the message's
            envelope; a message that fails to decode never reaches it. Its `publish` half is a
            publisher's, so one `Middleware` can be shared with a publisher.
        :param envelope: How a delivered message's envelope is completed: its `new_id` makes
            any id the message is missing, or all of them if its envelope can't be read. Random
            by default.
        :raises TypeError: If any `handle` middleware is a class that isn't a context manager.
        """
        middleware.require(type(self).__name__, asynchronous=True, publish=False)
        self._new_id = envelope.new_id
        self._handle_middleware = middleware.handle
        self._scope = scope
        self._deserializer = deserializer
        self._prefetch_count = prefetch_count
        self._drain_timeout = drain_timeout
        self._link = _Link("consumer", url, reconnect, self._resume, self._forget_consumers)
        self._registrations: dict[Handler[[Any], RabbitMQConsumeConfig, Any], _Registration] = {}
        self._starting: set[asyncio.Task[None]] = set()
        self._in_flight: set[asyncio.Task[Any]] = set()

    @property
    def state(self) -> ConnectionState:
        """The consumer's {py:class}`ConnectionState`, e.g. for a health check."""
        return self._link.state

    async def wait_for_state(self, state: ConnectionState) -> None:
        """
        Wait until the consumer's connection is in `state`.

        Returns at once if it's already there. Otherwise returns when it next gets there, even
        if it has moved on by the time the caller resumes. Bound the wait with
        `asyncio.timeout`.

        :param state: The {py:class}`ConnectionState` to wait for.
        """
        await self._link.wait_for(state)

    async def __aenter__(self) -> RabbitMQConsumer:
        """
        Open the connection and start consuming for every registration.

        :returns: `self`.
        :raises AlreadyConnectedError: If already connected.
        """
        try:
            await self._link.open()
        except AlreadyConnectedError:
            raise
        except BaseException:
            self._forget_consumers()
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop consuming, wait for running handlers, and close the connection, if opened."""
        channel = self._link.channel
        try:
            if channel is not None:
                await self._drain(channel)
        finally:
            self._forget_consumers()
            await self._link.close()

    async def _resume(self, channel: AbstractChannel) -> None:
        """Set QoS on a newly opened channel and start consuming for every registration."""
        if self._prefetch_count is not None:
            await channel.basic_qos(prefetch_count=self._prefetch_count)
        for registration in tuple(self._registrations.values()):
            await self._start(channel, registration)

    def _forget_consumers(self) -> None:
        """Cancel pending starts and clear consumer tags, whose channel is gone or going."""
        for task in tuple(self._starting):
            task.cancel()
        for registration in self._registrations.values():
            registration.consumer_tag = None

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

    @overload
    def handle[R: _Returns](
        self,
        event: PubSub[NoPayload, Literal[True]],
        fn: Callable[[], R],
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[NoPayload, None] | None = None,
    ) -> Handler[[], RabbitMQConsumeConfig, R]: ...

    @overload
    def handle(
        self,
        event: PubSub[NoPayload, Literal[True]],
        fn: None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[NoPayload, None] | None = None,
    ) -> _SignalConsumeDecorator: ...

    @overload
    def handle[S, R: _Returns](
        self,
        event: PubSub[S, Literal[False]],
        fn: Callable[[S], R],
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[S, None] | None = None,
    ) -> Handler[[S], RabbitMQConsumeConfig, R]: ...

    @overload
    def handle[S](
        self,
        event: PubSub[S, Literal[False]],
        fn: None = None,
        *,
        config: RabbitMQConsumeConfig,
        deserializer: Deserializer | None = None,
        hooks: Hooks[S, None] | None = None,
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
        :param fn: The handler, returning `None`. If omitted, returns a decorator.
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
        channel = self._link.channel
        if registration.consumer_tag is not None and channel is not None:
            await channel.basic_cancel(registration.consumer_tag)
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
        channel = self._link.channel
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
            with Envelope.scope(_envelope_from(message, self._new_id), new_id=self._new_id):
                if self._handle_middleware:
                    with receiving(_headers_from(message)):
                        await self._run_handler(registration, payload)
                else:
                    await self._run_handler(registration, payload)
        except Exception as exc:
            await self._fail(registration, message, exc, retry=True)
            return
        if message.delivery_tag is not None:
            await message.channel.basic_ack(message.delivery_tag)

    async def _run_handler(self, registration: _Registration, payload: Any) -> None:
        """Call the registration's handler with a decoded payload, in the scope if one is set."""
        if self._scope is None:
            await invoke(registration.handler, payload)
        else:
            async with self._scope:
                await invoke(registration.handler, payload)

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
