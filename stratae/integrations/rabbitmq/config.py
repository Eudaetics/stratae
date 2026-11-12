"""Immutable configs for publishing to and consuming from RabbitMQ, and the failure policy."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import KW_ONLY, InitVar, dataclass, field, replace
from typing import Any

from pamqp.commands import Basic

from stratae._frozen import FrozenMapping
from stratae.backoff import Backoff, Delays
from stratae.events import Destination, Serializer
from stratae.integrations.rabbitmq._retry import retry_delays, retry_queue_name

_NO_CONSUME_TARGET = "consume config requires a queue, an exchange, or both"

_FAILURE_POLICY_NEEDS_QUEUE = (
    "a failure policy needs a named queue to route retries and dead letters back to"
)

_NON_POSITIVE_DELAY = "retry delays must be positive, got {!r}"

_ENDLESS_RETRIES = "a failure policy's backoff must run out, since each delay names a retry queue"

_DROP_WITH_QUEUE = "dead_letter_queue is set, but dead_letter=False drops failed messages"

_NO_RETRIES = Delays()

ATTEMPTS_HEADER = "x-attempts"
"""Header counting how many times a retried or dead-lettered message has failed."""

FAILURE_HEADER = "x-failure"
"""Header naming the exception a retried or dead-lettered message last failed with."""

_SYSTEM = "rabbitmq"
"""The messaging system RabbitMQ destinations name, as OpenTelemetry's conventions spell it."""

_ROUTING_KEY = "messaging.rabbitmq.destination.routing_key"
"""The conventions' attribute for a published message's routing key."""


@dataclass(frozen=True, slots=True)
class RabbitMQConfig:
    """
    Exchange and routing key for an event's route on a RabbitMQ publisher.

    Route or bind an event to one on a
    {py:class}`RabbitMQPublisher <stratae.integrations.rabbitmq.publisher.RabbitMQPublisher>`.
    The routing key is a string, or a function giving each message's routing key from its
    payload. Each emit publishes with a config whose routing key is a string: this one, or the
    one {py:meth}`at` makes for the message's key. So middleware sees where each message went.

    An immutable value: configs with the same fields are equal.
    """

    exchange: str
    """The exchange to publish to."""
    routing_key: str | Callable[[Any], str]
    """The messages' routing key, or a function giving each message's key from its payload."""
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
    serializer: Serializer[Any] | None = None
    """
    Encodes this route's payloads, in place of the publisher's serializer. An outbox route to
    this config encodes its records with it too, in place of the producer's.
    """
    messaging_destination: Destination = field(init=False, repr=False, compare=False)
    """
    Where messages published with this config go, for tracing.

    Named `exchange:routing_key`, leaving out an empty part, or `amq.default` when both are
    empty. The routing key is also its own attribute.
    """

    def __post_init__(self) -> None:
        """Work out the destination once, since the config can't change."""
        exchange = self.exchange
        key = self.routing_key if isinstance(self.routing_key, str) else ""
        name = ":".join(part for part in (exchange, key) if part) or "amq.default"
        attributes = {_ROUTING_KEY: key} if key else {}
        destination = Destination(_SYSTEM, name, attributes=attributes)
        object.__setattr__(self, "messaging_destination", destination)

    def address(self, payload: Any, /) -> str | None:
        """
        Return the payload's routing key, or `None` if every message shares this config's.

        :param payload: The payload being emitted.
        """
        key = self.routing_key
        return None if isinstance(key, str) else key(payload)

    def at(self, routing_key: str) -> RabbitMQConfig:
        """
        Return this config with one message's routing key.

        :param routing_key: The message's routing key.
        """
        return replace(self, routing_key=routing_key)


@dataclass(frozen=True, slots=True)
class RabbitMQFailurePolicy:
    """
    What a RabbitMQ consumer does with a message whose handling fails.

    Give one to a {py:class}`RabbitMQConsumeConfig` as `on_failure`. A failed message is retried
    after each delay of its {py:class}`Backoff <stratae.backoff.Backoff>`, then dead-lettered or
    dropped once the backoff runs out. With no retries, it's dead-lettered or dropped on its
    first failure. A message that fails to decode would fail every attempt, so it skips the
    retries.

    Each delay gets a retry queue, `<queue>.retry.<milliseconds>`, whose message TTL routes
    the message back to the consumed queue once it expires. A dead-lettered message goes to
    `dead_letter_queue`, `<queue>.dead-letter` by default. Retries and dead letters keep the
    message's body, properties and envelope, and add {py:data}`ATTEMPTS_HEADER` and
    {py:data}`FAILURE_HEADER`. The consumer declares these queues when it starts consuming.

    Each failed message is published to its next queue before it's acked, so a crash between
    the two delivers it twice, never zero times.

    An immutable value: policies with the same fields are equal and hash alike.

    :raises ValueError: If the backoff never runs out, a delay isn't positive, or
        `dead_letter_queue` is given with `dead_letter=False`.
    """

    backoff: Backoff = _NO_RETRIES
    """
    The delay before each retry, e.g. `Delays(1, 10, 60)` for three retries. It must run out,
    since each delay names a retry queue.
    """
    _: KW_ONLY
    dead_letter: bool = True
    """Whether a message is dead-lettered once its retries run out, or acked and dropped."""
    dead_letter_queue: str | None = None
    """The dead-letter queue's name; defaults to `<queue>.dead-letter`."""

    def __post_init__(self) -> None:
        """Check the backoff runs out with positive delays, and the dead-letter settings agree."""
        if self.backoff.retries is None:
            raise ValueError(_ENDLESS_RETRIES)
        for delay in retry_delays(self.backoff):
            if delay <= 0:
                raise ValueError(_NON_POSITIVE_DELAY.format(delay))
        if not self.dead_letter and self.dead_letter_queue is not None:
            raise ValueError(_DROP_WITH_QUEUE)

    def retry_queue(self, queue: str, attempt: int) -> str | None:
        """
        Return the retry queue for a message's `attempt`-th failure, or `None` past the last.

        :param queue: The consumed queue.
        :param attempt: How many times the message has failed, counting this failure.
        """
        delay = self.backoff.delay(attempt)
        return None if delay is None else retry_queue_name(queue, delay)

    def dead_letter_target(self, queue: str) -> str | None:
        """
        Return the dead-letter queue for messages consumed from `queue`, or `None` to drop.

        :param queue: The consumed queue.
        """
        if not self.dead_letter:
            return None
        return self.dead_letter_queue or f"{queue}.dead-letter"


@dataclass(frozen=True, slots=True)
class RabbitMQConsumeConfig:
    """
    Queue or exchange for a RabbitMQ consumer's registration.

    Pass one to a consumer's
    {py:meth}`handle <stratae.integrations.rabbitmq.consumer.RabbitMQConsumer.handle>`. A
    `queue` alone is consumed competitively. An `exchange` declares and binds a queue of its
    own: private and auto-deleted if `queue` is omitted, durable if named. An immutable value:
    configs with the same fields are equal and hash alike.

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
    arguments: Mapping[str, Any] | None = None
    """Queue declaration arguments, e.g. a message TTL. Kept as a read-only copy."""
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
        if self.arguments is not None:
            object.__setattr__(self, "arguments", FrozenMapping(self.arguments))
        queue = self.queue
        name = ":".join(part for part in (self.exchange, queue) if part)
        destination = Destination(_SYSTEM, name, anonymous=queue is None)
        object.__setattr__(self, "messaging_destination", destination)
