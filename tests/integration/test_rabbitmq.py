"""
Integration tests for the RabbitMQ adapters against a live broker.

Run explicitly with ``poe test-rabbitmq`` (or ``pytest -m rabbitmq --no-cov``);
the default test run excludes the ``rabbitmq`` marker, and the suite skips
itself when no broker answers at ``RABBITMQ_URL`` (default
``amqp://guest:guest@localhost/``).

This test suite verifies the following behaviors:

- A msgspec struct round-trips publisher -> broker -> consumer with
  msgspec's encode/decode passed directly to the adapters.
- Envelope ids survive the wire: the handler observes the publishing
  scope's correlation chain, proving header encoding end to end.
- A fanout exchange delivers every message to every subscriber.
- Competing workers on one queue split a backlog without duplication
  under prefetch-based fair dispatch.
- A durable named queue parks messages while its consumer is offline.
- A poison message is dropped without redelivery and without reaching
  the handler.
- A schema-less event round-trips publisher -> broker -> zero-arg handler.
- Hooks passed to bind run around the publish, and hooks passed to handle around the
  consumer's handler, across the broker.
- Publish middleware runs around the publish, and handle middleware around the consumer's
  handler, across the broker.
- A header publish middleware adds reaches the consumer's handle middleware, alongside the
  envelope's.
- A delivery cancelled at the drain timeout closes its message scope with the cancellation,
  and the broker redelivers it to the next consumer, whose scope closes cleanly.
- Re-publishing with a held envelope delivers both copies with its message id.
- A redelivered message keeps the message id it was first published with.
- Under a failure policy, a handler that fails twice is retried after each delay, then acked,
  every attempt carrying the original message id.
- A handler that keeps failing is retried up to the limit, then dead-lettered with its
  attempt count, failure and original message id.
- A message that fails to decode is dead-lettered without being retried.
- A dropping policy leaves nothing behind for a failed message.
- A delivery cancelled at the drain timeout is neither retried nor dead-lettered, and is
  redelivered.
- A publisher's topology keeps messages published to a queue, or to an exchange bound to
  one, before any consumer exists, and delivers them once one starts.
- A publisher and a consumer declaring the same queue with a failure policy don't conflict,
  in either start order.
- A topology declaration that conflicts with the broker's raises on entering the publisher,
  leaving it disconnected.
- A mandatory publish that reaches no queue raises UnroutableError; with a queue bound, it's
  delivered; without mandatory, the unroutable message is dropped silently.
- Publishing to an exchange that doesn't exist raises.
- Through a proxy that cuts connections and refuses new ones for a while, as a broker outage
  does: a consumer reports reconnecting, then resumes and receives what's published after;
  a publisher's emit raises at once during the outage, then publishes once reconnected; a
  message being handled when the connection drops is redelivered, not retried or
  dead-lettered; and exiting during the outage returns promptly, leaving it closed.
- Through the outbox, a committed unit's events reach a consumer under the message ids they
  were written with, and a rolled-back unit's never do.
- A header the outbox producer's publish middleware adds is stored with the record, and
  reaches the consumer's handle middleware once the relay publishes it; the relay's own
  publish middleware sees it too.
- Traced with OpenTelemetry, one trace spans an HTTP request, the RabbitMQ publish its
  handler makes, and the consumer's handling of the message.
- Traced through the outbox, the consumer's handling is in the trace that wrote the record,
  and the relay's publish links to the write.
- Outbox records written during an outage are published once the broker is back, with no
  attempts counted.
"""

import asyncio
import os
import re
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any, cast, override
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx2
import msgspec
import pytest
from aiormq import connect
from aiormq.abc import AbstractChannel
from aiormq.exceptions import ChannelClosed, ChannelPreconditionFailed
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from stratae.backoff import Delays, Exponential
from stratae.events import Envelope, HandleCall, Hooks, Middleware, PublishCall, PubSub
from stratae.events.exceptions import NotConnectedError, UnroutableError
from stratae.events.outbox import InMemoryOutboxStore, OutboxConfig, OutboxProducer, OutboxRelay
from stratae.integrations.opentelemetry import tracing
from stratae.integrations.rabbitmq import (
    ATTEMPTS_HEADER,
    FAILURE_HEADER,
    ConnectionState,
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQConsumer,
    RabbitMQFailurePolicy,
    RabbitMQPublisher,
)
from stratae.lifecycle import AsyncScope

pytestmark = pytest.mark.rabbitmq

_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost/")


class _DropMe(RuntimeError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("drop me")


class _PaymentDeclined(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("payment declined")


class _StillBroken(RuntimeError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("still broken")


class _TryAgain(RuntimeError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("try again")


class Message(msgspec.Struct):
    """Integration payload carrying a single text field."""

    text: str


message_event = PubSub(Message)
heartbeat_event = PubSub(name="heartbeat")


class _Broker:
    """Admin channel on the live broker plus the names to clean up after a test."""

    def __init__(self, channel: AbstractChannel) -> None:
        self.channel = channel
        self.queues: list[str] = []
        self.exchanges: list[str] = []


@pytest.fixture
async def broker() -> AsyncIterator[_Broker]:
    """Yield an admin channel on the live broker, skipping when unreachable."""
    try:
        connection = await asyncio.wait_for(connect(_URL), timeout=5)
    except OSError:
        pytest.skip(f"no RabbitMQ broker reachable at {_URL}")
    channel = await connection.channel()
    broker = _Broker(channel)
    try:
        yield broker
    finally:
        for queue in broker.queues:
            await channel.queue_delete(queue)
        for exchange in broker.exchanges:
            await channel.exchange_delete(exchange)
        await connection.close()


def _unique(prefix: str) -> str:
    """Return a broker-unique name so runs never collide with stale state."""
    return f"{prefix}-{uuid4().hex[:8]}"


_WAIT = 10.0
"""Seconds a test waits for the broker to deliver, or a connection to change state."""


class _Log[T](list[T]):
    """A list a test can wait on, woken each time an item is appended."""

    def __init__(self) -> None:
        super().__init__()
        self._grew = asyncio.Event()

    @override
    def append(self, item: T) -> None:
        super().append(item)
        self._grew.set()
        self._grew = asyncio.Event()

    async def wait_for_length(self, length: int) -> None:
        """Wait until the log holds `length` items."""
        await self._until(lambda: len(self) >= length)

    async def wait_for_item(self, item: T) -> None:
        """Wait until `item` is in the log."""
        await self._until(lambda: item in self)

    async def _until(self, done: Callable[[], bool]) -> None:
        """Wait for appends until `done` holds, failing after `_WAIT` seconds."""
        async with asyncio.timeout(_WAIT):
            while not done():
                await self._grew.wait()


async def _wait_for_state(
    link: RabbitMQPublisher | RabbitMQConsumer, state: ConnectionState
) -> None:
    """Wait until a publisher or consumer reaches `state`, failing after `_WAIT` seconds."""
    async with asyncio.timeout(_WAIT):
        await link.wait_for_state(state)


async def test_msgspec_round_trip(broker: _Broker):
    """
    A struct should round-trip through the broker with msgspec's encode and decode.

    Given: A publisher and consumer passing msgspec's encode/decode directly
    When: A payload is published to the consumer's fanout exchange
    Then: The handler should receive an equal reconstructed struct
    """
    # Arrange
    exchange = _unique("stratae-it")
    broker.exchanges.append(exchange)
    received = _Log[Message]()
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(message_event, received.append, config=RabbitMQConsumeConfig(exchange=exchange))

    # Act
    async with consumer:
        async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
            send = publisher.bind(
                message_event,
                factory=Message,
                config=RabbitMQConfig(exchange, "", exchange_type="fanout"),
            )
            await send(text="hello")
        await received.wait_for_length(1)

    # Assert
    assert received[0] == Message(text="hello")


async def test_signal_event_round_trip(broker: _Broker):
    """
    A schema-less event should round-trip publisher -> broker -> zero-arg handler.

    Given: A publisher and consumer bound to a schema-less event
    When: The event is sent with no payload
    Then: The zero-parameter handler should run
    """
    # Arrange
    exchange = _unique("stratae-it")
    broker.exchanges.append(exchange)
    calls = _Log[bool]()
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)

    def on_heartbeat() -> None:
        calls.append(True)

    consumer.handle(heartbeat_event, on_heartbeat, config=RabbitMQConsumeConfig(exchange=exchange))

    # Act
    async with consumer:
        async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
            send = publisher.bind(
                heartbeat_event, config=RabbitMQConfig(exchange, "", exchange_type="fanout")
            )
            await send()
        await calls.wait_for_length(1)

    # Assert
    assert calls == [True]


async def test_hooks_run_on_both_sides_of_the_broker(broker: _Broker):
    """
    Hooks should run on the publishing and the consuming side.

    Given: A publisher bound with hooks, and a consumer's handler registered with hooks
    When: A payload is published and consumed through the broker
    Then: The publish hooks run around the publish, seeing no reply, and the handle hooks
        run around the handler, seeing its return
    """
    # Arrange
    exchange = _unique("stratae-it")
    broker.exchanges.append(exchange)
    log = _Log[str]()

    def hooks(side: str) -> Hooks[Any, Any]:
        def before(event: Any, payload: Message) -> None:
            log.append(f"{side} before {payload.text}")

        def success(event: Any, payload: Message, reply: object) -> None:
            log.append(f"{side} success {reply!r}")

        return Hooks(before=(before,), success=(success,))

    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)

    def on_message(message: Message) -> None:
        log.append(f"handler {message.text}")

    consumer.handle(
        message_event,
        on_message,
        config=RabbitMQConsumeConfig(exchange=exchange),
        hooks=hooks("handle"),
    )

    # Act
    async with consumer:
        async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
            send = publisher.bind(
                message_event,
                factory=Message,
                config=RabbitMQConfig(exchange, "", exchange_type="fanout"),
                hooks=hooks("publish"),
            )
            await send(text="hello")
        await log.wait_for_length(5)

    # Assert: each side in order; the two sides may interleave.
    assert [entry for entry in log if entry.startswith("publish")] == [
        "publish before hello",
        "publish success None",
    ]
    assert [entry for entry in log if not entry.startswith("publish")] == [
        "handle before hello",
        "handler hello",
        "handle success None",
    ]


async def test_middleware_runs_on_both_sides_of_the_broker(broker: _Broker):
    """
    Middleware should run on the publishing and the consuming side.

    Given: A publisher with publish middleware and a consumer with handle middleware
    When: A payload is published and consumed through the broker
    Then: Each side's middleware runs around its call, seeing the payload
    """
    # Arrange
    exchange = _unique("stratae-it")
    broker.exchanges.append(exchange)
    log = _Log[str]()

    @asynccontextmanager
    async def on_publish(call: PublishCall) -> AsyncGenerator[None, None]:
        log.append(f"publish mw {call.payload.text}")
        yield

    @asynccontextmanager
    async def on_handle(call: HandleCall) -> AsyncGenerator[None, None]:
        yield
        log.append(f"handle mw {call.payload.text}")

    consumer = RabbitMQConsumer(
        _URL, deserializer=msgspec.json.decode, middleware=Middleware(handle=(on_handle,))
    )
    consumer.handle(
        message_event,
        lambda message: log.append(f"handler {message.text}"),
        config=RabbitMQConsumeConfig(exchange=exchange),
    )

    # Act
    async with consumer:
        async with RabbitMQPublisher(
            _URL, serializer=msgspec.json.encode, middleware=Middleware(publish=(on_publish,))
        ) as publisher:
            send = publisher.bind(
                message_event,
                factory=Message,
                config=RabbitMQConfig(exchange, "", exchange_type="fanout"),
            )
            await send(text="hello")
        await log.wait_for_length(3)

    # Assert: the publish side runs first; the handler runs inside the handle middleware.
    assert log == ["publish mw hello", "handler hello", "handle mw hello"]


@asynccontextmanager
async def _tag_tenant(call: PublishCall) -> AsyncGenerator[None, None]:
    call.headers["x-tenant"] = "acme"
    yield


def _recording_headers(seen: list[dict[str, str]]) -> Any:
    """Return handle middleware recording the headers it reads."""

    @asynccontextmanager
    async def record(call: HandleCall) -> AsyncGenerator[None, None]:
        seen.append(dict(call.headers))
        yield

    return record


async def test_middleware_header_crosses_the_broker(broker: _Broker):
    """
    A header publish middleware adds should reach the consumer's handle middleware.

    Given: A publisher whose middleware adds a tenant header, and a consumer whose handle
        middleware records the headers
    When: A payload is published and consumed through the broker
    Then: The handle middleware read the tenant header, and the envelope's correlation id
    """
    # Arrange
    queue = _unique("stratae-it-headers")
    broker.queues.append(queue)
    seen = _Log[dict[str, str]]()
    consumer = RabbitMQConsumer(
        _URL,
        deserializer=msgspec.json.decode,
        middleware=Middleware(handle=(_recording_headers(seen),)),
    )
    consumer.handle(message_event, lambda message: None, config=RabbitMQConsumeConfig(queue))

    # Act
    async with consumer:
        async with RabbitMQPublisher(
            _URL, serializer=msgspec.json.encode, middleware=Middleware(publish=(_tag_tenant,))
        ) as publisher:
            with Envelope.scope() as envelope:
                publisher.route(message_event, RabbitMQConfig("", queue))
                await publisher.emit(message_event, Message("hi"))
        await seen.wait_for_length(1)

    # Assert
    (headers,) = seen
    assert headers["x-tenant"] == "acme"
    assert headers["x-correlation-id"] == str(envelope.correlation_id)


async def test_envelope_survives_the_wire(broker: _Broker):
    """
    Envelope ids should propagate broker-to-handler intact.

    Given: A publish made inside an envelope scope
    When: The consumer's handler runs
    Then: The current envelope should continue the publisher's chain;
          same correlation id, caused by the publishing envelope
    """
    # Arrange
    exchange = _unique("stratae-it")
    broker.exchanges.append(exchange)
    seen = _Log[Envelope | None]()
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(
        message_event,
        lambda payload: seen.append(Envelope.current()),
        config=RabbitMQConsumeConfig(exchange=exchange),
    )

    # Act
    async with consumer:
        async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
            send = publisher.bind(
                message_event,
                factory=Message,
                config=RabbitMQConfig(exchange, "", exchange_type="fanout"),
            )
            with Envelope.scope() as envelope:
                await send(text="traced")
        await seen.wait_for_length(1)

    # Assert
    delivered = seen[0]
    assert delivered is not None
    assert delivered.correlation_id == envelope.correlation_id
    assert delivered.causation_id == envelope.message_id


async def test_fanout_delivers_to_every_subscriber(broker: _Broker):
    """
    A fanout exchange should deliver every message to every subscriber.

    Given: Two consumers subscribed to the same exchange
    When: One message is published
    Then: Both handlers should receive it
    """
    # Arrange
    exchange = _unique("stratae-it")
    broker.exchanges.append(exchange)
    first = _Log[Message]()
    second = _Log[Message]()
    consumer_a = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer_a.handle(message_event, first.append, config=RabbitMQConsumeConfig(exchange=exchange))
    consumer_b = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer_b.handle(message_event, second.append, config=RabbitMQConsumeConfig(exchange=exchange))

    # Act
    async with consumer_a, consumer_b:
        async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
            send = publisher.bind(
                message_event,
                factory=Message,
                config=RabbitMQConfig(exchange, "", exchange_type="fanout"),
            )
            await send(text="broadcast")
        await first.wait_for_length(1)
        await second.wait_for_length(1)

    # Assert
    assert first[0] == Message(text="broadcast")
    assert second[0] == Message(text="broadcast")


async def test_competing_workers_split_backlog(broker: _Broker):
    """
    Two workers on one queue should share a backlog without duplication.

    Given: Two prefetch-1 consumers competing on the same durable queue
    When: Ten messages are published to it
    Then: Every message should be handled exactly once, spread across both
    """
    # Arrange
    queue = _unique("stratae-it-work")
    broker.queues.append(queue)
    handled = _Log[tuple[str, Message]]()

    async def worker_a(payload: Message) -> None:
        handled.append(("a", payload))
        await asyncio.sleep(0.05)

    async def worker_b(payload: Message) -> None:
        handled.append(("b", payload))
        await asyncio.sleep(0.05)

    consumer_a = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode, prefetch_count=1)
    consumer_a.handle(message_event, worker_a, config=RabbitMQConsumeConfig(queue))
    consumer_b = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode, prefetch_count=1)
    consumer_b.handle(message_event, worker_b, config=RabbitMQConsumeConfig(queue))

    # Act
    async with consumer_a, consumer_b:
        async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
            send = publisher.bind(message_event, factory=Message, config=RabbitMQConfig("", queue))
            for index in range(10):
                await send(text=f"task-{index}")
        await handled.wait_for_length(10)

    # Assert
    texts = sorted(message.text for _, message in handled)
    assert texts == sorted(f"task-{index}" for index in range(10))
    assert {worker for worker, _ in handled} == {"a", "b"}


async def test_durable_queue_parks_offline_messages(broker: _Broker):
    """
    A durable named queue should hold messages while its consumer is offline.

    Given: A named queue bound to an exchange by a consumer that then exits
    When: A message is published while no consumer is connected
    Then: Reconnecting the consumer should deliver the parked message
    """
    # Arrange
    exchange = _unique("stratae-it")
    queue = _unique("stratae-it-park")
    broker.exchanges.append(exchange)
    broker.queues.append(queue)
    received = _Log[Message]()
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(
        message_event, received.append, config=RabbitMQConsumeConfig(queue, exchange=exchange)
    )
    async with consumer:
        pass  # first contact declares and binds the durable queue, then goes offline

    # Act
    async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
        send = publisher.bind(
            message_event,
            factory=Message,
            config=RabbitMQConfig(exchange, "", exchange_type="fanout"),
        )
        await send(text="parked")
    async with consumer:
        await received.wait_for_length(1)

    # Assert
    assert received[0] == Message(text="parked")


async def test_poison_message_dropped_without_redelivery(broker: _Broker):
    """
    A message the deserializer rejects should drop without redelivery.

    Given: A consumer decoding with msgspec and a publisher sending bytes
           that are not valid JSON
    When: The message is delivered
    Then: The handler should never run and the queue should end up empty
    """
    # Arrange
    queue = _unique("stratae-it-poison")
    broker.queues.append(queue)
    received: list[Message] = []
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(message_event, received.append, config=RabbitMQConsumeConfig(queue))

    # Act
    async with consumer:
        async with RabbitMQPublisher(_URL, serializer=lambda payload: b"not json") as publisher:
            send = publisher.bind(message_event, factory=Message, config=RabbitMQConfig("", queue))
            await send(text="ignored")
        await asyncio.sleep(0.5)

    # Assert
    declared = await broker.channel.queue_declare(queue, passive=True)
    assert received == []
    assert declared.message_count == 0


async def test_cancelled_scoped_delivery_is_redelivered(broker: _Broker):
    """
    A delivery cancelled at shutdown should close its scope and be redelivered.

    Given: A consumer with a message scope and a short drain timeout, whose handler never
           returns on its first delivery
    When: The consumer exits mid-delivery, and a second consumer starts on the same queue
    Then: The first delivery's scope closes with the cancellation, and the message is
          redelivered and handled, its scope closing cleanly
    """
    # Arrange
    queue = _unique("stratae-it-scoped")
    broker.queues.append(queue)
    scope = AsyncScope("message")
    events = _Log[str]()

    @scope.cache()
    async def get_transaction() -> AsyncGenerator[None, None]:
        await asyncio.sleep(0)
        try:
            yield
        except BaseException as exc:
            events.append(f"rolled back on {type(exc).__name__}")
            raise
        events.append("committed")

    started = asyncio.Event()

    async def stuck(message: Message) -> None:
        await get_transaction()
        started.set()
        await asyncio.Event().wait()

    async def handled(message: Message) -> None:
        await get_transaction()
        events.append(f"handled {message.text}")

    first = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode, drain_timeout=0.1, scope=scope)
    first.handle(message_event, stuck, config=RabbitMQConsumeConfig(queue))
    second = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode, scope=scope)
    second.handle(message_event, handled, config=RabbitMQConsumeConfig(queue))

    # Act
    async with first:
        async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
            send = publisher.bind(message_event, factory=Message, config=RabbitMQConfig("", queue))
            await send(text="retry me")
        await asyncio.wait_for(started.wait(), timeout=5)
    await events.wait_for_item("rolled back on CancelledError")
    async with second:
        await events.wait_for_item("committed")

    # Assert
    assert events == ["rolled back on CancelledError", "handled retry me", "committed"]


async def test_republished_envelope_keeps_message_id(broker: _Broker):
    """
    Re-publishing with a held envelope should deliver each copy with its message id.

    Given: A consumer recording each delivery's envelope, and a held envelope
    When: The same message is published twice with the held envelope
    Then: Both deliveries carry the held envelope's message and correlation ids
    """
    # Arrange
    queue = _unique("stratae-it-republish")
    broker.queues.append(queue)
    seen = _Log[Envelope | None]()
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(
        message_event,
        lambda payload: seen.append(Envelope.current()),
        config=RabbitMQConsumeConfig(queue),
    )
    held = Envelope()

    # Act
    async with consumer:
        async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
            for _ in range(2):
                publisher.route(message_event, RabbitMQConfig("", queue))
                await publisher.emit(message_event, Message("again"), envelope=held)
        await seen.wait_for_length(2)

    # Assert
    assert [(e.message_id, e.correlation_id) for e in seen if e is not None] == [
        (held.message_id, held.correlation_id),
        (held.message_id, held.correlation_id),
    ]


async def test_redelivered_message_keeps_message_id(broker: _Broker):
    """
    A redelivered message should keep the message id it was first published with.

    Given: A consumer with a short drain timeout whose handler records the message id and
           never returns
    When: The consumer exits mid-delivery, and a second consumer starts on the same queue
    Then: The redelivered message carries the first delivery's message id
    """
    # Arrange
    queue = _unique("stratae-it-redeliver")
    broker.queues.append(queue)
    seen = _Log[UUID]()

    def record() -> None:
        envelope = Envelope.current()
        assert envelope is not None
        seen.append(envelope.message_id)

    async def stuck(message: Message) -> None:
        record()
        await asyncio.Event().wait()

    first = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode, drain_timeout=0.1)
    first.handle(message_event, stuck, config=RabbitMQConsumeConfig(queue))
    second = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    second.handle(message_event, lambda message: record(), config=RabbitMQConsumeConfig(queue))

    # Act
    async with first:
        async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
            publisher.route(message_event, RabbitMQConfig("", queue))
            await publisher.emit(message_event, Message("once"))
        await seen.wait_for_length(1)
    async with second:
        await seen.wait_for_length(2)

    # Assert
    assert seen[0] == seen[1]


def _failure_queues(broker: _Broker, prefix: str, policy: RabbitMQFailurePolicy) -> str:
    """Return a unique queue name, registering it and its policy's queues for cleanup."""
    queue = _unique(prefix)
    broker.queues.append(queue)
    broker.queues.extend(
        name
        for name in (
            *(policy.retry_queue(queue, n) for n in range(1, (policy.backoff.retries or 0) + 1)),
            policy.dead_letter_target(queue),
        )
        if name is not None
    )
    return queue


async def _publish(queue: str, body: bytes = b'{"text": "work"}') -> Envelope:
    """Publish a raw body straight to a queue, returning the envelope it was sent with."""
    envelope = Envelope()
    async with RabbitMQPublisher(_URL, serializer=lambda payload: body) as publisher:
        publisher.route(message_event, RabbitMQConfig("", queue))
        await publisher.emit(message_event, Message("work"), envelope=envelope)
    return envelope


async def _count(broker: _Broker, queue: str) -> int:
    """Return how many messages are ready on a queue."""
    declared = await broker.channel.queue_declare(queue, passive=True)
    return declared.message_count or 0


async def test_failing_handler_retried_with_backoff_then_acked(broker: _Broker):
    """
    A handler that fails twice should be retried after each delay, then acked.

    Given: A registration retrying after 0.2 and 0.4 seconds, and a handler failing twice
    When: A message is published
    Then: It's handled three times, each retry no sooner than its delay, every attempt with
          the original message id, and the queue ends empty
    """
    # Arrange
    policy = RabbitMQFailurePolicy(Delays(0.2, 0.4))
    queue = _failure_queues(broker, "stratae-it-retry", policy)
    loop = asyncio.get_running_loop()
    attempts = _Log[tuple[float, UUID]]()

    def flaky(message: Message) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        attempts.append((loop.time(), envelope.message_id))
        if len(attempts) < 3:
            raise _TryAgain()

    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(message_event, flaky, config=RabbitMQConsumeConfig(queue, on_failure=policy))

    # Act
    async with consumer:
        sent = await _publish(queue)
        await attempts.wait_for_length(3)
        await asyncio.sleep(0.1)

    # Assert
    (first, _), (second, _), (third, _) = attempts
    assert second - first >= 0.2
    assert third - second >= 0.4
    assert {message_id for _, message_id in attempts} == {sent.message_id}
    assert await _count(broker, queue) == 0
    assert await _count(broker, cast(str, policy.dead_letter_target(queue))) == 0


async def test_failing_handler_dead_lettered_after_retries(broker: _Broker):
    """
    A handler that keeps failing should be retried up to the limit, then dead-lettered.

    Given: A registration retrying twice, and a handler that always fails
    When: A message is published
    Then: It's handled three times, then lands on the dead-letter queue with three attempts,
          the failure and the original message id
    """
    # Arrange
    policy = RabbitMQFailurePolicy(Delays(0.1, 0.1))
    queue = _failure_queues(broker, "stratae-it-dlq", policy)
    dead_letter = cast(str, policy.dead_letter_target(queue))
    calls = _Log[Message]()

    def failing(message: Message) -> None:
        calls.append(message)
        raise _StillBroken()

    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(message_event, failing, config=RabbitMQConsumeConfig(queue, on_failure=policy))

    # Act
    async with consumer:
        sent = await _publish(queue)
        await calls.wait_for_length(3)
        await asyncio.sleep(0.2)
        parked = await broker.channel.basic_get(dead_letter, no_ack=True)

    # Assert
    properties = parked.header.properties
    headers = properties.headers or {}
    assert parked.body == b'{"text": "work"}'
    assert headers[ATTEMPTS_HEADER] == 3
    assert headers[FAILURE_HEADER] == "_StillBroken: still broken"
    assert properties.message_id == str(sent.message_id)
    assert len(calls) == 3


async def test_undecodable_message_dead_lettered_without_retry(broker: _Broker):
    """
    A message that fails to decode should be dead-lettered on its first attempt.

    Given: A registration with retries, and a body that isn't valid JSON
    When: The message is published
    Then: The handler never runs, and it lands on the dead-letter queue with one attempt
    """
    # Arrange
    policy = RabbitMQFailurePolicy(Delays(0.1, 0.1))
    queue = _failure_queues(broker, "stratae-it-poison-dlq", policy)
    dead_letter = cast(str, policy.dead_letter_target(queue))
    received: list[Message] = []
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(
        message_event, received.append, config=RabbitMQConsumeConfig(queue, on_failure=policy)
    )

    # Act
    async with consumer:
        await _publish(queue, b"not json")
        await asyncio.sleep(0.3)
        parked = await broker.channel.basic_get(dead_letter, no_ack=True)

    # Assert
    assert received == []
    assert parked.body == b"not json"
    assert (parked.header.properties.headers or {})[ATTEMPTS_HEADER] == 1


async def test_dropping_policy_leaves_nothing(broker: _Broker):
    """
    A dropping policy should leave nothing behind for a failed message.

    Given: A registration that drops failures without retrying, and a failing handler
    When: A message is published
    Then: The handler runs once, and the queue ends empty
    """
    # Arrange
    policy = RabbitMQFailurePolicy(dead_letter=False)
    queue = _failure_queues(broker, "stratae-it-drop", policy)
    calls = _Log[Message]()

    def failing(message: Message) -> None:
        calls.append(message)
        raise _DropMe()

    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(message_event, failing, config=RabbitMQConsumeConfig(queue, on_failure=policy))

    # Act
    async with consumer:
        await _publish(queue)
        await calls.wait_for_length(1)
        await asyncio.sleep(0.3)

    # Assert
    assert len(calls) == 1
    assert await _count(broker, queue) == 0


async def test_cancelled_delivery_under_policy_is_redelivered(broker: _Broker):
    """
    A delivery cancelled at the drain timeout should be redelivered, not retried.

    Given: A registration with retries, and a first consumer whose handler never returns
    When: It exits mid-delivery, and a second consumer starts on the same queue
    Then: The second consumer receives the message, and the retry and dead-letter queues
          stay empty
    """
    # Arrange
    policy = RabbitMQFailurePolicy(Delays(0.1))
    queue = _failure_queues(broker, "stratae-it-cancel", policy)
    config = RabbitMQConsumeConfig(queue, on_failure=policy)
    started = asyncio.Event()
    received = _Log[Message]()

    async def stuck(message: Message) -> None:
        started.set()
        await asyncio.Event().wait()

    first = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode, drain_timeout=0.1)
    first.handle(message_event, stuck, config=config)
    second = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    second.handle(message_event, received.append, config=config)

    # Act
    async with first:
        await _publish(queue)
        await asyncio.wait_for(started.wait(), timeout=5)
    async with second:
        await received.wait_for_length(1)

    # Assert
    assert await _count(broker, cast(str, policy.retry_queue(queue, 1))) == 0
    assert await _count(broker, cast(str, policy.dead_letter_target(queue))) == 0


async def test_topology_keeps_messages_published_before_consumers(broker: _Broker):
    """
    A publisher's topology should keep messages sent before any consumer has run.

    Given: A publisher whose topology is a queue, and one bound to a topic exchange, with an
           event routed to each
    When: A message is published to each, then consumers start
    Then: Both consumers receive their message
    """
    # Arrange
    direct = _unique("stratae-it-topology")
    bound = _unique("stratae-it-topology-bound")
    exchange = _unique("stratae-it-topology-x")
    broker.queues += [direct, bound]
    broker.exchanges.append(exchange)
    direct_config = RabbitMQConsumeConfig(direct)
    bound_config = RabbitMQConsumeConfig(
        bound, exchange=exchange, binding_key="job.*", exchange_type="topic"
    )
    bound_event = PubSub(Message, name="BoundMessage")
    received = _Log[Message]()

    # Act
    async with RabbitMQPublisher(
        _URL, serializer=msgspec.json.encode, topology=[direct_config, bound_config]
    ) as publisher:
        publisher.route(message_event, RabbitMQConfig("", direct))
        publisher.route(bound_event, RabbitMQConfig(exchange, "job.run"))
        await publisher.emit(message_event, Message("direct"))
        await publisher.emit(bound_event, Message("bound"))
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(message_event, received.append, config=direct_config)
    consumer.handle(bound_event, received.append, config=bound_config)
    async with consumer:
        await received.wait_for_length(2)

    # Assert
    assert sorted(message.text for message in received) == ["bound", "direct"]


@pytest.mark.parametrize("publisher_first", [True, False])
async def test_publisher_and_consumer_declare_alike(broker: _Broker, publisher_first: bool):
    """
    A publisher and a consumer declaring one queue with a failure policy shouldn't conflict.

    Given: One consume config with a failure policy, shared by a publisher and a consumer
    When: They start in either order, and a message is published
    Then: Neither declaration is refused, and the consumer receives the message
    """
    # Arrange
    policy = RabbitMQFailurePolicy(Delays(0.1))
    queue = _failure_queues(broker, "stratae-it-alike", policy)
    config = RabbitMQConsumeConfig(queue, on_failure=policy)
    received = _Log[Message]()
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(message_event, received.append, config=config)
    publisher = RabbitMQPublisher(_URL, serializer=msgspec.json.encode, topology=[config])
    first, second = (publisher, consumer) if publisher_first else (consumer, publisher)

    # Act
    async with first, second:
        publisher.route(message_event, RabbitMQConfig("", queue))
        await publisher.emit(message_event, Message("alike"))
        await received.wait_for_length(1)

    # Assert
    assert received == [Message("alike")]


async def test_conflicting_topology_raises_on_enter(broker: _Broker):
    """
    A topology declaration the broker refuses should raise when the publisher connects.

    Given: A queue the broker already holds with a length limit, and a publisher declaring
           it without one
    When: The publisher's context is entered
    Then: ChannelPreconditionFailed is raised, and the publisher is left disconnected
    """
    # Arrange
    queue = _unique("stratae-it-conflict")
    broker.queues.append(queue)
    await broker.channel.queue_declare(queue, durable=True, arguments={"x-max-length": 10})
    publisher = RabbitMQPublisher(
        _URL, serializer=msgspec.json.encode, topology=[RabbitMQConsumeConfig(queue)]
    )

    # Act
    with pytest.raises(ChannelPreconditionFailed, match="x-max-length"):
        async with publisher:
            pass

    # Assert
    publisher.route(message_event, RabbitMQConfig("", queue))
    with pytest.raises(NotConnectedError):
        await publisher.emit(message_event, Message("lost"))


async def test_mandatory_publish_requires_a_queue(broker: _Broker):
    """
    A mandatory publish should raise when no queue matches, and deliver when one does.

    Given: A topic exchange with one queue bound for "job.*"
    When: Mandatory messages are published with "job.run" and "report.run", each message's
          routing key coming from its payload, and a plain one with "report.run"
    Then: "job.run" is delivered, the mandatory "report.run" raises UnroutableError, and the
          plain one is dropped without an error
    """
    # Arrange
    exchange = _unique("stratae-it-mandatory")
    queue = _unique("stratae-it-mandatory-q")
    broker.exchanges.append(exchange)
    broker.queues.append(queue)
    config = RabbitMQConsumeConfig(
        queue, exchange=exchange, binding_key="job.*", exchange_type="topic"
    )
    received = _Log[Message]()
    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(message_event, received.append, config=config)

    # Act
    async with consumer:
        async with (
            RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as mandatory,
            RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as plain,
        ):
            by_text = RabbitMQConfig(
                exchange,
                lambda message: "job.run" if message.text == "ok" else "report.run",
                mandatory=True,
            )
            mandatory.route(message_event, by_text)
            plain.route(message_event, RabbitMQConfig(exchange, "report.run"))
            await mandatory.emit(message_event, Message("ok"))
            with pytest.raises(UnroutableError, match=re.escape("report.run")):
                await mandatory.emit(message_event, Message("lost"))
            await plain.emit(message_event, Message("dropped"))
        await received.wait_for_length(1)
        await asyncio.sleep(0.2)

    # Assert
    assert received == [Message("ok")]


async def test_publish_to_missing_exchange_raises(broker: _Broker):
    """
    Publishing to an exchange that doesn't exist should raise.

    Given: A connected publisher and an exchange name the broker doesn't know
    When: A message is published to it
    Then: The channel-close error is raised
    """
    # Arrange
    missing = _unique("stratae-it-missing")

    # Act / Assert
    async with RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
        publisher.route(message_event, RabbitMQConfig(missing, "any"))
        with pytest.raises(ChannelClosed, match="NOT_FOUND"):
            await publisher.emit(message_event, Message("x"))


class _Proxy:
    """
    A TCP proxy to the broker that can cut its connections and refuse new ones.

    Cutting every connection and refusing new ones until `restore` is how a broker outage
    looks to a client, without needing control of the broker itself.
    """

    def __init__(self, upstream_port: int) -> None:
        self._upstream_port = upstream_port
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self.port = 0

    @property
    def url(self) -> str:
        """The AMQP URL that reaches the broker through this proxy."""
        return f"amqp://guest:guest@127.0.0.1:{self.port}/"

    async def start(self) -> None:
        """Accept connections, on the same port as before once started."""
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", self.port)
        self.port = self._server.sockets[0].getsockname()[1]

    async def outage(self) -> None:
        """Stop accepting connections, and cut every open one."""
        if self._server is not None:
            self._server.close()
            self._server = None
        for writer in tuple(self._writers):
            writer.transport.abort()
        self._writers.clear()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_reader, upstream_writer = await asyncio.open_connection(
            "127.0.0.1", self._upstream_port
        )
        self._writers |= {writer, upstream_writer}
        await asyncio.gather(
            self._pipe(reader, upstream_writer),
            self._pipe(upstream_reader, writer),
            return_exceptions=True,
        )

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        finally:
            writer.transport.abort()
            self._writers.discard(writer)


@pytest.fixture
async def proxy(broker: _Broker) -> AsyncIterator[_Proxy]:
    """Yield a started proxy to the live broker, shut down after the test."""
    proxy = _Proxy(urlsplit(_URL).port or 5672)
    await proxy.start()
    try:
        yield proxy
    finally:
        await proxy.outage()


_QUICK = Exponential(0.1, cap=0.2)


async def test_consumer_resumes_after_outage(broker: _Broker, proxy: _Proxy):
    """
    A consumer should reconnect after an outage and receive what's published afterwards.

    Given: A consumer connected through the proxy
    When: The proxy cuts its connection and refuses new ones, then lets them through again
    Then: It reports reconnecting during the outage, connected after, and receives a message
          published once it's back
    """
    # Arrange
    queue = _unique("stratae-it-outage")
    broker.queues.append(queue)
    received = _Log[Message]()
    consumer = RabbitMQConsumer(proxy.url, deserializer=msgspec.json.decode, reconnect=_QUICK)
    consumer.handle(message_event, received.append, config=RabbitMQConsumeConfig(queue))

    async with consumer:
        # Act
        await proxy.outage()
        await _wait_for_state(consumer, ConnectionState.RECONNECTING)
        await asyncio.sleep(0.3)
        during = consumer.state
        await proxy.start()
        await _wait_for_state(consumer, ConnectionState.CONNECTED)
        await _publish(queue)
        await received.wait_for_length(1)

    # Assert
    assert during is ConnectionState.RECONNECTING
    assert received == [Message("work")]


async def test_publisher_publishes_after_outage(broker: _Broker, proxy: _Proxy):
    """
    A publisher should fail fast during an outage, then publish once it has reconnected.

    Given: A publisher connected through the proxy
    When: The proxy has an outage, and then recovers
    Then: emit during the outage raises NotConnectedError at once, and emit after the
          reconnect is delivered
    """
    # Arrange
    queue = _unique("stratae-it-outage-pub")
    broker.queues.append(queue)
    config = RabbitMQConsumeConfig(queue)
    publisher = RabbitMQPublisher(
        proxy.url, serializer=msgspec.json.encode, topology=[config], reconnect=_QUICK
    )
    loop = asyncio.get_running_loop()

    async with publisher:
        # Act
        await proxy.outage()
        await _wait_for_state(publisher, ConnectionState.RECONNECTING)
        started = loop.time()
        publisher.route(message_event, RabbitMQConfig("", queue))
        with pytest.raises(NotConnectedError, match="reconnecting"):
            await publisher.emit(message_event, Message("lost"))
        failed_after = loop.time() - started
        await proxy.start()
        await _wait_for_state(publisher, ConnectionState.CONNECTED)
        await publisher.emit(message_event, Message("sent"))

    # Assert
    assert failed_after < 0.1
    parked = await broker.channel.basic_get(queue, no_ack=True)
    assert msgspec.json.decode(parked.body, type=Message) == Message("sent")


async def test_in_flight_message_redelivered_after_outage(broker: _Broker, proxy: _Proxy):
    """
    A message being handled when the connection drops should be redelivered, not retried.

    Given: A consumer through the proxy with a failure policy, whose handler waits on its
           first delivery
    When: The connection drops mid-handling, and the proxy recovers
    Then: The message is handled again after the reconnect, and its retry and dead-letter
          queues stay empty
    """
    # Arrange
    policy = RabbitMQFailurePolicy(Delays(0.1))
    queue = _failure_queues(broker, "stratae-it-outage-inflight", policy)
    started = asyncio.Event()
    handled = _Log[Message]()

    async def first_waits(message: Message) -> None:
        if not started.is_set():
            started.set()
            await asyncio.Event().wait()
        handled.append(message)

    consumer = RabbitMQConsumer(proxy.url, deserializer=msgspec.json.decode, reconnect=_QUICK)
    consumer.handle(
        message_event, first_waits, config=RabbitMQConsumeConfig(queue, on_failure=policy)
    )

    async with consumer:
        await _publish(queue)
        await asyncio.wait_for(started.wait(), timeout=5)

        # Act
        await proxy.outage()
        await _wait_for_state(consumer, ConnectionState.RECONNECTING)
        await proxy.start()
        await handled.wait_for_length(1)

    # Assert
    assert await _count(broker, cast(str, policy.retry_queue(queue, 1))) == 0
    assert await _count(broker, cast(str, policy.dead_letter_target(queue))) == 0


async def test_exit_during_outage_returns_promptly(broker: _Broker, proxy: _Proxy):
    """
    Exiting during an outage should stop reconnecting and return promptly.

    Given: A consumer and a publisher through the proxy
    When: The proxy has an outage that doesn't end, and both contexts exit
    Then: Both exits return within a second, leaving both closed
    """
    # Arrange
    consumer = RabbitMQConsumer(proxy.url, deserializer=msgspec.json.decode, reconnect=_QUICK)
    publisher = RabbitMQPublisher(proxy.url, serializer=msgspec.json.encode, reconnect=_QUICK)
    await consumer.__aenter__()
    await publisher.__aenter__()
    await proxy.outage()
    await _wait_for_state(consumer, ConnectionState.RECONNECTING)
    await _wait_for_state(publisher, ConnectionState.RECONNECTING)

    # Act
    async with asyncio.timeout(1):
        await consumer.__aexit__(None, None, None)
        await publisher.__aexit__(None, None, None)

    # Assert
    assert consumer.state is ConnectionState.CLOSED
    assert publisher.state is ConnectionState.CLOSED


async def test_outbox_relays_committed_events(broker: _Broker):
    """
    The outbox should deliver a committed unit's events under their ids, and drop the rest.

    Given: An outbox bound to a queue, and a consumer on it
    When: One unit of work commits an event, another rolls back, and the relay runs
    Then: The consumer receives only the committed event, under the message id it was
          written with, and its record is marked sent
    """
    # Arrange
    queue = _unique("stratae-it-outbox")
    broker.queues.append(queue)
    store = InMemoryOutboxStore()
    outbox = OutboxProducer(store, serializer=msgspec.json.encode)
    send = outbox.bind(
        message_event, factory=Message, config=OutboxConfig(RabbitMQConfig("", queue))
    )
    received = _Log[tuple[Message, UUID]]()

    def record(message: Message) -> None:
        envelope = Envelope.current()
        assert envelope is not None
        received.append((message, envelope.message_id))

    consumer = RabbitMQConsumer(_URL, deserializer=msgspec.json.decode)
    consumer.handle(message_event, record, config=RabbitMQConsumeConfig(queue))

    async with store.transaction():
        await send(text="committed")

    async def act() -> None:
        async with store.transaction():
            await send(text="rolled back")
            raise _PaymentDeclined()

    with pytest.raises(_PaymentDeclined):
        await act()
    (written,) = store.pending()

    # Act
    async with consumer, RabbitMQPublisher(_URL, serializer=msgspec.json.encode) as publisher:
        sent = await OutboxRelay(store, publisher, outbox).run_once()
        await received.wait_for_length(1)
        await asyncio.sleep(0.2)

    # Assert
    assert sent == 1
    assert received == [(Message("committed"), written.id)]
    assert store.sent() == [written]


async def test_outbox_relays_middleware_headers(broker: _Broker):
    """
    A header the producer's publish middleware adds should reach the consumer after the relay.

    Given: An outbox producer whose middleware adds a tenant header, a relay publisher whose
        middleware records the headers it sees, and a consumer recording them too
    When: An event is written, and the relay runs
    Then: The record holds the tenant header; the relay's middleware and the consumer's both
        read it, under the record's message id
    """
    # Arrange
    queue = _unique("stratae-it-outbox-headers")
    broker.queues.append(queue)
    store = InMemoryOutboxStore()
    outbox = OutboxProducer(
        store, serializer=msgspec.json.encode, middleware=Middleware(publish=(_tag_tenant,))
    )
    send = outbox.bind(
        message_event, factory=Message, config=OutboxConfig(RabbitMQConfig("", queue))
    )
    relayed: list[dict[str, str]] = []

    @asynccontextmanager
    async def on_relay(call: PublishCall) -> AsyncGenerator[None, None]:
        relayed.append(dict(call.headers))
        yield

    seen = _Log[dict[str, str]]()
    consumer = RabbitMQConsumer(
        _URL,
        deserializer=msgspec.json.decode,
        middleware=Middleware(handle=(_recording_headers(seen),)),
    )
    consumer.handle(message_event, lambda message: None, config=RabbitMQConsumeConfig(queue))
    await send(text="tagged")
    (written,) = store.pending()

    # Act
    async with (
        consumer,
        RabbitMQPublisher(
            _URL, serializer=msgspec.json.encode, middleware=Middleware(publish=(on_relay,))
        ) as publisher,
    ):
        await OutboxRelay(store, publisher, outbox).run_once()
        await seen.wait_for_length(1)

    # Assert
    assert written.headers["x-tenant"] == "acme"
    assert relayed == [{"x-tenant": "acme"}]
    (headers,) = seen
    assert headers["x-tenant"] == "acme"
    assert headers["x-message-id"] == str(written.id)


class _Ended(SpanProcessor):
    """Logs the name of each span as it ends, so a test can wait for one."""

    def __init__(self, names: _Log[str]) -> None:
        self._names = names

    @override
    def on_end(self, span: ReadableSpan) -> None:
        self._names.append(span.name)


def _traced() -> tuple[TracerProvider, InMemorySpanExporter, _Log[str]]:
    """Return a tracer provider exporting to a new in-memory exporter, and its ended spans."""
    exporter, ended = InMemorySpanExporter(), _Log[str]()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    provider.add_span_processor(_Ended(ended))
    return provider, exporter, ended


def _ids(span: ReadableSpan) -> tuple[int, int]:
    """Return a finished span's trace id and span id."""
    assert span.context is not None
    return span.context.trace_id, span.context.span_id


def _parent(span: ReadableSpan) -> int | None:
    """Return a span's parent's span id, or None for a root."""
    return span.parent.span_id if span.parent is not None else None


async def test_trace_spans_request_publish_and_handler(broker: _Broker):
    """
    One trace should span an HTTP request, the publish it makes, and the message's handling.

    Given: An HTTP app instrumented with OpenTelemetry's ASGI middleware, whose route
        publishes through a traced publisher, and a traced consumer on the queue
    When: The route is requested
    Then: The publish span is the server span's child, the process span the publish span's,
        all in one trace
    """
    # Arrange
    queue = _unique("stratae-it-otel")
    broker.queues.append(queue)
    provider, exporter, ended = _traced()
    handled: list[Message] = []
    consumer = RabbitMQConsumer(
        _URL, deserializer=msgspec.json.decode, middleware=tracing(provider)
    )
    consumer.handle(message_event, handled.append, config=RabbitMQConsumeConfig(queue))
    publisher = RabbitMQPublisher(
        _URL, serializer=msgspec.json.encode, middleware=tracing(provider)
    )

    async def place_order(request: Request) -> PlainTextResponse:
        publisher.route(message_event, RabbitMQConfig("", queue))
        await publisher.emit(message_event, Message("lamp"))
        return PlainTextResponse("placed")

    app = OpenTelemetryMiddleware(
        Starlette(routes=[Route("/orders", place_order, methods=["POST"])]),
        tracer_provider=provider,
    )

    # Act
    async with consumer, publisher:
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post("/orders")
        await ended.wait_for_item(f"process {queue}")

    # Assert
    spans = exporter.get_finished_spans()
    (server,) = (span for span in spans if span.kind is SpanKind.SERVER)
    (publish,) = (span for span in spans if span.name == f"publish {queue}")
    (process,) = (span for span in spans if span.name == f"process {queue}")
    assert response.status_code == 200
    assert handled == [Message("lamp")]
    assert _parent(publish) == _ids(server)[1]
    assert _parent(process) == _ids(publish)[1]
    assert _ids(server)[0] == _ids(publish)[0] == _ids(process)[0]


async def test_outbox_trace_links_relay_to_write(broker: _Broker):
    """
    Through the outbox, handling should be in the write's trace, and the relay link to it.

    Given: A traced outbox producer, relay publisher and consumer
    When: An event is written inside a request span, then relayed and consumed
    Then: The write is the request's child and the process span the write's, in one trace;
        the relay's publish is a CLIENT span in its own trace, linking to the write
    """
    # Arrange
    queue = _unique("stratae-it-otel-outbox")
    broker.queues.append(queue)
    provider, exporter, ended = _traced()
    store = InMemoryOutboxStore()
    outbox = OutboxProducer(store, msgspec.json.encode, middleware=tracing(provider))
    send = outbox.bind(
        message_event, factory=Message, config=OutboxConfig(RabbitMQConfig("", queue))
    )
    consumer = RabbitMQConsumer(
        _URL, deserializer=msgspec.json.decode, middleware=tracing(provider)
    )
    consumer.handle(message_event, lambda message: None, config=RabbitMQConsumeConfig(queue))
    with provider.get_tracer("test").start_as_current_span("request"):
        await send(text="lamp")

    # Act
    async with (
        consumer,
        RabbitMQPublisher(
            _URL, serializer=msgspec.json.encode, middleware=tracing(provider)
        ) as publisher,
    ):
        await OutboxRelay(store, publisher, outbox).run_once()
        await ended.wait_for_item(f"process {queue}")

    # Assert
    spans = {span.name: span for span in exporter.get_finished_spans()}
    request, write = spans["request"], spans[f"create {queue}"]
    relay, process = spans[f"publish {queue}"], spans[f"process {queue}"]
    assert _parent(write) == _ids(request)[1]
    assert _parent(process) == _ids(write)[1]
    assert _ids(request)[0] == _ids(write)[0] == _ids(process)[0]
    assert relay.kind is SpanKind.CLIENT
    assert [link.context.span_id for link in relay.links] == [_ids(write)[1]]
    assert _ids(relay)[0] != _ids(write)[0]


async def test_outbox_waits_out_an_outage(broker: _Broker, proxy: _Proxy):
    """
    Outbox records written during an outage should be published once the broker is back.

    Given: A relay publishing through the proxy, and a record written while the proxy is down
    When: The relay runs during the outage, then after the broker is back
    Then: Nothing is sent during the outage and no attempt is counted; afterwards the record
          is published and marked sent, with zero attempts
    """
    # Arrange
    queue = _unique("stratae-it-outbox-outage")
    broker.queues.append(queue)
    config = RabbitMQConsumeConfig(queue)
    store = InMemoryOutboxStore()
    outbox = OutboxProducer(store, serializer=msgspec.json.encode)
    send = outbox.bind(
        message_event, factory=Message, config=OutboxConfig(RabbitMQConfig("", queue))
    )
    publisher = RabbitMQPublisher(
        proxy.url, serializer=msgspec.json.encode, topology=[config], reconnect=_QUICK
    )
    relay = OutboxRelay(store, publisher, outbox)

    async with publisher:
        await proxy.outage()
        await _wait_for_state(publisher, ConnectionState.RECONNECTING)
        await send(text="during the outage")

        # Act
        during = await relay.run_once()
        waiting = store.pending()
        await proxy.start()
        await _wait_for_state(publisher, ConnectionState.CONNECTED)
        after = await relay.run_once()

    # Assert
    assert (during, after) == (0, 1)
    assert [r.attempts for r in waiting] == [0]
    assert [r.attempts for r in store.sent()] == [0]
    parked = await broker.channel.basic_get(queue, no_ack=True)
    assert msgspec.json.decode(parked.body, type=Message) == Message("during the outage")
