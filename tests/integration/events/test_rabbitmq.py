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
"""

import asyncio
import os
from collections.abc import AsyncIterator, Callable
from uuid import uuid4

import msgspec
import pytest
from aiormq import connect
from aiormq.abc import AbstractChannel

from stratae.events import Envelope, Event, PubSub
from stratae.integrations.events.rabbitmq import (
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQConsumer,
    RabbitMQPublisher,
)

pytestmark = pytest.mark.rabbitmq

_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost/")


class Message(msgspec.Struct):
    """Integration payload carrying a single text field."""

    text: str


message_event = Event(Message, PubSub)


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


async def _eventually(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    """Poll until the condition holds, failing after the timeout."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition():
        if loop.time() > deadline:
            msg = "condition not met before timeout"
            raise AssertionError(msg)
        await asyncio.sleep(0.05)


async def test_msgspec_round_trip(broker: _Broker):
    """
    A struct should round-trip through the broker with msgspec serde.

    Given: A publisher and consumer passing msgspec's encode/decode directly
    When: A payload is published to the consumer's fanout exchange
    Then: The handler should receive an equal reconstructed struct
    """
    # Arrange
    exchange = _unique("stratae-it")
    broker.exchanges.append(exchange)
    received: list[Message] = []
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
        await _eventually(lambda: len(received) == 1)

    # Assert
    assert received[0] == Message(text="hello")


async def test_envelope_survives_the_wire(broker: _Broker):
    """
    Envelope ids should propagate broker-to-handler intact.

    Given: A publish made inside an envelope scope
    When: The consumer's handler runs
    Then: The current envelope should continue the publisher's chain —
          same correlation id, caused by the publishing envelope
    """
    # Arrange
    exchange = _unique("stratae-it")
    broker.exchanges.append(exchange)
    seen: list[Envelope | None] = []
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
        await _eventually(lambda: len(seen) == 1)

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
    first: list[Message] = []
    second: list[Message] = []
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
        await _eventually(lambda: len(first) == 1 and len(second) == 1)

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
    first: list[Message] = []
    second: list[Message] = []

    async def worker_a(payload: Message) -> None:
        first.append(payload)
        await asyncio.sleep(0.05)

    async def worker_b(payload: Message) -> None:
        second.append(payload)
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
        await _eventually(lambda: len(first) + len(second) == 10, timeout=10.0)

    # Assert
    texts = sorted(message.text for message in first + second)
    assert texts == sorted(f"task-{index}" for index in range(10))
    assert first and second


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
    received: list[Message] = []
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
        await _eventually(lambda: len(received) == 1)

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
