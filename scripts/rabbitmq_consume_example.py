"""
Manual verification script: consume events published to a local RabbitMQ instance.

There is no ``RabbitMQConsumer`` (or any subscribe-side adapter) in
``stratae.integrations.events.rabbitmq`` yet — only ``RabbitMQPublisher``. So
this script talks to ``aiormq`` directly and decodes payloads the same way
``RabbitMQPublisher.emit`` encoded them: JSON bytes via ``stratae.serde.pack``,
which a plain ``json.loads`` reverses.

It declares the exchange, queue, and binding itself, so it can be started
before or after ``scripts/rabbitmq_publish_example.py``.

Requires a RabbitMQ broker reachable at AMQP_URL, e.g.:

    docker run --rm -p 5672:5672 rabbitmq:3-management

and the "rabbitmq" extra installed:

    pip install -e '.[rabbitmq]'
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import TYPE_CHECKING, TypedDict, cast

from aiormq import connect

if TYPE_CHECKING:
    from aiormq.abc import DeliveredMessage

AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost/")
EXCHANGE = "stratae.demo"
ROUTING_KEY = "order.placed"
QUEUE = "stratae.demo.order-placed"
EXPECTED_MESSAGES = 3
TIMEOUT_SECONDS = 30.0


class OrderPlacedPayload(TypedDict):
    """Decoded shape of the ``OrderPlaced`` payload published by the example publisher."""

    order_id: int
    item: str


async def main() -> None:
    """Bind the demo queue and print each decoded payload as it arrives."""
    connection = await connect(AMQP_URL)
    channel = await connection.channel()

    await channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    await channel.queue_declare(queue=QUEUE, durable=True)
    await channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)

    received: asyncio.Queue[OrderPlacedPayload] = asyncio.Queue()

    async def on_message(message: DeliveredMessage) -> None:
        payload = cast(OrderPlacedPayload, json.loads(message.body))
        await channel.basic_ack(delivery_tag=message.delivery.delivery_tag)
        await received.put(payload)

    await channel.basic_consume(queue=QUEUE, consumer_callback=on_message)
    print(f"waiting for {EXPECTED_MESSAGES} message(s) on queue '{QUEUE}'...")

    try:
        for _ in range(EXPECTED_MESSAGES):
            payload = await asyncio.wait_for(received.get(), timeout=TIMEOUT_SECONDS)
            print(f"received: {payload}")
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
