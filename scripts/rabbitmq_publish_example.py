"""
Manual verification script: publish events to a local RabbitMQ instance.

Exercises ``RabbitMQPublisher`` against a real broker rather than the mocked
connection used in ``tests/unit/integrations/events/test_rabbitmq.py``. Run
``scripts/rabbitmq_consume_example.py`` first (or alongside this, in another
terminal) to see the published payloads arrive and get decoded.

Requires a RabbitMQ broker reachable at AMQP_URL, e.g.:

    docker run --rm -p 5672:5672 rabbitmq:3-management

and the "rabbitmq" extra installed:

    pip install -e '.[rabbitmq]'
"""

from __future__ import annotations

import asyncio
import os

from stratae.events.event import PubSub, event
from stratae.integrations.events.rabbitmq import RabbitMQConfig, RabbitMQPublisher

AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost/")
EXCHANGE = "stratae.demo"
ROUTING_KEY = "order.placed"


@event(PubSub)
class OrderPlaced:
    """Example payload published for manual RabbitMQ verification."""

    def __init__(self, order_id: int, item: str) -> None:
        self.order_id = order_id
        self.item = item

    def to_dict(self) -> dict[str, int | str]:
        """Return the JSON-serializable representation used by ``pack``."""
        return {"order_id": self.order_id, "item": self.item}


async def main() -> None:
    """Publish a handful of ``OrderPlaced`` events to the demo exchange."""
    async with RabbitMQPublisher(AMQP_URL) as publisher:
        order_placed = publisher.bind(OrderPlaced, config=RabbitMQConfig(EXCHANGE, ROUTING_KEY))
        for order_id in range(1, 4):
            await order_placed(order_id=order_id, item=f"widget-{order_id}")
            print(f"published order_id={order_id}")


if __name__ == "__main__":
    asyncio.run(main())
