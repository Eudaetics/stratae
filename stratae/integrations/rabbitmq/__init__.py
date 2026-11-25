"""
RabbitMQ async publish and consume adapters.

{py:class}`RabbitMQPublisher <stratae.integrations.rabbitmq.publisher.RabbitMQPublisher>` and
{py:class}`RabbitMQConsumer <stratae.integrations.rabbitmq.consumer.RabbitMQConsumer>` implement the
{py:class}`Producer <stratae.events.protocols.Producer>` and
{py:class}`Consumer <stratae.events.protocols.Consumer>` protocols over `aiormq`. Publish with
{py:func}`bind <stratae.integrations.rabbitmq.publisher.RabbitMQPublisher.bind>` and a
{py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.config.RabbitMQConfig>`. Consume with
{py:func}`handle <stratae.integrations.rabbitmq.consumer.RabbitMQConsumer.handle>` and a
{py:class}`RabbitMQConsumeConfig <stratae.integrations.rabbitmq.config.RabbitMQConsumeConfig>`. Both
connect in `async with`. Every name here can be imported from this package. A consumer declares its
queues when it starts; give a publisher the same consume configs as `topology` to declare them when
it connects instead, so messages published before any consumer has run aren't dropped.

Both report their
{py:class}`ConnectionState <stratae.integrations.rabbitmq.connection.ConnectionState>` as `state`,
e.g. for a health check. Their `wait_for_state` waits until they reach a given state, e.g. to hold
off work until reconnected. If the connection or channel drops, they reconnect after each delay of
their `reconnect` {py:class}`Backoff <stratae.backoff.Backoff>`, then declare their topology again,
or resume every registration. A backoff that runs out leaves them closed. While reconnecting, `emit`
raises {py:exc}`NotConnectedError <stratae.events.exceptions.NotConnectedError>` at once, so the
caller decides whether to retry; nothing is buffered in memory. A message whose handler was running
when the connection dropped is cancelled unacked, and the broker redelivers it.

Each message carries its {py:class}`Envelope <stratae.events.envelope.Envelope>` in its
headers, so correlation ids propagate across publishes and handlers. Each publish gets a
new envelope, a child of the active one, unless `emit` is given one: re-publishing a message
with the envelope it was first sent with, as an outbox relay or a job retry would, keeps its
message id, so consumers can deduplicate on it. Redelivery keeps it too. The envelope's
headers are named by the `headers` of the publisher's and the consumer's
{py:class}`EnvelopeFactory <stratae.events.envelope.EnvelopeFactory>`, so a publisher and its
consumers need the same names.
Headers that publish middleware adds are sent with the message, as AMQP field values: text,
numbers, booleans, timestamps, bytes, and lists and tables of them. A value AMQP can't carry
is refused with `TypeError` when the message is sent. Handle middleware reads a delivered
message's headers as they arrived, without its envelope's. It reads the envelope as a value,
from {py:meth}`Envelope.current <stratae.events.envelope.Envelope.current>`, as on a direct bus.
`emit` can be given headers as well, so a message can be re-published with the ones it was
first sent with.
{py:class}`Hooks <stratae.events.hooks.Hooks>` passed to `bind` or `emit` run around each
publish, and those passed to `handle` around each delivery's handler, before its ack or nack.

Run each message's work in its own
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`, as
{py:func}`scoped_route <stratae.integrations.starlette.scoped_route>` does per request, by
activating it in a handle middleware. As the first one, it holds the other middleware, the
hooks and the handler. The message is acked only once the middleware returns, so a commit in
the scope's cleanup lands before the ack. A middleware placed outside it, such as one that
retries, gets a fresh activation for each attempt.

````{example} A scope for each message
```{code-block} python
from stratae.events import Middleware
from stratae.lifecycle import AsyncScope

message = AsyncScope("message")

async def in_message(call, next):
    async with message:
        return await next(call)

middleware = Middleware(handle=(in_message,))
```
````

Give a registration a
{py:class}`RabbitMQFailurePolicy <stratae.integrations.rabbitmq.config.RabbitMQFailurePolicy>`
to retry a failed message after growing delays, then move it to a dead-letter queue, or drop it.
Retries wait in TTL retry queues, so they need no broker plugin, and every attempt keeps the
message's envelope.

````{example} Retrying a failed message three times, then dead-lettering it
```{code-block} python
from stratae.backoff import Delays
from stratae.integrations.rabbitmq import RabbitMQConsumeConfig, RabbitMQFailurePolicy

policy = RabbitMQFailurePolicy(Delays(1, 10, 60))
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

from .config import (
    ATTEMPTS_HEADER,
    FAILURE_HEADER,
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQFailurePolicy,
    RabbitMQPublish,
)
from .connection import ConnectionState
from .consumer import RabbitMQConsumer
from .publisher import RabbitMQPublisher

__all__ = [
    "ATTEMPTS_HEADER",
    "FAILURE_HEADER",
    "ConnectionState",
    "RabbitMQConfig",
    "RabbitMQConsumeConfig",
    "RabbitMQConsumer",
    "RabbitMQFailurePolicy",
    "RabbitMQPublish",
    "RabbitMQPublisher",
]
