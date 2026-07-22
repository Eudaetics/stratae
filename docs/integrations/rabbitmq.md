# RabbitMQ

`stratae.integrations.rabbitmq` implements `stratae.events`' `Producer`/`Consumer` protocols over AMQP (via `aiormq`), so the same event definitions used with `DirectBus` in the [Events guide](../events) work unchanged against a real broker — only the bus changes.

## Publishing

`RabbitMQPublisher` is an async context manager that owns a connection and channel:

```python
from stratae.integrations.rabbitmq import RabbitMQPublisher, RabbitMQConfig

async with RabbitMQPublisher("amqp://guest:guest@localhost/") as publisher:
    place_order = publisher.bind(
        order_placed,
        config=RabbitMQConfig(exchange="events", routing_key="order.placed", exchange_type="topic"),
    )
    await place_order(order_id=42)
```

`.bind(event, config=...)` returns the same kind of callable `bus.bind`/`abind` return elsewhere in `stratae.events` — `publisher.emit` is just the underlying `Producer`. `RabbitMQConfig` pairs an exchange and routing key; give it `exchange_type` to have the exchange declared automatically before the first publish (skip it if the exchange is already declared elsewhere). Using the publisher before entering its `async with` block raises `NotConnectedError`.

## Consuming: work queues vs. fan-out

`RabbitMQConsumeConfig` has two modes, chosen by whether you give it an `exchange`:

- **Queue mode** (`queue`, no `exchange`) — declares a named, durable queue and consumes it directly. Handlers registered on the *same* queue compete for messages, round-robin — a work queue, not a fan-out.
- **Subscriber mode** (`exchange` given) — declares the exchange and binds a queue to it by `binding_key`. Omit `queue` for an ephemeral, exclusive, auto-deleted queue (true pub/sub: one queue per subscriber); name it for a durable, shared subscription instead.

```python
from stratae.integrations.rabbitmq import RabbitMQConsumer, RabbitMQConsumeConfig

consumer = RabbitMQConsumer("amqp://guest:guest@localhost/", prefetch_count=1)

@consumer.handle(
    order_placed,
    config=RabbitMQConsumeConfig(exchange="events", binding_key="order.*"),
)
def on_order(payload: OrderPlaced) -> None:
    print(f"order {payload.order_id} placed")

async with consumer:
    ...  # consumes in the background until the context exits
```

`.handle(...)` accepts sync or async handlers, works as a decorator or a direct call, and returns a `Handler` you keep if you plan to `.remove()` it later. Each registration opens its own AMQP consumer — fan-out comes from the queue/exchange topology, not from how many handlers you register.

`prefetch_count=1` is the standard way to get fair round-robin dispatch among competing consumers in queue mode; leave it `None` for the broker's default.

## Failure handling: no built-in retry

If deserialization or the handler raises, the message is logged and `nack`'d with `requeue=False` — dropped, not redelivered. There's no retry or dead-letter behavior by default; if you need failed messages preserved somewhere, configure a dead-letter exchange via `RabbitMQConsumeConfig`'s `arguments`. This is a deliberate choice to keep a broken handler from wedging a queue, not an oversight — plan for it explicitly rather than assuming messages come back.

## Correlation across the wire

Every publish stamps AMQP headers with an `Envelope` — a child of whatever envelope is currently active, or a fresh root if none is. Every delivery reconstructs that envelope from headers and reopens it as active for the duration of the handler, so `correlation_id` threads automatically through a publish → consume → (handler publishes again) → consume chain, with no manual plumbing:

```python
from stratae.events import Envelope

@consumer.handle(order_placed, config=RabbitMQConsumeConfig(queue="orders"))
async def on_order(payload: OrderPlaced) -> None:
    print(Envelope.current().correlation_id)  # same id the publisher's envelope carried
```

If headers are partially present, missing ids are minted and logged at `info`; if they're malformed, a fresh envelope is scoped instead and logged as a `warning` — a delivery with bad headers degrades rather than fails.

See the [Events guide](../events#correlation-with-envelope) for what `Envelope` is doing conceptually, independent of any transport.

Full signatures: {doc}`stratae.integrations.rabbitmq API reference <../apidocs/stratae.integrations/stratae.integrations.rabbitmq>`.
