# Building a service

The other tutorials each take one part of Stratae on its own. This one puts them together in one small service, from the HTTP request to the consumer on the other side of a broker. It places orders over HTTP and ships them from a queue. Along the way, it shows where each scope begins and ends. It shows how storage and the broker are injected, and how hooks and middleware divide the cross-cutting work. And it shows how one correlation id and one trace follow an order the whole way.

```{motivation}
Each part of a service is simple on its own. The difficult part is the seams between them. A request's database transaction must commit before its response is sent. An event about an order must be published if, and only if, the order was saved. A message must be acked only after the work it caused has committed, and retried if that work failed. When something goes wrong, the logs and traces from the request, the relay and the consumer must be found together. Stratae's pieces are built to meet at these seams, and this guide shows each one.
```

The service does this:

1. `POST /orders` sends a `PlaceOrder` command to an in-process bus.
2. The command's handler saves the order, and writes an `OrderPlaced` event to the outbox, in the same unit of work.
3. The outbox relay publishes the event to RabbitMQ once the unit of work has committed.
4. A consumer ships the order, in a unit of work of its own, retrying if the warehouse is down.

It uses SQLAlchemy for storage, Starlette for HTTP, RabbitMQ as the broker, and OpenTelemetry for tracing. Install Stratae with the extras it needs:

```bash
uv add "stratae[rabbitmq,starlette,opentelemetry]" "sqlalchemy[asyncio]" aiosqlite opentelemetry-sdk opentelemetry-instrumentation-asgi
```

## Scopes and storage

A service has a few kinds of unit of work, and each is a [scope](../apidocs/stratae.lifecycle/stratae.lifecycle). `application` lasts as long as the process, and holds what's shared: the database engine. `request` lasts for one HTTP request. `work` is one unit of work, with one database transaction. It's used by a request's handler, and by each message the consumer handles.

`request` holds per-request values, such as the current user. This guide caches nothing in it. It's still needed, because the HTTP route activates `work` inside it. That lets `work` close, and commit, before the response is sent. `request` lasts until the response is done.

The session commits when its `work` scope closes cleanly, and rolls back when it closes with an error. That's the recipe from [A database session per scope](database.md). The outbox records go in the same unit of work, so an order and its event are kept or dropped together. This guide uses the in-memory outbox store to stay short. Its `transaction()` keeps records only if the block exits cleanly, as a database transaction would. It stages records only while a session is open, though. A record written with no session open is kept at once. A real service keeps them in the database, with the store from [An outbox store on SQLAlchemy](outbox.md).

````{example} The scopes, the database, and a session and outbox transaction per unit of work
```{code-block} python
import asyncio
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from typing import Annotated, Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

from stratae.depends import Depends, inject
from stratae.events.outbox import InMemoryOutboxStore
from stratae.lifecycle import AsyncScope, async_resource

application = AsyncScope("application", isolation="shared")
request = AsyncScope("request", requires=application)
work = AsyncScope("work", requires=application)

class Base(DeclarativeBase):
    pass

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    item: Mapped[str]
    shipped: Mapped[bool] = mapped_column(default=False)

class AuditEntry(Base):
    __tablename__ = "audit"
    id: Mapped[int] = mapped_column(primary_key=True)
    entry: Mapped[str]

outbox_store = InMemoryOutboxStore()

@application.cache()
@async_resource
async def get_engine() -> AsyncGenerator[AsyncEngine, None]:
    # In-memory SQLite for the guide; a real service passes its database URL.
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield engine
    finally:
        await engine.dispose()

@application.cache()
async def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(await get_engine(), expire_on_commit=False)

@work.cache()
@async_resource
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with (await get_sessionmaker())() as session, outbox_store.transaction():
        try:
            yield session
        except BaseException:
            await session.rollback()
            raise
        await session.commit()

type SessionDep = Annotated[AsyncSession, Depends(get_session)]
```
````

Nothing here knows about HTTP or RabbitMQ. A function asks for a `SessionDep`, and gets the session of whatever unit of work is active when it's called.

## Tracing

Tracing is the same for every bus, so it's one [middleware](../apidocs/stratae.events/stratae.events.middleware), shared by all of them. [`tracing`](../apidocs/stratae.integrations/stratae.integrations.opentelemetry) opens a span around each emit and each handler call. It also carries the trace context in each message's headers, so the consumer's spans join the trace that published the message. The guide collects spans in memory, so it can print them. A real service configures an exporter instead, and passes nothing to `tracing`, which then uses the global tracer provider.

````{example} A tracer provider, and middleware that traces every bus
```{code-block} python
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from stratae.integrations.opentelemetry import tracing

exporter = InMemorySpanExporter()
tracer_provider = TracerProvider()
tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
traced = tracing(tracer_provider)
```
````

## Commands and events

The command and the event are [events](../apidocs/stratae.events/stratae.events) of two patterns. `PlaceOrder` is a `Request`: it has exactly one handler, which replies. `OrderPlaced` is a `PubSub` event: it tells whoever is interested, and expects nothing back. Its payload crosses the broker, so it's registered with [serde](serde.md) to encode and decode.

The command goes through an `AsyncDirectBus`, in-process. With `use_envelope=True`, each command gets an [envelope](../apidocs/stratae.events/stratae.events.envelope), a child of the request's. The event goes to the outbox. Its record gets an envelope of its own, a child of the command's. So the request, the command and the event share one correlation id.

Hooks and middleware both run around a call, but they suit different work. Middleware runs around every event on a bus, like the tracing. A hook belongs to one event, or one handler, and is typed for it. Here, a `complete` hook counts how each command ended, as a metric would. It counts handled commands. A command whose handler returned, but whose commit then failed, still counts as `ok`.

A `success`, `failure` or `complete` hook watches a call, and can't change its outcome. If one raises, the error is logged, and the call's result stands. Only a `before` hook can stop a call, before it runs. Observing hooks on an async bus also run concurrently. So anything that must commit with the order belongs in the handler, not a hook. The handler writes the order's audit entry itself.

````{example} The command, the event, and the handler that saves an order and announces it
```{code-block} python
from collections import Counter

from stratae.events import AsyncDirectBus, Envelope, Hooks, PubSub, Request
from stratae.events.outbox import OutboxConfig, OutboxProducer
from stratae.integrations.rabbitmq import RabbitMQConfig
from stratae.serde import deserialize, encode, serialize

@dataclass
class PlaceOrder:
    item: str

@dataclass
class OrderPlaced:
    order_id: int
    item: str

@encode.register
def encode_order_placed(event: OrderPlaced) -> dict[str, Any]:
    return asdict(event)

@deserialize.register(OrderPlaced)
def decode_order_placed(value: dict[str, Any]) -> OrderPlaced:
    return OrderPlaced(**value)

place_order = Request(PlaceOrder, reply=OrderPlaced)
order_placed = PubSub(OrderPlaced)

commands = AsyncDirectBus(use_envelope=True, middleware=traced)
outbox = OutboxProducer(outbox_store, serializer=serialize, middleware=traced)
announce = outbox.bind(
    order_placed,
    config=OutboxConfig(RabbitMQConfig("orders", "order.placed", exchange_type="topic")),
)

handled: Counter[str] = Counter()

def count_outcome(event: Any, command: PlaceOrder, error: BaseException | None) -> None:
    handled["failed" if error else "ok"] += 1

@commands.handle(place_order, hooks=Hooks(complete=(count_outcome,)))
@inject
async def handle_place_order(command: PlaceOrder, session: SessionDep) -> OrderPlaced:
    order = Order(item=command.item)
    session.add(order)
    await session.flush()
    session.add(AuditEntry(entry=f"order {order.id} placed: {order.item}"))
    placed = OrderPlaced(order_id=order.id, item=order.item)
    await announce(placed)
    return placed
```
````

The handler doesn't commit, and doesn't publish. It adds an order, its audit entry and an outbox record to its unit of work, and the unit decides. If anything fails before the commit, all three roll back together. Nothing is ever announced, or audited, about an order that wasn't saved.

## The broker

The publisher and the consumer are created now, and connect later, when the service starts. The outbox relay publishes committed records through the publisher. If the broker is down, the records wait in the outbox, and the relay retries once it's back.

The consumer handles each message in its own `work` scope. So the shipping handler gets a session of its own, which commits before the message is acked. If the handler fails, the [failure policy](../apidocs/stratae.integrations/stratae.integrations.rabbitmq) retries the message after one second, then ten, then sixty. After that, it dead-letters the message. To show a retry, the handler here fails its first attempt, as if the warehouse were down.

````{example} The publisher, the relay, and a consumer that ships orders, with retries
```{code-block} python
from stratae.events.outbox import OutboxRelay
from stratae.integrations.rabbitmq import (
    RabbitMQConsumeConfig,
    RabbitMQConsumer,
    RabbitMQFailurePolicy,
    RabbitMQPublisher,
)

BROKER_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost/")

publisher = RabbitMQPublisher(BROKER_URL, middleware=traced)
relay = OutboxRelay(outbox_store, publisher, outbox)
consumer = RabbitMQConsumer(BROKER_URL, scope=work, middleware=traced)

shipping = RabbitMQConsumeConfig(
    "orders.shipping",
    exchange="orders",
    binding_key="order.placed",
    exchange_type="topic",
    on_failure=RabbitMQFailurePolicy(retry_delays=(1, 10, 60)),
)

attempts: list[int] = []
shipped = asyncio.Event()

@consumer.handle(order_placed, config=shipping)
@inject
async def ship_order(placed: OrderPlaced, session: SessionDep) -> None:
    attempts.append(placed.order_id)
    if len(attempts) == 1:
        raise ConnectionError("warehouse unavailable")
    order = await session.get(Order, placed.order_id)
    assert order is not None
    order.shipped = True
    print(f"shipped order {placed.order_id} on attempt {len(attempts)}")
    shipped.set()
```
````

## The HTTP app

The route is built with [`scoped_route`](../apidocs/stratae.integrations/stratae.integrations.starlette). It activates `request` for each request, and `work` around the endpoint, so the transaction commits before the response is sent. A commit that fails becomes a 500, not a lost write behind a success. The endpoint emits the command with `commands.emit`. Its second argument is the routing config, which a broker needs and a direct bus doesn't, so it's `None`.

Two ASGI middleware wrap the app. [`EnvelopeMiddleware`](../apidocs/stratae.integrations/stratae.integrations.asgi) opens an envelope for each request. It joins the caller's correlation id, if they sent one, and returns it in the response. OpenTelemetry's ASGI middleware opens the request's server span, which the bus's spans join. The lifespan connects everything when the service starts, and closes it, in reverse, when it stops. The consumer starts before the publisher, so its queue exists before anything is published to it.

````{example} The route, the app, and its lifespan
```{code-block} python
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request as HTTPRequest
from starlette.responses import JSONResponse

from stratae.integrations.asgi import EnvelopeMiddleware
from stratae.integrations.starlette import scoped_route

async def post_order(http_request: HTTPRequest) -> JSONResponse:
    command = PlaceOrder(**await http_request.json())
    placed = await commands.emit(place_order, None, command)
    return JSONResponse(asdict(placed), status_code=201)

@asynccontextmanager
async def lifespan(app: Starlette) -> AsyncGenerator[None, None]:
    async with application, consumer, publisher, relay:
        yield

ScopedRoute = scoped_route(request, handler=work)
app = Starlette(
    lifespan=lifespan,
    routes=[ScopedRoute("/orders", post_order, methods=["POST"])],
    middleware=[
        Middleware(EnvelopeMiddleware),
        Middleware(OpenTelemetryMiddleware, tracer_provider=tracer_provider),
    ],
)
```
````

In production, the consumer often runs in a process of its own, so a slow queue doesn't hold up requests. It needs only the `application` and `work` scopes, the database, and the consumer. The request scope and the app stay with the web process.

## Trying it without a broker

Everything up to the publish runs in-process. So the request, the command, the unit of work and the outbox can be tried without RabbitMQ. This runs one request against the app, shows the hook's count, and reads the committed audit log. Then it relays the outbox once, to a publisher that prints instead of publishing. The record carries the request's correlation id.

````{example} Placing an order, and relaying it to a printing publisher
```{code-block} python
from collections.abc import Mapping
from uuid import UUID

import httpx2
from sqlalchemy import select

from stratae.events.outbox import OutboxPublisher

class PrintingPublisher(OutboxPublisher):
    """Stands in for RabbitMQPublisher: the relay calls `emit` the same way."""

    def __init__(self) -> None:
        self.envelope: Envelope | None = None

    async def emit(
        self,
        event: Any,
        config: RabbitMQConfig,
        payload: bytes,
        *,
        serializer: Any = None,
        envelope: Envelope | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        # The relay passes the record's body, already encoded, as the payload.
        self.envelope = envelope
        print(f"published {payload!r} to {config.exchange}:{config.routing_key}")

@inject
async def audit_log(session: SessionDep) -> list[str]:
    return list(await session.scalars(select(AuditEntry.entry)))

async def try_it() -> None:
    printer = PrintingPublisher()
    async with application:
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://orders") as client:
            response = await client.post("/orders", json={"item": "lamp"})
        print(response.status_code, response.json())
        print("handled:", dict(handled))
        async with work:
            print("audit log:", await audit_log())
        await OutboxRelay(outbox_store, printer, outbox).run_once()
    chain = UUID(response.headers["x-correlation-id"])
    assert printer.envelope is not None
    print("same correlation id:", printer.envelope.correlation_id == chain)

asyncio.run(try_it())
```
```{output}
201 {'order_id': 1, 'item': 'lamp'}
handled: {'ok': 1}
audit log: ['order 1 placed: lamp']
published b'{"order_id": 1, "item": "lamp"}' to orders:order.placed
same correlation id: True
```
````

The relay gives the publisher the record's body as it was encoded, and the record's own envelope. A publisher sends them as they are, so a relayed message keeps the message id it was written with, and consumers can deduplicate on it.

## Running it with RabbitMQ

With a broker running, the lifespan connects the consumer, the publisher and the relay. Then the whole path runs. The relay polls the outbox in the background. Here it's woken after the request, as a service might after a commit, so it doesn't wait for its next poll. The first attempt to ship fails, and is logged as a warning. The retry ships the order a second later.

Then the guide prints the spans as a tree, by parent. It leaves out the `http send` and `http receive` spans that OpenTelemetry's ASGI middleware adds, to keep the tree short. The bus's and the outbox's spans are children of the request's server span. Both attempts to ship are children of the outbox write, in the request's trace. That holds even though they ran in the consumer, after a broker round trip. The relay's publish ran later, outside the request. So it's in a trace of its own, and links back to the write.

<!--- skip: next -->
````{example} Placing an order, and following it to the consumer
```{code-block} python
from opentelemetry.sdk.trace import ReadableSpan

def span_id(span: ReadableSpan) -> int:
    context = span.get_span_context()
    assert context is not None
    return context.span_id

def show(spans: list[ReadableSpan]) -> None:
    names = {span_id(span): span.name for span in spans}
    children: dict[int | None, list[ReadableSpan]] = {}
    for span in sorted(spans, key=lambda span: span.start_time or 0):
        parent = span.parent.span_id if span.parent else None
        children.setdefault(parent if parent in names else None, []).append(span)

    def branch(parent: int | None, depth: int) -> None:
        for span in children.get(parent, []):
            links = "".join(f", linked to {names[link.context.span_id]}" for link in span.links)
            print(f"{'  ' * depth}{span.name} ({span.kind.name}){links}")
            branch(span_id(span), depth + 1)

    branch(None, 0)

async def main() -> None:
    exporter.clear()
    async with lifespan(app):
        transport = httpx2.ASGITransport(app=app)
        async with httpx2.AsyncClient(transport=transport, base_url="http://orders") as client:
            response = await client.post("/orders", json={"item": "anvil"})
        print(response.status_code, response.json())
        relay.wake()
        async with asyncio.timeout(10):
            await shipped.wait()
    spans = [s for s in exporter.get_finished_spans() if not s.name.endswith(("send", "receive"))]
    show(spans)

asyncio.run(main())
```
```{output}
201 {'order_id': 1, 'item': 'anvil'}
shipped order 1 on attempt 2
POST /orders (SERVER)
  emit PlaceOrder (INTERNAL)
    handle PlaceOrder (INTERNAL)
      create orders:order.placed (PRODUCER)
        process orders:orders.shipping (CONSUMER)
        process orders:orders.shipping (CONSUMER)
publish orders:order.placed (CLIENT), linked to create orders:order.placed
```
````

The consumer's handler ran with the request's correlation id too, since each message carries its envelope. A log filter reading `Envelope.current()`, like the one in the [ASGI integration](../apidocs/stratae.integrations/stratae.integrations.asgi)'s docs, puts that id on every log line, so the request's and the consumer's logs can be found together.

## Next steps

- Keep the outbox in the database, with the store from [An outbox store on SQLAlchemy](outbox.md). Then several relays can share it.
- Export spans with an OpenTelemetry exporter, and drop the in-memory one. `tracing()` with no argument uses the global tracer provider.
- Check a request's permissions before its handler runs, with [checks](checks.md).
- Put per-request values, such as the current user, in a [context](context.md), where providers and the session's setup can read them.
