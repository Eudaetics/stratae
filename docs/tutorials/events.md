# Events

`stratae.events` separates *what a message is* from *how it gets delivered*. An event definition — its payload and whether it expects a reply — is bus-agnostic; the same definition can be dispatched in-process for tests or over a real broker in production, with the bus swapped and nothing else.

## Defining an event

An event is a factory (usually a class) paired with a dispatch pattern:

```python
from stratae.events import event, PubSub, Request


class BookCreated:
    def __init__(self, title: str, author: str) -> None:
        self.title = title
        self.author = author


class Book:
    def __init__(self, title: str, author: str) -> None:
        self.title = title
        self.author = author


class BookQuery:
    def __init__(self, title: str) -> None:
        self.title = title


book_created = event(BookCreated, PubSub)
book_query = event(BookQuery, Request[Book])
```

`PubSub` is fire-and-forget: any number of handlers, no reply. `Request[Reply]` expects exactly one handler ("responder") and blocks for its return value — it must be subscripted with the reply type, since there's no other way to recover it at runtime. `event(...)` infers the payload type from `factory` when it's a plain class; for a plain function factory, or anything async, pass `payload_type` explicitly.

## Wiring up a bus

`DirectBus` (and its async counterpart `AsyncDirectBus`) dispatches in-process, with no broker involved — the batteries-included adapter for tests and single-process apps. `.bind(event)` turns an event definition into a callable, so producers don't construct payloads or talk to the bus directly:

```python
from stratae.events import DirectBus

bus = DirectBus()
catalog: dict[str, Book] = {}

create_book = bus.bind(book_created)
query_book = bus.bind(book_query)


@bus.handle(book_created)
def _(event: BookCreated) -> None:
    catalog[event.title] = Book(event.title, event.author)


@bus.handle(book_query)
def _(query: BookQuery) -> Book:
    return catalog[query.title]


create_book(title="Dune", author="Frank Herbert")
query_book(title="Dune")  # -> Book("Dune", "Frank Herbert")
```

A `Request` event needs exactly one registered responder at emit time: zero raises `NoResponderError`, more than one raises `MultipleRespondersError`. `PubSub` handlers all run regardless — if one fails, the others still do, and their failures are collected into one `ExceptionGroup` rather than stopping at the first.

## Composing with dependency injection and lifecycle

A handler is just a plain callable, so an `@inject`-decorated function works as one directly — the bus doesn't need to know anything about DI:

```python
from stratae.depends import Depends, Injected, inject
from stratae.lifecycle import Lifecycle, Scope

lifecycle = Lifecycle([Scope("application", isolation="shared")])
bus = DirectBus()


@lifecycle.cache("application")
def order_store() -> dict[int, dict]:
    return {}


order_placed = event(OrderPlaced, PubSub)
price_order = event(PriceOrder, Request[Quote])

place_order = bus.bind(order_placed)
request_quote = bus.bind(price_order)


@bus.handle(order_placed)
@inject
def _(order: OrderPlaced, store: Injected[dict, Depends(order_store)]) -> None:
    store[order.order_id] = {"status": "placed"}


@bus.handle(price_order)
@inject
def _(request: PriceOrder, store: Injected[dict, Depends(order_store)]) -> Quote:
    return Quote(order_id=request.order_id, total=100)


with lifecycle.start("application"):
    place_order(order_id=42)
    request_quote(order_id=42)
```

`@bus.handle(...)` goes on the outside, `@inject` on the inside — the bus registers the injectable wrapper, and calls it with just the payload; `@inject` resolves the rest. Both handlers share the same `order_store`, cached once per `lifecycle.start("application")` activation — pub/sub and request/reply handlers can share scoped state with no events-specific plumbing at all. See the [Dependency Injection](dependency-injection) and [Lifecycle](lifecycle) guides for the pieces this is built from.

## Correlation with Envelope

An `Envelope` is a small record — `message_id`, `correlation_id`, `causation_id`, `timestamp` — that tracks which message caused which. Pass `use_envelope=True` to a bus and every `emit` opens a child of whatever envelope is currently active (or mints a fresh root if none is), so nested emissions inside a handler automatically chain:

```python
bus = DirectBus(use_envelope=True)


@bus.handle(order_placed)
def _(order: OrderPlaced) -> None:
    log_message(text=f"order {order.order_id} placed")  # a nested emit --
    # its envelope is a child of order_placed's
```

For pure in-process dispatch this is optional overhead you can skip. It earns its keep once a message crosses a real transport — see the [RabbitMQ integration](integrations/rabbitmq), which stamps envelopes onto the wire so correlation survives a publish/consume hop.

## Async

`AsyncDirectBus` accepts a mix of sync and async handlers on the same event — `PubSub` handlers run concurrently via `asyncio.gather`; `Request` dispatch awaits the responder if it's async, calls it directly otherwise. `abind` (the async counterpart to `bind`) accepts a sync or async factory either way, awaiting it only if needed.

```python
from stratae.events import AsyncDirectBus

bus = AsyncDirectBus()


@bus.handle(book_created)
async def _(event: BookCreated) -> None:
    await db.insert(event)


@bus.handle(book_created)
def _(event: BookCreated) -> None:
    logger.info("book created: %s", event.title)


await create_book(
    title="Dune", author="Frank Herbert"
)  # both handlers run, concurrently where async
```

## Errors

| Exception | Raised when |
|---|---|
| `NoResponderError` | A `Request` event is emitted with zero registered responders |
| `MultipleRespondersError` | A `Request` event has more than one registered responder |
| `NotConnectedError` | A transport adapter (e.g. RabbitMQ) is used before its connection is open — `DirectBus` never raises this |

`bind`/`BoundEvent` require a sync factory (`TypeError` otherwise, since a sync `__call__` can't await); use `abind`/`AsyncBoundEvent` for async factories. `DirectBus` similarly rejects async handlers outright — register those on `AsyncDirectBus`.

Full signatures and every exported name: {doc}`stratae.events API reference <../apidocs/stratae.events/stratae.events>`.
