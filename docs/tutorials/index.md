# Tutorials

```{toctree}
:maxdepth: 1
:hidden:
getting-started
walkthrough
dependency-injection
lifecycle
events
context
checks
serde
integrations/index
```

New to Stratae? See [Getting Started](getting-started) for installation and examples, then the [Project Walkthrough](walkthrough) for a working example that grows from a simple script into one using each piece of Stratae.

## The core

[Dependency Injection](dependency-injection), [Lifecycle](lifecycle.md), and [Events](events.md) are the core elements and are fully independent. Read whichever one matches the problem you have. Each tutorial shows how it combines with the other two.

### Dependency Injection

[Dependency Injection](dependency-injection) wires a function's parameters to provider callables, resolved at call time. Mark a parameter with `Annotated[T, Depends(provider)]` and decorate the function with `@inject`. Callers never see or pass that parameter; it's resolved automatically.

````{example} Injecting a tax rate provider
```{code-block} python
from typing import Annotated
from stratae.depends import Depends, inject

def get_tax_rate() -> float:
    return 0.08

@inject
def total_with_tax(
    subtotal: float, tax_rate: Annotated[float, Depends(get_tax_rate)]
) -> float:
    return subtotal + subtotal * tax_rate

print(total_with_tax(100.0))
```
```{output}
108.0
```
````

### Lifecycle

[Lifecycle](lifecycle.md) scopes caching and cleanup to a unit of work, such as a request, a background job, or an application run. `.cache` registers a function to run once and be reused for the life of an active scope. Wrap a generator in `resource` to also clean it up automatically: the code after `yield` runs when the scope exits, not when the function returns.

````{example} Caching and cleaning up a batch within a scope
```{code-block} python
from stratae.lifecycle import Scope, resource

batch = Scope("batch")

@batch.cache()
@resource
def get_batch():
    print("opening batch")
    items: list[str] = []
    try:
        yield items
    finally:
        print(f"processed {len(items)} items: {items}")

with batch.activate():
    get_batch().append("order-1")
    get_batch().append("order-2")
```
```{output}
opening batch
processed 2 items: ['order-1', 'order-2']
```
````

### Events

[Events](events.md) decouples code that produces an event from whatever handles it. Pair a payload schema with a dispatch pattern using `Event(...)`: `PubSub` for fire-and-forget, `Request[Reply]` for exactly one responder. Bind the result to a bus, then register handlers separately from wherever the event gets triggered.

````{example} Publishing and handling an order event
```{code-block} python
from stratae.events import DirectBus, Event, PubSub

class OrderPlaced:
    def __init__(self, order_id: str) -> None:
        self.order_id = order_id

order_placed = Event(PubSub, OrderPlaced)

bus = DirectBus()
notify_order_placed = bus.bind(order_placed, factory=OrderPlaced)

@bus.handle(order_placed)
def send_confirmation_email(event: OrderPlaced) -> None:
    print(f"Confirmation sent for order {event.order_id}")

notify_order_placed(order_id="A1001")
```
```{output}
Confirmation sent for order A1001
```
````

## Utilities

Standalone, no dependency on the core three or on each other:

- [Context](context.md) carries per-task state through a call stack without passing it explicitly. It's often used as a Dependency Injection provider itself, e.g. injecting the current user or tenant from a context-backed value.
- [Checks](checks.md) runs a set of preconditions and raises if they don't hold. It's a natural fit for a Dependency Injection provider that exists only for its side effect (raising), not a return value. However, checks are useful well beyond that.
- [Serde](serde.md) turns arbitrary objects into bytes and back. Particularly useful alongside Events integrations, where a payload has to cross a process boundary such as over RabbitMQ.

## Integrations

[Integrations](integrations/index) bridges the core modules to specific third-party tools:

- [FastAPI](integrations/fastapi) — `scoped_route` builds an `APIRoute` subclass that activates an `AsyncScope` around each request.
- [Starlette](integrations/starlette) — the same `scoped_route` pattern, built on Starlette's `Route`.
- [RabbitMQ](integrations/rabbitmq) implements `stratae.events`' bus protocols over AMQP.
- [msgspec](integrations/msgspec) registers a fast path onto `stratae.serde.pack` for `msgspec.Struct` payloads.

## Looking for something specific?

:::{dropdown} I want to improve testability.
Use [Dependency Injection](dependency-injection) and `override`/`overrides` to swap in a mock for the duration of a `with` block, without restructuring the function it's injected into.
:::

:::{dropdown} I need authorization guards on business logic or an HTTP endpoint.
Use [Checks](checks.md)'s `require` decorator, or a check as a Dependency Injection provider, to guard any callable with permission checks that raise before it runs.
:::

:::{dropdown} I need to trigger several side effects (an email, a cache invalidation, an analytics event) without hard-coding them together.
Use [Events](events.md)'s `PubSub` dispatch pattern: define the event once, then register each side effect as an independent handler.
:::

:::{dropdown} I need a real broker in production, but an in-memory stand-in locally.
[Events](events.md) buses are swappable by design. Bind to `DirectBus` locally or in tests, and to [RabbitMQ](integrations/rabbitmq) in production, with no change to the event definitions or handlers.
:::

:::{dropdown} I need a connection to close automatically, even on error.
Use [Lifecycle](lifecycle.md)'s `resource` to wrap a generator so the cleanup code after `yield` runs when the scope exits, regardless of how it ended.
:::

:::{dropdown} I need a different implementation depending on the user, tenant, environment, or feature flag.
Use a Dependency Injection provider that returns the correct implementation, or use [Context](context.md) as a shortcut to set it explicitly. Either way, the routing logic lives in one place instead of scattered through business logic.
:::
