# API Reference

Stratae is organized around three core tools: dependency injection, lifecycle management, and events. Some additional supporting modules are also included to provide additional utility.

## Core

### Dependency Injection

Mark a parameter with `Annotated[T, Depends(provider)]` and decorate the function with `inject`: the parameter is resolved by calling its provider at call time. Supports sync and async providers, and temporary overrides for testing.

````{example} Injecting a provider and overriding it
```{code-block} python
from dataclasses import dataclass
from typing import Annotated
from stratae.depends import Depends, inject, override

@dataclass
class User:
    name: str

def get_current_user() -> User:
    return User(name="anonymous")

@inject
def greeting(user: Annotated[User, Depends(get_current_user)]) -> str:
    return f"Welcome, {user.name}!"

print(greeting())

with override(get_current_user).to(User(name="Jane")):
    print(greeting())

print(greeting())
```
```{output}
Welcome, anonymous!
Welcome, Jane!
Welcome, anonymous!
```
````

{doc}`Full reference <apidocs/stratae.depends/stratae.depends>`

### Lifecycle

Manage hierarchical, scoped contexts for caching and cleanup. `Scope`/`AsyncScope` describe the blocks and configuration directly - no separate container to build. A cached generator function is a resource: its yielded value is cached, and the code after its `yield` runs when the scope ends.

````{example} Committing a transaction, or rolling back if the scope raises
```{code-block} python
from stratae.lifecycle import Scope

class Connection:
    def commit(self):
        print("committing")

    def rollback(self):
        print("rolling back")

    def close(self):
        print("closing")

request = Scope("request")

@request.cache()
def get_transaction():
    conn = Connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

with request:
    conn = get_transaction()
    # ... do work using conn ...

try:
    with request:
        conn = get_transaction()
        raise ValueError("payment failed")
except ValueError:
    pass
```
```{output}
committing
closing
rolling back
closing
```
````

{doc}`Full reference <apidocs/stratae.lifecycle/stratae.lifecycle>`

### Events

Event definitions, bound-event facades, and dispatch protocols for pub/sub and request/reply messaging.

````{example} Dispatching a pub/sub event through an in-process bus
```{code-block} python
from stratae.events import DirectBus, PubSub

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_placed_event = PubSub(OrderPlaced)

bus = DirectBus()
place_order = bus.bind(order_placed_event, factory=OrderPlaced)

@bus.handle(order_placed_event)
def notify_shipping(order: OrderPlaced) -> None:
    print(f"shipping notified for order {order.order_id}")

place_order(order_id=42)
```
```{output}
shipping notified for order 42
```
````

{doc}`Full reference <apidocs/stratae.events/stratae.events>`

## Supporting modules

### Context

Callable, injectable values backed by `contextvars`. Set once with `.set()` or `with ctx.use(value):` and read anywhere within that scope. It is usable directly as a `Depends()` provider.

````{example} Impersonating a customer, then reverting to the agent's session
```{code-block} python
from stratae.context import Context

user_id = Context[int]("user_id")

with user_id.use(42):  # the support agent's own account
    print(f"acting as user {user_id()}")

    # "View as customer" temporarily impersonates the customer
    # to reproduce a bug, then reverts to the agent's session.
    with user_id.use(7):
        print(f"acting as user {user_id()}")

    print(f"acting as user {user_id()}")
```
```{output}
acting as user 42
acting as user 7
acting as user 42
```
````

{doc}`Full reference <apidocs/stratae.context/stratae.context>`

### Middleware

One contract for middleware on lifecycle scopes and event buses: context managers entered around each activation or call, which can observe an error but not swallow it.

````{example} One middleware around a scope's activations and a bus's emits
```{code-block} python
from contextlib import contextmanager
from stratae.events import DirectBus, Middleware, PubSub
from stratae.lifecycle import Scope

@contextmanager
def logged(wrapped):
    name = type(wrapped).__name__
    print(f"enter {name}")
    try:
        yield
    finally:
        print(f"exit {name}")

class Ping:
    pass

ping = PubSub(Ping)
work = Scope("work", middleware=(logged,))
bus = DirectBus(middleware=Middleware(publish=(logged,)))

with work:
    bus.emit(ping, None, Ping())
```
```{output}
enter Scope
enter PublishCall
exit PublishCall
exit Scope
```
````

{doc}`Full reference <apidocs/stratae.middleware/stratae.middleware>`

### Checks

Run collections of zero-argument checks, raising or gathering their failures: `check`, `check_async`, and the `require` decorator.

````{example} Rejecting an action that fails a check
```{code-block} python
from types import SimpleNamespace
from stratae.checks import require

user = SimpleNamespace(id=1, is_admin=False)

def is_admin():
    assert user.is_admin, "not an admin"

@require(is_admin)
def delete_account(account_id: int):
    print(f"deleting account {account_id}")

try:
    delete_account(24)
except AssertionError as exc:
    print(f"access denied: {exc}")
```
```{output}
access denied: not an admin
```
````

{doc}`Full reference <apidocs/stratae.checks/stratae.checks>`

### Callables

Tell whether calling something gives a coroutine, with `is_async_callable`. Stratae decides this the same way everywhere it treats sync and async callables differently. Libraries built on Stratae can use it too.

````{example} Telling sync and async callables apart
```{code-block} python
from functools import partial
from stratae.callables import is_async_callable

class Client:
    async def __call__(self) -> str:
        return "sent"

async def send(to: str) -> str:
    return f"sent to {to}"

print(is_async_callable(partial(send, "ops")))
print(is_async_callable(Client()))
print(is_async_callable(Client))
```
```{output}
True
True
False
```
````

{doc}`Full reference <apidocs/stratae.callables/stratae.callables>`

### Integrations

Bridges between Stratae and third-party tools. FastAPI and Starlette get lifecycle-scoped routes. RabbitMQ gets async publish and consume adapters. OpenTelemetry traces emits and handlers across them.

````{example} Publishing a msgspec.Struct event to RabbitMQ, then consuming it back
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
        consumer,
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

        await asyncio.sleep(0.05)

asyncio.run(main())
```
```{output}
received order 42
```
````

{doc}`Full reference <apidocs/stratae.integrations/stratae.integrations>`

```{toctree}
:hidden:

depends <apidocs/stratae.depends/stratae.depends>
lifecycle <apidocs/stratae.lifecycle/stratae.lifecycle>
events <apidocs/stratae.events/stratae.events>
checks <apidocs/stratae.checks/stratae.checks>
callables <apidocs/stratae.callables/stratae.callables>
context <apidocs/stratae.context/stratae.context>
middleware <apidocs/stratae.middleware/stratae.middleware>
integrations <apidocs/stratae.integrations/stratae.integrations>
```
