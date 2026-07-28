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

with override(get_current_user, User(name="Jane")):
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

Manage hierarchical, scoped contexts for caching and cleanup. `Lifecycle`/`AsyncLifecycle`, `Scope` describe the blocks and configuration. Use `resource`/`async_resource` for cached resources that need teardown.

````{example} Committing a transaction, or rolling back if the scope raises
```{code-block} python
from stratae.lifecycle import Lifecycle, Scope, resource

class Connection:
    def commit(self):
        print("committing")

    def rollback(self):
        print("rolling back")

    def close(self):
        print("closing")

lifecycle = Lifecycle([Scope("request", "context")])

@lifecycle.cache("request")
@resource
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

with lifecycle.start("request"):
    conn = get_transaction()
    # ... do work using conn ...

try:
    with lifecycle.start("request"):
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
from stratae.events import DirectBus, Event, PubSub

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_placed_event = Event(PubSub, OrderPlaced)

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

### Serde

Serialization and deserialization tools for encoding/decoding data: `encode`, `pack`, and `unpack_json`.

````{example} Round-tripping a dataclass through pack/unpack
```{code-block} python
from dataclasses import asdict, dataclass
from uuid import UUID, uuid4
from stratae.serde import pack, unpack_json

@dataclass
class Widget:
    id: UUID
    name: str

    def __post_init__(self):
        if isinstance(self.id, str):
            self.id = UUID(self.id)

    def to_dict(self):
        return asdict(self)

widget = Widget(id=uuid4(), name="sprocket")
data = pack(widget)
print(data)

restored = unpack_json(data, type=Widget)
print(restored.name)
```
```{output}
b'{"id": "05483b1e-b51a-4a28-a992-59dc75499b1f, "name": "sprocket"}'
sprocket
```
````

{doc}`Full reference <apidocs/stratae.serde/stratae.serde>`

### Integrations

Bridges between Stratae and third-party tools. FastAPI and Starlette get lifecycle-scoped routes. msgspec enables a faster pack path. RabbitMQ gets async publish and consume adapters.

````{example} Publishing a msgspec.Struct event to RabbitMQ, then consuming it back
<!--- skip: next -->
```{code-block} python
import asyncio
import msgspec
import stratae.integrations.msgspec  # noqa: F401
from stratae.events import Event, PubSub
from stratae.integrations.rabbitmq import (
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQConsumer,
    RabbitMQPublisher,
)

class OrderPlaced(msgspec.Struct):
    order_id: int

order_placed_event = Event(PubSub, OrderPlaced)

consumer = RabbitMQConsumer("amqp://guest:guest@localhost/")

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
        RabbitMQPublisher("amqp://guest:guest@localhost/") as publisher,
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
context <apidocs/stratae.context/stratae.context>
serde <apidocs/stratae.serde/stratae.serde>
integrations <apidocs/stratae.integrations/stratae.integrations>
```
