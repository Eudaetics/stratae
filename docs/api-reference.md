# API Reference

Stratae is organized around three core tools — dependency injection, lifecycle management, and events — plus a few supporting modules used across all three.

## Core

### Dependency Injection

Mark a parameter with `Injected[T, Depends(provider)]` and decorate the function with `inject`: the parameter is resolved by calling its provider at call time. Supports sync and async providers, and temporary overrides for testing.

```{code-block} python
:caption: Injecting the current user and swapping in a test user.

from dataclasses import dataclass
from stratae.depends import Depends, Injected, inject, override

@dataclass
class User:
    name: str

def get_current_user() -> User:
    return User(name="anonymous")

@inject
def greeting(user: Injected[User, Depends(get_current_user)]) -> str:
    return f"Welcome, {user.name}!"

assert greeting() == "Welcome, anonymous!"

with override(get_current_user, User(name="Jane")):
    assert greeting() == "Welcome, Jane!"

assert greeting() == "Welcome, anonymous!"
```

{doc}`Full reference <apidocs/stratae.depends/stratae.depends>`

### Lifecycle

Manage hierarchical, scoped contexts for caching and cleanup — `Lifecycle`/`AsyncLifecycle`, `Scope`, and `resource`/`async_resource` for resources that need teardown.

```{code-block} python
:caption: Commit a transaction if the request scope exits cleanly, roll back if it raises

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
        conn.commit()  # request scope exited without raising
    except Exception:
        conn.rollback()  # an exception propagated out of the request
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
    pass  # transaction was rolled back before the exception propagated here
```

{doc}`Full reference <apidocs/stratae.lifecycle/stratae.lifecycle>`

### Events

Event definitions, bound-event facades, and dispatch protocols for pub/sub and request/reply messaging.

```{code-block} python
:caption: Defining a pub/sub event and dispatching it through an in-process bus

from stratae.events import DirectBus, PubSub, event

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_placed = event(OrderPlaced, PubSub)

bus = DirectBus()
place_order = bus.bind(order_placed)

received: list[int] = []

@bus.handle(order_placed)
def on_order_placed(order: OrderPlaced) -> None:
    received.append(order.order_id)

place_order(order_id=42)
assert received == [42]
```

{doc}`Full reference <apidocs/stratae.events/stratae.events>`

## Supporting modules

### Context

Callable, injectable values backed by `contextvars` — set once with `.set()` or `with ctx.use(value):`, read anywhere within that scope, and usable directly as a `Depends()` provider.

```{code-block} python
:caption: Setting and reading a value across nested scopes

from stratae.context import Context

user_id = Context[int]("user_id")

with user_id.use(42):  # the support agent's own account
    assert user_id() == 42

    # "View as customer" temporarily impersonates the customer
    # to reproduce a bug, then reverts to the agent's session.
    with user_id.use(7):
        assert user_id() == 7

    assert user_id() == 42  # back to the agent's own session
```

{doc}`Full reference <apidocs/stratae.context/stratae.context>`

### Checks

Run collections of zero-argument checks, raising or gathering their failures — `check`, `check_async`, and the `require` decorator.

```{code-block} python
:caption: Reject account deletion if the user is not an admin

from types import SimpleNamespace
import pytest
from stratae.checks import require

user = SimpleNamespace(id=1, is_admin=False)

def is_admin():
    assert user.is_admin

@require(is_admin)
def delete_account(account_id: int):
    # Code in here only runs if the require checks do not raise
    print("Deleting Account")

with pytest.raises(AssertionError):
    delete_account(24)  # Will abort for the above user since it fails the check
```

{doc}`Full reference <apidocs/stratae.checks/stratae.checks>`

### Serde

Serialization and deserialization tools for encoding/decoding data — `encode`, `pack`, `unpack_json`.

```{code-block} python
:caption: Round-tripping a dataclass through the default pack/unpack pair

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
assert data == f'{{"id": "{widget.id}", "name": "sprocket"}}'.encode()

restored = unpack_json(data, type=Widget)
assert restored == widget
```

{doc}`Full reference <apidocs/stratae.serde/stratae.serde>`

### Integrations

Bridges between Stratae modules and third-party tools: event adapters (RabbitMQ), lifecycle integrations (ASGI), and serde integrations (msgspec).

```{code-block} python
:caption: Packing a msgspec.Struct uses the faster msgspec-based encoder

import msgspec
import stratae.integrations.msgspec  # noqa: F401 (registers the pack fast path)
from stratae.serde import pack

class Point(msgspec.Struct):
    x: int
    y: int

point = Point(x=1, y=2)
result = pack(point)
assert isinstance(result, bytes)
assert msgspec.json.decode(result, type=Point) == point
```

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
