# Stratae

Stratae is a set of developer tools for Python 3.12+. It currently covers dependency injection, lifecycle-scoped caching and cleanup, and events. Each tool works on its own: use lifecycle management without injection, or dependency injection by itself.

These tools work anywhere instead of being tied to a particular framework. A function decorated with @inject is still an ordinary function: callable directly, importable, or wired into a web framework or worker. The same holds for @lifecycle.cache.

```bash
pip install stratae
```

## Quick Start

```python
from stratae.depends import Depends, Injected, inject
from stratae.lifecycle import Lifecycle, Scope

lifecycle = Lifecycle([Scope("application", "shared")])

type Database = dict[str, list[dict[str, str]]]


# Simple database connection (just a dict for demo)
@lifecycle.cache("application")
def get_database() -> Database:
    return {"users": []}


@inject
def create_user(name: str, db: Injected[Database, Depends(get_database)]):
    user = {"name": name}
    db["users"].append(user)
    return user


with lifecycle.start("application"):
    user = create_user("Alice")
    print(f"Created user: {user['name']}")
```

## Features

### Dependency Injection

Dependency injection in Stratae uses familiar decorator syntax that works with callables. Use this to send values, objects, or anything into a function.

```python
from stratae.depends import Depends, Injected, inject


def get_config():
    return {"env": "dev", "mode": "strict"}


@inject
def endpoint(config: Injected[dict[str, str], Depends(get_config)]):
    print(f"Environment: {config['env']}, Mode: {config['mode']}")


endpoint()
# Environment: dev, Mode: strict
```

### Lifecycle Management

Use lifecycle management when you want to cache objects or guarantee resource cleanup for context managers. With managed resources, everything is cleaned up automatically at the end of a lifecycle scope.

```python
lifecycle = Lifecycle([Scope("application", "shared"), Scope("request", "shared")])


# Cache the yielded value and return it for all calls within a request;
# @resource marks get_session as a contextmanager to be auto-entered
@lifecycle.cache("request")
@resource
def get_session():
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


# Set up your lifecycle boundaries
with lifecycle.start("application"):
    with lifecycle.start("request"):
        # Session is created at first call and cached automatically
        # All get_session calls in this request will return the same session
        db = get_session()
        assert db is get_session()
        db.users.create_user("John")
    with lifecycle.start("request"):
        # New request, new session
        db = get_session()
```

Each `Scope` also takes a `storage` option (`"dense"`, the default, or `"sparse"`) that controls how cached slots are allocated. Dense indexes slots by position and is cheapest per access; sparse allocates lazily and resets only the slots touched during an activation. Dense wins for scopes with few registered functions or where most cached functions get used per activation; sparse pulls ahead for scopes registering many functions where a given activation only touches a handful of the cached functions.

### Context Variables

Stratae uses context variables for setting values that are needed deep in dependency chains. Change values at runtime, or even whole behavior, without needing to thread parameters or manipulate overrides.

```python
from stratae.context import Context
from stratae.depends import Depends, Injected, inject

lifecycle = Lifecycle([Scope("request", "shared")])
user_id = Context[int]("user_id")


@lifecycle.cache("request")
@inject
def get_current_user(uid: Injected[int, Depends(user_id)]) -> User:
    return fetch_user(uid)


@inject
def create_post(
    content: str,
    user: Injected[User, Depends(get_current_user)],
) -> Post:
    return Post(author=user, content=content)


with lifecycle.start("request"), user_id.use(123):
    post = create_post("Hello world!")
```

### Events

Stratae events separate what an event is from how it is dispatched. An event pairs a payload with a dispatch pattern: `PubSub` for fire-and-forget fan-out, or `Request[Reply]` for a blocking call answered by exactly one responder. Emitting a payload for the event dispatches it to the registered handlers.

```python
from stratae.events import EventConfig, PubSub
from stratae.events.adapters import DirectBus

bus = DirectBus()


class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


order_placed = EventConfig(OrderPlaced, PubSub)


@bus.handle(order_placed)
def notify(order: OrderPlaced) -> None:
    print(f"Order {order.order_id} placed")


bus.emit(OrderPlaced(42), order_placed)
# Order 42 placed
```

`bind` is an optional helper that wraps `emit` in a callable: calling the bound event constructs the payload from its arguments and forwards it to the same `emit`, so emission can be passed around like an ordinary function.

```python
place_order = bus.bind(order_placed)

place_order(order_id=42)
# Order 42 placed
```

The `EventConfig` is the shareable definition of the event. What `bind` and `handle` take beyond that is adapter-specific, and the two sides are independent: the direct buses need no routing config and use the event definition itself as the handler key, while a broker adapter might bind an emitter with an exchange and routing key and register handlers against a queue.

Request events block until their responder returns, and the reply is fully typed:

```python
from stratae.events import EventConfig, Request


class Quote:
    def __init__(self, total: int) -> None:
        self.total = total


class PriceOrder:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


price_order = EventConfig(PriceOrder, Request[Quote])


@bus.handle(price_order)
def price(order: PriceOrder) -> Quote:
    return Quote(total=100)


quote = bus.emit(PriceOrder(42), price_order)  # typed as Quote
```

`AsyncDirectBus` offers the same surface for async code, accepting both sync and async handlers. Handlers are ordinary functions, so `@inject` and lifecycle-cached dependencies compose with them unchanged.

### Async Support

Stratae is fully async compatible. Injection natively works with sync or async functions. Lifecycle offers versions for sync and async handling of resources.

```python
from stratae.depends import Depends, Injected, inject
from stratae.lifecycle import AsyncLifecycle, Scope

lifecycle = AsyncLifecycle([Scope("application", "shared"), Scope("request", "context")])


@lifecycle.cache("application")
async def get_database() -> Database:
    return await Database(url="postgresql://...")


@inject
async def create_user(
    name: str,
    db: Injected[Database, Depends(get_database)],
) -> User:
    return await db.users.create(name=name)


async with lifecycle.start("application"):
    async with lifecycle.start("request"):
        user = await create_user("Alice")
```

### Framework Agnostic

Stratae doesn't have a complex framework to configure or objects to pass around. Write your business logic once with injection, then simply call those functions anywhere.

```python
# Business logic - framework-independent
@inject
async def create_user(
    name: str,
    db: Injected[Database, Depends(get_database)],
) -> User:
    return await db.users.create(name=name)


# FastAPI
@app.post("/users")
async def api_create(name: str):
    return await create_user(name)


# CLI
@click.command()
def cli_create(name: str):
    asyncio.run(create_user(name))
```

### Testing Overrides

Swap a dependency's value in a `with` block without touching the function that declares it - useful for tests or temporarily forcing a code path.

```python
from stratae.depends import override


def get_config():
    return {"env": "prod"}


with override(get_config, {"env": "test"}):
    endpoint()  # sees {"env": "test"}
endpoint()  # back to {"env": "prod"}
```

### Guard Checks

`require` runs zero-arg checks ahead of a function call, in order. A check's return value is discarded - only its side effects and raises matter, and the first one to raise aborts the call.

```python
from stratae.check import require


def is_admin():
    if not current_user().is_admin:
        raise PermissionError("admin required")


@require(is_admin)
def delete_account(account_id: int): ...
```

Sync functions only accept sync checks. Async functions accept a mix of sync and async checks, run in order.

### Simple Integrations

The design of Stratae means integrating with other tools or frameworks is typically easy. For FastAPI, an ASGI middleware that starts the request lifecycle is enough to add Stratae's lifecycle management.

```python
from fastapi import FastAPI
from stratae.depends import Depends, Injected, inject
from stratae.integrations import RequestLifecycleMiddleware
from stratae.lifecycle import AsyncLifecycle, Scope, async_resource


app = FastAPI()
lifecycle = AsyncLifecycle([Scope("request", "context")])

# Add the middleware that starts a lifecycle request
app.add_middleware(RequestLifecycleMiddleware, lifecycle, "request")


# Every FastAPI request will now get the same session within that request
@lifecycle.cache("request")
@async_resource
async def get_session():
    session = AsyncSession()
    try:
        yield session
        await session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@app.post("/users")
@inject
async def post_user(
    name: str,
    # Using Stratae Depends
    db: Injected[Session, Depends(get_session)],
):
    await db.users.create(name=name)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

Before contributing, please:

1. Check for open issues or open a new issue to start a discussion
2. Fork the repository on GitHub
3. Install development dependencies with `uv sync --extra dev --extra test` (or, without uv, `pip install -e ".[dev,test]"`)
4. Run pre-commit hooks with `pre-commit install`
5. Make your changes following the project's coding style
6. Write tests that cover your changes
7. Update documentation if needed
8. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
