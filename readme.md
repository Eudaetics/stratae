# Stratae

Stratae is a set of developer tools for Python 3.12+. It currently covers dependency injection, lifecycle-scoped caching and cleanup, and events. Each tool works on its own: use lifecycle management without injection, or dependency injection by itself.

These tools work anywhere instead of being tied to a particular framework. A function decorated with @inject is still an ordinary function: callable directly, importable, or wired into a web framework or worker. The same holds for @scope.cache.

```bash
pip install stratae
```

## Quick Start

```python
from typing import Annotated
from stratae.depends import Depends, inject
from stratae.lifecycle import Scope

application = Scope("application", isolation="shared")

type Database = dict[str, list[dict[str, str]]]


# Simple database connection (just a dict for demo)
@application.cache()
def get_database() -> Database:
    return {"users": []}


@inject
def create_user(name: str, db: Annotated[Database, Depends(get_database)]):
    user = {"name": name}
    db["users"].append(user)
    return user


with application:
    user = create_user("Alice")
    print(f"Created user: {user['name']}")
```

## Features

### Dependency Injection

Dependency injection in Stratae uses familiar decorator syntax that works with callables. Use this to send values, objects, or anything into a function.

```python
from typing import Annotated
from stratae.depends import Depends, inject


def get_config():
    return {"env": "dev", "mode": "strict"}


@inject
def endpoint(config: Annotated[dict[str, str], Depends(get_config)]):
    print(f"Environment: {config['env']}, Mode: {config['mode']}")


endpoint()
# Environment: dev, Mode: strict
```

### Lifecycle Management

Use lifecycle management when you want to cache objects or guarantee resource cleanup for context managers. With managed resources, everything is cleaned up automatically at the end of a lifecycle scope.

```python
application = Scope("application", isolation="shared")
request = Scope("request", isolation="shared", requires=application)


# Cache the yielded value and return it for all calls within a request;
# the code after the yield runs when the request scope ends
@request.cache()
def get_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# Set up your lifecycle boundaries
with application:
    with request:
        # Session is created at first call and cached automatically
        # All get_session calls in this request will return the same session
        db = get_session()
        assert db is get_session()
        db.users.create_user("John")
    with request:
        # New request, new session
        db = get_session()
```

Each `Scope` also takes a `storage` option (`"dense"`, the default, or `"sparse"`) that controls how cached slots are allocated. Dense indexes slots by position and is cheapest per access; sparse allocates lazily and resets only the slots touched during an activation. Dense wins for scopes with few registered functions or where most cached functions get used per activation; sparse pulls ahead for scopes registering many functions where a given activation only touches a handful of the cached functions.

### Context Variables

Stratae uses context variables for setting values that are needed deep in dependency chains. Change values at runtime, or even whole behavior, without needing to thread parameters or manipulate overrides.

```python
from typing import Annotated
from stratae.context import Context
from stratae.depends import Depends, inject

request = Scope("request", isolation="shared")
user_id = Context[int]("user_id")


@request.cache()
@inject
def get_current_user(uid: Annotated[int, Depends(user_id)]) -> User:
    return fetch_user(uid)


@inject
def create_post(
    content: str,
    user: Annotated[User, Depends(get_current_user)],
) -> Post:
    return Post(author=user, content=content)


with request, user_id.use(123):
    post = create_post("Hello world!")
```

### Events

Stratae events separate what an event is from how it is dispatched. Each dispatch pattern is its own event class: `PubSub(Schema)` for fire-and-forget fan-out, or `Request(Schema, reply=Reply)` for a blocking call answered by exactly one responder. A `Request` without a `reply` is a command, replying `None`. Emitting a payload for the event dispatches it to the registered handlers.

```python
from stratae.events import DirectBus, PubSub

bus = DirectBus()


class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


order_placed = PubSub(OrderPlaced)


@bus.handle(order_placed)
def notify(order: OrderPlaced) -> None:
    print(f"Order {order.order_id} placed")


bus.emit(order_placed, OrderPlaced(42))
# Order 42 placed
```

`bind` is an optional helper that wraps `emit` in a callable. Pass `factory` to have the bound event construct the payload from its arguments before forwarding to `emit`; omit it to forward an already-built payload directly.

```python
place_order = bus.bind(order_placed, factory=OrderPlaced)

place_order(order_id=42)
# Order 42 placed
```

The event is the shareable definition. What `bind` and `handle` take beyond that is adapter-specific, and the two sides are independent. The direct buses need no routing config, and use the event definition itself as the handler key. A broker adapter routes each event once, to an exchange and routing key, and registers handlers against a queue.

Request events block until their responder returns, and the reply is fully typed:

```python
from stratae.events import Request


class Quote:
    def __init__(self, total: int) -> None:
        self.total = total


class PriceOrder:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


price_order = Request(PriceOrder, reply=Quote)


@bus.handle(price_order)
def price(order: PriceOrder) -> Quote:
    return Quote(total=100)


quote = bus.emit(price_order, PriceOrder(42))  # typed as Quote
```

`AsyncDirectBus` offers the same surface for async code, accepting both sync and async handlers. It runs an event's async handlers one at a time by default. This is so they can share a resource that refuses concurrent use, such as a database session. To run them concurrently, build it with `AsyncDirectBus(dispatchers=(Dispatch(PubSub, fan_out_concurrent),))`. Handlers are ordinary functions, so `@inject` and lifecycle-cached dependencies compose with them unchanged.

Middleware runs around every emit and every handler call. Each one takes the call and `next`, the rest of the chain, as web frameworks' middleware does:

```python
from stratae.events import Middleware


def tag_tenant(call, next):
    return next(call.with_headers({"x-tenant": "acme"}))


def log_tenant(call, next):
    print(f"handling for {call.headers['x-tenant']}")
    return next(call)


bus = DirectBus(middleware=Middleware(publish=(tag_tenant,), handle=(log_tenant,)))
```

### Async Support

Stratae is fully async compatible. Injection natively works with sync or async functions. Lifecycle offers versions for sync and async handling of resources.

```python
from typing import Annotated
from stratae.depends import Depends, inject
from stratae.lifecycle import AsyncScope

application = AsyncScope("application", isolation="shared")
request = AsyncScope("request", requires=application)


@application.cache()
async def get_database() -> Database:
    return await Database(url="postgresql://...")


@inject
async def create_user(
    name: str,
    db: Annotated[Database, Depends(get_database)],
) -> User:
    return await db.users.create(name=name)


async with application:
    async with request:
        user = await create_user("Alice")
```

### Framework Agnostic

Stratae doesn't have a complex framework to configure or objects to pass around. Write your business logic once with injection, then simply call those functions anywhere.

```python
# Business logic - framework-independent
@inject
async def create_user(
    name: str,
    db: Annotated[Database, Depends(get_database)],
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


with override(get_config).to({"env": "test"}):
    endpoint()  # sees {"env": "test"}
endpoint()  # back to {"env": "prod"}
```

### Guard Checks

`require` runs zero-argument checks ahead of a function call, in order. A check passes by returning `None` and fails by raising, and the first one to raise aborts the call. A check that returns anything else is refused with `CheckResultError`, so a predicate such as `lambda: user.is_admin` can't pass by mistake.

```python
from stratae.checks import require


def is_admin():
    if not current_user().is_admin:
        raise PermissionError("admin required")


@require(is_admin)
def delete_account(account_id: int): ...
```

Sync functions only accept sync checks. Async functions accept a mix of sync and async checks, run in order.

### Simple Integrations

The design of Stratae means integrating with other tools or frameworks is typically easy. For FastAPI, a route class that activates a scope around each request is enough to add Stratae's lifecycle management.

```python
from typing import Annotated
from fastapi import FastAPI
from stratae.depends import Depends, inject
from stratae.integrations.fastapi import scoped_route
from stratae.lifecycle import AsyncScope


request = AsyncScope("request")

app = FastAPI()
app.router.route_class = scoped_route(request)


@request.cache()
async def get_session():
    session = AsyncSession()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


@app.post("/users")
@inject
async def post_user(
    name: str,
    # Using Stratae Depends
    db: Annotated[AsyncSession, Depends(get_session)],
):
    await db.users.create(name=name)
```

The request scope closes once the response has been sent, with the error the request ended with, if any. So a resource like this session rolls back on an `HTTPException` too.

### More Integrations

Each integration is a module of `stratae.integrations`, installed with the extra of the same name:

- **`fastapi`** and **`starlette`**: route classes that open a scope around each request. Starlette's also cover WebSocket connections.
- **`rabbitmq`**: a publisher and a consumer for RabbitMQ, with per-route routing keys, retry queues and dead letters, and reconnecting with backoff.
- **`opentelemetry`**: tracing middleware for any bus or adapter, following OpenTelemetry's messaging conventions.
- **`asgi`**: middleware that gives each request to any ASGI application its own envelope, so events it emits share its correlation id. It needs no extra.

The **`uuid`** extra makes envelope ids faster to create, and can make them time-ordered.

`stratae.events.outbox` writes events to an outbox store in the same transaction as your data, and a relay publishes what committed. `stratae.backoff` holds the backoff values reconnects, retries and the relay's polling share.

```bash
pip install "stratae[rabbitmq,opentelemetry]"
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

Before contributing, please:

1. Check for open issues or open a new issue to start a discussion
2. Fork the repository on GitHub
3. Install the development dependencies and every extra with `uv sync --all-extras --all-groups`
4. Install the pre-commit hooks with `uv run pre-commit install`
5. Make your changes following the project's coding style
6. Write tests that cover your changes
7. Update documentation if needed
8. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
