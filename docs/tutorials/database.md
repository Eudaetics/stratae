# A database session per scope

Most services need one database transaction per unit of work, such as a request, a consumed message or a job. It should commit when the work succeeds and roll back when it fails. The commit should land *before* the response goes out or the message is acked. Stratae has no database integration because it doesn't need one. A [lifecycle](../apidocs/stratae.lifecycle/stratae.lifecycle) resource cached in a scope already has that shape. This page is the recipe, shown with SQLAlchemy's `AsyncSession`.

```{motivation}
A session is a resource like any other. It's opened on first use and shared by everything inside the unit of work. It's closed when the unit ends. What makes it a *transaction* is the cleanup code after `yield`. It commits if the scope closed cleanly and rolls back if the scope closed with an error. A resource's cleanup already sees the exception its scope closed with, so nothing more is needed. As a recipe, every choice stays with the application. That includes the driver, read-only sessions, several databases, savepoints, and any setup at the start of each transaction.
```

## The recipe

The engine and sessionmaker live in an application scope, opened once. The session lives in a `work` scope, with one activation per unit of work. `begin_transaction` is the hook for per-transaction setup that must happen before the first query.

````{example} An engine per application, a session and transaction per unit of work
```{code-block} python
import asyncio
from typing import Annotated, AsyncIterator

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

from stratae.context import Context
from stratae.depends import Depends, inject
from stratae.lifecycle import AsyncScope, async_resource

class Base(DeclarativeBase):
    pass

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    item: Mapped[str]

application = AsyncScope("application", isolation="shared")
work = AsyncScope("work", requires=application)

current_user = Context[str]("current_user", default="system")

@application.cache()
@async_resource
async def get_engine() -> AsyncIterator[AsyncEngine]:
    # In-memory SQLite for the example; a real service passes its database URL.
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

async def begin_transaction(session: AsyncSession) -> None:
    # On Postgres, e.g. for row-level security policies to read:
    #   SELECT set_config('app.user_id', :user, true)
    await session.execute(text("SELECT :user"), {"user": current_user()})
    print(f"transaction for {current_user()}")

@work.cache()
@async_resource
async def get_session() -> AsyncIterator[AsyncSession]:
    async with (await get_sessionmaker())() as session:
        await begin_transaction(session)
        try:
            yield session
        except BaseException:
            await session.rollback()
            print("rolled back")
            raise
        await session.commit()
        print("committed")

type SessionDep = Annotated[AsyncSession, Depends(get_session)]

@inject
async def place_order(item: str, session: SessionDep) -> None:
    session.add(Order(item=item))
    await session.flush()

@inject
async def list_orders(session: SessionDep) -> list[str]:
    return list(await session.scalars(select(Order.item).order_by(Order.id)))
```
````

Three details carry the guarantees:

- **The commit is in the cleanup, after `yield`.** It runs when the `work` scope closes cleanly. Everything inside has finished by then, so no handler commits half a unit of work.
- **It rolls back on `BaseException`, not `Exception`.** A cancelled request, or a message cancelled at shutdown, closes its scope with `CancelledError`. `except Exception` would miss it. The session would then close without an explicit rollback.
- **`begin_transaction` runs before `yield`.** Whatever it sets up is in place before anyone's first query. A typical example is the setting a row-level security policy reads. On Postgres, `set_config(..., true)` keeps it local to the transaction.

## Commit and roll back

Each `async with work:` is one unit of work. Everything in it shares one session. It commits if the block finishes and rolls back if it raises.

````{example} A clean unit commits; a failing one rolls back, leaving nothing behind
```{code-block} python
async def main() -> None:
    async with application:
        with current_user.use("alice"):
            async with work:
                await place_order("keyboard")
            try:
                async with work:
                    await place_order("mouse")
                    raise ValueError("payment declined")
            except ValueError as exc:
                print(f"failed: {exc}")
        async with work:
            print(await list_orders())

asyncio.run(main())
```
```{output}
transaction for alice
committed
transaction for alice
rolled back
failed: payment declined
transaction for system
['keyboard']
committed
```
````

The `mouse` order was flushed, so the database had seen it. The rollback discarded it anyway. Each unit also ran `begin_transaction` first, with whoever was the current user at the time.

A unit of work that's cancelled rolls back too. That's why the recipe catches `BaseException`.

````{example} A cancelled unit of work rolls back
```{code-block} python
async def cancelled() -> None:
    async with application:
        started = asyncio.Event()

        async def slow_order() -> None:
            async with work:
                await place_order("monitor")
                started.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(slow_order())
        await started.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            print("cancelled")
        async with work:
            print(await list_orders())

asyncio.run(cancelled())
```
```{output}
transaction for system
rolled back
cancelled
transaction for system
[]
committed
```
````

## Event handlers share the transaction

An event handler running inside the unit of work gets the same session. Its writes commit or roll back with the rest. Emitting on an [`AsyncDirectBus`](../apidocs/stratae.events/stratae.events.direct) runs handlers in the emitter's context, inside its scope.

````{example} An audit handler's write commits and rolls back with the order
```{code-block} python
from stratae.events import AsyncDirectBus, PubSub

class AuditEntry(Base):
    __tablename__ = "audit"
    id: Mapped[int] = mapped_column(primary_key=True)
    note: Mapped[str]

class OrderPlaced:
    def __init__(self, item: str) -> None:
        self.item = item

order_placed = PubSub(OrderPlaced)
bus = AsyncDirectBus()
announce_order = bus.bind(order_placed, factory=OrderPlaced)

@bus.handle(order_placed)
@inject
async def record_audit(event: OrderPlaced, session: SessionDep) -> None:
    session.add(AuditEntry(note=f"placed {event.item}"))

async def checkout(item: str) -> None:
    await place_order(item)
    await announce_order(item=item)
    if item == "gift card":
        raise ValueError("gift cards are sold out")

@inject
async def counts(session: SessionDep) -> tuple[int, int]:
    orders = await session.scalar(select(func.count()).select_from(Order))
    audit = await session.scalar(select(func.count()).select_from(AuditEntry))
    return orders or 0, audit or 0

async def shared() -> None:
    async with application:
        async with work:
            await checkout("headphones")
        try:
            async with work:
                await checkout("gift card")
        except ValueError as exc:
            print(f"failed: {exc}")
        async with work:
            print(await counts())

asyncio.run(shared())
```
```{output}
transaction for system
committed
transaction for system
rolled back
failed: gift cards are sold out
transaction for system
(1, 1)
committed
```
````

There's one order and one audit entry. The gift card's order *and* its audit entry rolled back together. This holds for in-process buses only. An event published to a broker leaves the transaction the moment it's sent. It's out even if the transaction later rolls back. The [outbox](../apidocs/stratae.events/stratae.events.outbox) fixes that.

## Where the commit lands

The unit of work is whatever scope the entry point activates. Make that the session's scope, and the commit lands exactly where it belongs.

**HTTP.** Give {py:func}`scoped_route <stratae.integrations.starlette.scoped_route>` the session's scope as its `handler` scope. It closes when the endpoint returns, before the response is sent. A failed commit becomes a 500, so the client never sees a success for a transaction that didn't commit.

````{example} The commit lands before the response; a failing request rolls back as a 500
```{code-block} python
from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.testclient import TestClient
from stratae.integrations.starlette import scoped_route

request_scope = AsyncScope("request", requires=application)

async def create_order(request: Request) -> JSONResponse:
    item = request.path_params["item"]
    await place_order(item)
    if item == "anvil":
        raise ValueError("too heavy to ship")
    return JSONResponse({"placed": item})

async def orders(request: Request) -> JSONResponse:
    return JSONResponse(await list_orders())

@asynccontextmanager
async def lifespan(app: Starlette):
    async with application:
        yield

ScopedRoute = scoped_route(request_scope, handler=work)
app = Starlette(
    lifespan=lifespan,
    routes=[
        ScopedRoute("/orders/{item}", create_order, methods=["POST"]),
        ScopedRoute("/orders", orders),
    ],
)

with TestClient(app, raise_server_exceptions=False) as client:
    print(client.post("/orders/lamp").status_code)
    print(client.post("/orders/anvil").status_code)
    print(client.get("/orders").json())
```
```{output}
transaction for system
committed
200
transaction for system
rolled back
500
transaction for system
committed
['lamp']
```
````

**Messages.** Give {py:class}`RabbitMQConsumer <stratae.integrations.rabbitmq.RabbitMQConsumer>` the session's scope as its `scope`. A message is acked only once its scope has closed, so its transaction has committed first. A failed commit fails the message. It's nacked, or retried and dead-lettered under its {py:class}`RabbitMQFailurePolicy <stratae.integrations.rabbitmq.RabbitMQFailurePolicy>`. A message cancelled at shutdown, or by a dropped connection, rolls back and is redelivered.

<!--- skip: next -->
```{code-block} python
from stratae.integrations.rabbitmq import RabbitMQConsumer

consumer = RabbitMQConsumer(
    "amqp://guest:guest@localhost/",
    scope=work,
    prefetch_count=10,
)
```

The application scope belongs to whatever runs the service. That's the ASGI lifespan, or the worker's `main`. Enter it before the consumer.

## Common mistakes

- **Rolling back on `except Exception`.** Cancellation is a `BaseException`. A client disconnect, a timeout or a shutdown cancels the unit of work, and the rollback never runs.
- **Committing inside a handler.** A commit halfway through a unit of work splits it. A later failure rolls back only what came after. Commit once, in the resource's cleanup.
- **Caching the session in the application scope.** Every request then shares one session and one transaction. Cache the engine and sessionmaker there. Cache the session in the unit-of-work scope.
- **Opening the session in the outer request scope.** It then commits after the response has been sent. A failed commit can't change what the client was told. Use the `handler` scope instead.
- **Expecting a broker publish to roll back.** Only in-process handlers share the transaction. A message sent to a broker during the unit of work is out, even if the unit later rolls back. Write it through the [outbox](../apidocs/stratae.events/stratae.events.outbox) instead, so it commits with the data.
