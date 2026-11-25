# An outbox store on SQLAlchemy

The [outbox](../apidocs/stratae.events/stratae.events.outbox) keeps its records in an `OutboxStore`, a small protocol any storage can implement. Most services keep the records in the same SQL database as their data. A record then commits in the same transaction as the change it describes. This page is the recipe for that store on SQLAlchemy. It builds on [A database session per scope](database.md).

```{motivation}
The outbox only works if a record and its data commit together. So the store must add records through the unit of work's own session, not a session of its own. The relay's work is different. It claims, marks and reschedules records outside any unit of work, so it opens a short transaction of its own for each step. Two relays must never claim the same record. A relay that dies mid-batch must not strand the records it claimed. The recipe handles each of these, and leaves the schema and the cleanup policy with the application.
```

## The table and the unit of work

The application's own table, `orders`, sits beside the outbox table. One outbox row per record. A row holds a message, which never changes once it's added, and its delivery state: `available_at`, `attempts` and `error`. The relay writes only the delivery columns, by message id. The message's envelope takes four columns: `id` holds its message id and `created_at` its timestamp, beside `correlation_id` and `causation_id`. So a chain of messages can be found by its correlation id. `address` holds where a route that addresses each payload sent it, such as a RabbitMQ routing key built from the payload. `claimed_until` is the lease that keeps a claimed record from being claimed again. `failed` marks a record parked after its last attempt. `headers` holds the application's headers, without the envelope's. It's a column of type `Headers`: `jsonb` on Postgres, and JSON on other databases. The store must give back what it was given, and JSON has no type for a timestamp. So `Headers` stores a timestamp as `{"$datetime": ...}`, at any depth, and turns it back into one. Text that looks like a timestamp stays text. The database's driver does the JSON encoding itself, so `Headers` hands it plain values and tags only what JSON can't hold. A value JSON can't hold that isn't tagged, such as bytes, fails the add inside the unit of work, and nothing commits. If your middleware sends such values, tag them in `tagged` and `untagged` too. The session recipe is the same as in [A database session per scope](database.md).

````{example} The outbox table, the engine, and a session per unit of work
```{code-block} python
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Awaitable, Callable, Sequence
from uuid import UUID

from sqlalchemy import (
    JSON,
    DateTime,
    LargeBinary,
    String,
    Text,
    TypeDecorator,
    delete,
    exists,
    or_,
    select,
    tuple_,
    update,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, aliased, mapped_column
from sqlalchemy.engine import Dialect
from sqlalchemy.pool import StaticPool
from sqlalchemy.types import TypeEngine

from stratae.events import Envelope
from stratae.events.outbox import OutboxDelivery, OutboxMessage, OutboxRecord
from stratae.lifecycle import AsyncScope

class Base(DeclarativeBase):
    pass

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True)
    item: Mapped[str]

def utc(moment: datetime) -> datetime:
    # SQLite returns timestamps without a timezone. Postgres keeps it.
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)

def tagged(value: Any) -> Any:
    # JSON has no timestamp, so one is stored tagged, at any depth.
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, dict):
        return {key: tagged(item) for key, item in value.items()}
    if isinstance(value, list):
        return [tagged(item) for item in value]
    return value

def untagged(value: Any) -> Any:
    if isinstance(value, dict):
        if value.keys() == {"$datetime"}:
            return datetime.fromisoformat(value["$datetime"])
        return {key: untagged(item) for key, item in value.items()}
    if isinstance(value, list):
        return [untagged(item) for item in value]
    return value

class Headers(TypeDecorator[dict[str, Any]]):
    # jsonb on Postgres, JSON elsewhere, with timestamps kept as timestamps.
    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(JSON())

    def process_bind_param(self, value: Any, dialect: Dialect) -> Any:
        return tagged(value)

    def process_result_value(self, value: Any, dialect: Dialect) -> Any:
        return untagged(value)

class OutboxRow(Base):
    __tablename__ = "outbox"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    correlation_id: Mapped[UUID]
    causation_id: Mapped[UUID | None]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    route: Mapped[str] = mapped_column(String(200))
    body: Mapped[bytes] = mapped_column(LargeBinary)
    headers: Mapped[dict[str, Any]] = mapped_column(Headers)
    key: Mapped[str | None] = mapped_column(String(200), index=True)
    address: Mapped[str | None] = mapped_column(String(200))
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    claimed_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(default=0)
    error: Mapped[str | None] = mapped_column(Text)
    failed: Mapped[bool] = mapped_column(default=False, index=True)

    @classmethod
    def from_message(cls, message: OutboxMessage) -> "OutboxRow":
        envelope = message.envelope
        return cls(
            id=envelope.message_id,
            correlation_id=envelope.correlation_id,
            causation_id=envelope.causation_id,
            created_at=envelope.timestamp,
            route=message.route,
            body=message.body,
            headers=dict(message.headers),
            key=message.key,
            address=message.address,
            available_at=envelope.timestamp,
        )

    def to_record(self) -> OutboxRecord:
        envelope = Envelope(self.id, self.correlation_id, self.causation_id, utc(self.created_at))
        message = OutboxMessage(
            envelope=envelope,
            route=self.route,
            body=self.body,
            headers=self.headers,
            key=self.key,
            address=self.address,
        )
        delivery = OutboxDelivery(utc(self.available_at), self.attempts, self.error)
        return OutboxRecord(message, delivery)

application = AsyncScope("application", isolation="shared")
work = AsyncScope("work", requires=application)

@application.cache()
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

@work.cache()
async def get_session() -> AsyncIterator[AsyncSession]:
    async with (await get_sessionmaker())() as session:
        try:
            yield session
        except BaseException:
            await session.rollback()
            raise
        await session.commit()
```
````

## The store

`add` puts the row into the unit of work's session. It's written when that session commits, and discarded if it rolls back. The other four methods belong to the relay. Each runs in a short transaction of its own.

`claim` keeps records with the same key in order. It locks the due, unclaimed rows, skipping any another relay has locked. It leaves out each keyed row with an earlier row of its key that's waiting for a retry, or claimed. So rows held back that way never use up the limit, and can't crowd out rows of other keys. A second query then leaves out each keyed row whose key has an earlier row still pending outside this claim. That catches an earlier row another relay has locked but not yet claimed, which the first query can't see. A row dropped that way has already used up a place under the limit, so the claim can return fewer rows than it could have. A lock hides a row from the first query, but not from the second, so two relays can't split a key between them. Rows are ordered by creation time, then by id. Time-ordered ids, such as `uuid7`'s, make that the order they were written in, even when two share a timestamp.

````{example} An outbox store on SQLAlchemy
```{code-block} python
class SqlAlchemyOutboxStore:
    def __init__(
        self,
        session: Callable[[], Awaitable[AsyncSession]],
        sessionmaker: Callable[[], Awaitable[async_sessionmaker[AsyncSession]]],
        lease: float = 60.0,
    ) -> None:
        self._session = session
        self._sessionmaker = sessionmaker
        self._lease = timedelta(seconds=lease)

    async def add(self, message: OutboxMessage) -> None:
        (await self._session()).add(OutboxRow.from_message(message))

    async def claim(self, limit: int) -> Sequence[OutboxRecord]:
        now = datetime.now(timezone.utc)
        due = (
            select(OutboxRow)
            .where(
                ~OutboxRow.failed,
                OutboxRow.available_at <= now,
                or_(OutboxRow.claimed_until.is_(None), OutboxRow.claimed_until <= now),
                ~self._waiting_before(now),
            )
            .order_by(OutboxRow.created_at, OutboxRow.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        async with (await self._sessionmaker())() as session, session.begin():
            rows = (await session.scalars(due)).all()
            held = set(await session.scalars(self._held_back(rows)))
            claimed = [row for row in rows if row.id not in held]
            for row in claimed:
                row.claimed_until = now + self._lease
            return [row.to_record() for row in claimed]

    def _waiting_before(self, now: datetime) -> Any:
        # An earlier pending row of the same key that's waiting for a retry, or claimed.
        earlier = aliased(OutboxRow)
        return exists().where(
            earlier.key == OutboxRow.key,
            ~earlier.failed,
            or_(earlier.available_at > now, earlier.claimed_until > now),
            tuple_(earlier.created_at, earlier.id) < tuple_(OutboxRow.created_at, OutboxRow.id),
        )

    def _held_back(self, rows: Sequence[OutboxRow]) -> Any:
        # The keyed rows with an earlier pending row of their key outside this claim:
        # waiting for a retry, or claimed or locked by another relay.
        earlier = aliased(OutboxRow)
        ids = [row.id for row in rows]
        return select(OutboxRow.id).where(
            OutboxRow.id.in_([row.id for row in rows if row.key is not None]),
            exists().where(
                earlier.key == OutboxRow.key,
                ~earlier.failed,
                earlier.id.not_in(ids),
                tuple_(earlier.created_at, earlier.id)
                < tuple_(OutboxRow.created_at, OutboxRow.id),
            ),
        )

    async def mark_sent(self, message_ids: Sequence[UUID]) -> None:
        await self._write(delete(OutboxRow).where(OutboxRow.id.in_(message_ids)))

    async def reschedule(self, message_id: UUID, delivery: OutboxDelivery) -> None:
        await self._write(
            update(OutboxRow)
            .where(OutboxRow.id == message_id)
            .values(
                available_at=delivery.available_at,
                attempts=delivery.attempts,
                error=delivery.error,
                claimed_until=None,
            )
        )

    async def mark_failed(self, message_id: UUID, delivery: OutboxDelivery) -> None:
        await self._write(
            update(OutboxRow)
            .where(OutboxRow.id == message_id)
            .values(
                failed=True, attempts=delivery.attempts, error=delivery.error, claimed_until=None
            )
        )

    async def _write(self, statement: Any) -> None:
        async with (await self._sessionmaker())() as session, session.begin():
            await session.execute(statement)
```
````

The store takes `get_session` and `get_sessionmaker` as plain callables. It doesn't know about scopes. Whatever unit of work is active when the outbox writes is the one its record joins.

## A record commits with its data

Bind an event to an `OutboxProducer` over the store, and emit it inside the unit of work. The order and its record are written together, or not at all. A relay publishes what committed, and `mark_sent` removes it.

````{example} A committed unit keeps its order and record; a rolled-back one keeps neither
```{code-block} python
import asyncio
from typing import Annotated
from sqlalchemy import func
from stratae.depends import Depends, inject
from stratae.events import Encoded, Envelope, EnvelopeFactory, PubSub
from stratae.events.outbox import OutboxConfig, OutboxProducer, OutboxRelay

class OrderPlaced:
    def __init__(self, order_id: int, item: str) -> None:
        self.order_id = order_id
        self.item = item

class PrintingPublisher:
    envelope_factory = EnvelopeFactory()

    def __init__(self) -> None:
        self.routes: dict[Any, Any] = {}

    def route(self, event: Any, config: Any) -> None:
        self.routes[event] = config

    async def emit(self, event: Any, payload: Encoded, *, envelope: Envelope | None = None,
                   headers: Any = None) -> None:
        print(f"published {payload.body!r} to {self.routes[event]}")

order_placed = PubSub(OrderPlaced)
store = SqlAlchemyOutboxStore(get_session, get_sessionmaker)
outbox = OutboxProducer(store, serializer=lambda order: f"{order.order_id}:{order.item}".encode())
announce = outbox.bind(order_placed, factory=OrderPlaced, config=OutboxConfig("orders"))

type SessionDep = Annotated[AsyncSession, Depends(get_session)]

@inject
async def place_order(item: str, session: SessionDep) -> None:
    order = Order(item=item)
    session.add(order)
    await session.flush()
    await announce(order_id=order.id, item=item)

@inject
async def counts(session: SessionDep) -> tuple[int, int]:
    orders = await session.scalar(select(func.count()).select_from(Order))
    records = await session.scalar(select(func.count()).select_from(OutboxRow))
    return orders or 0, records or 0

async def main() -> None:
    async with application:
        async with work:
            await place_order("lamp")
        try:
            async with work:
                await place_order("anvil")
                raise ValueError("too heavy to ship")
        except ValueError as exc:
            print(f"failed: {exc}")
        async with work:
            print(await counts())
        print(f"sent {await OutboxRelay(store, PrintingPublisher(), outbox).run_once()}")
        async with work:
            print(await counts())

asyncio.run(main())
```
```{output}
failed: too heavy to ship
(1, 1)
published b'1:lamp' to orders
sent 1
(1, 0)
```
````

## Several relays on Postgres

Run more than one relay for throughput or availability, and they share the table. On Postgres, the claim query ends in `FOR UPDATE SKIP LOCKED`. Each relay locks the rows it selects, and skips rows another relay holds. The lock lasts only for the claim's short transaction. After that, `claimed_until` keeps the rows claimed while the relay publishes them. A relay that dies leaves its rows unmarked. Once their lease runs out, another relay claims them, and publishes them under the same message ids.

Make the lease longer than a batch takes to publish. If a slow batch outlives its lease, another relay can publish the same records too. That's still within the outbox's at-least-once guarantee, but it adds duplicates for consumers to discard.

## SQLite

SQLite has no row locks, so SQLAlchemy leaves `FOR UPDATE SKIP LOCKED` out of the query. Run one relay against a SQLite database. SQLite also returns timestamps without a timezone, which is what `utc` puts back. Postgres stores `timestamptz` and needs neither.

## Cleaning up

`mark_sent` deletes sent rows, so the table holds only the work still to do. To keep an audit trail instead, add a `sent_at` column. Set it in `mark_sent`, filter on it in `claim`, and purge old rows on a schedule.

Failed rows stay until someone deals with them. They keep their `error`, so they can be inspected with a query. To replay one, clear `failed`, reset `attempts`, and set `available_at` to now. The relay picks it up on its next poll.
