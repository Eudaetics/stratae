"""
A transactional outbox: events written next to the data they describe, and published after.

Publishing an event straight to a broker from inside a unit of work goes wrong two ways. If
the unit then rolls back, the event has already left. If it commits, a consumer can still
read before the commit lands. The outbox writes the event as a record in the unit's own
transaction instead, so it commits or rolls back with the data. A relay publishes committed
records afterwards.

{py:class}`OutboxProducer` is bound like any bus. Emitting through it serializes the payload
and adds an {py:class}`OutboxRecord` to an {py:class}`OutboxStore`. Nothing is published
then. {py:class}`OutboxRelay` claims records, publishes each with the envelope it was
emitted with, and marks it sent once the publisher returns. With publisher confirms, that's
once the broker has it. Headers the producer's publish middleware adds, such as a trace, are
stored with the record and published with it. So they survive the wait in the outbox.

Storage is the application's. A store implements five methods, each sync or async to suit
its storage. It keeps records in the same transaction as the data they describe.
{py:class}`InMemoryOutboxStore` is a store for tests and examples, with a `transaction()`
standing in for a database's. {doc}`An outbox store on SQLAlchemy </tutorials/outbox>` shows a
store that shares the application's database.

The guarantees:

- **At-least-once delivery.** A relay that stops after a publish but before marking it sent
  publishes it again, with the same message id. Consumers deduplicate on it.
- **No ordering promise.** Records are claimed oldest first, so order usually holds, but
  retries and several relays reorder them. An {py:class}`OutboxConfig` `key` lets a store
  that supports it keep order per key.
- **Nothing lost.** A record is either sent, or parked as failed with its reason once it
  has failed `max_attempts` times. A broker outage never counts as an attempt.

````{example} Writing events in a transaction, then relaying the committed ones
```{code-block} python
import asyncio
from typing import Any
from stratae.events import Envelope, PubSub
from stratae.events.outbox import (
    InMemoryOutboxStore,
    OutboxConfig,
    OutboxProducer,
    OutboxRelay,
)

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

class PrintingPublisher:
    async def emit(self, event: Any, config: Any, payload: Any, *, serializer: Any = None,
                   envelope: Envelope | None = None, headers: Any = None) -> None:
        print(f"published {serializer(payload)!r} to {config} as {event.name}")

order_placed = PubSub(OrderPlaced)
store = InMemoryOutboxStore()
outbox = OutboxProducer(store, serializer=lambda order: str(order.order_id).encode())
place_order = outbox.bind(order_placed, factory=OrderPlaced, config=OutboxConfig("orders"))

async def main() -> None:
    async with store.transaction():
        await place_order(order_id=1)
    try:
        async with store.transaction():
            await place_order(order_id=2)
            raise ValueError("payment declined")
    except ValueError:
        pass
    relay = OutboxRelay(store, PrintingPublisher(), outbox)
    print(f"sent {await relay.run_once()}")

asyncio.run(main())
```
```{output}
published b'1' to orders as OrderPlaced
sent 1
```
````
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from inspect import isawaitable
from types import MappingProxyType
from typing import (
    Any,
    Protocol,
    cast,
    overload,
)
from uuid import UUID

from stratae.events.binding import abind
from stratae.events.envelope import ENVELOPE_HEADERS, Envelope
from stratae.events.exceptions import NotConnectedError
from stratae.events.hooks import Hooks
from stratae.events.middleware import MessageHeaders, Middleware
from stratae.events.patterns import NoPayload, PubSub
from stratae.events.pipeline import run_emit_async
from stratae.events.protocols import AnyEvent, EventType, PayloadEvent, SignalEvent

_log = logging.getLogger(__name__)

_PUBSUB_ONLY = (
    "the outbox holds fire-and-forget events only, got {!r}: a request needs its reply now"
)

_ROUTE_TAKEN = (
    "outbox route {!r} is already bound to another event or config; give this binding its"
    " own route, or reuse the same config object"
)

_UNKNOWN_ROUTE = "no outbox binding for route {!r}; bind it before the relay starts"

_ALREADY_RUNNING = "the relay is already running; exit it before entering it again"

_OUTAGES = (NotConnectedError, ConnectionError)
"""Errors that mean the publisher can't reach its broker, so no record is at fault."""


def _now() -> datetime:
    """Return the current time, timezone-aware in UTC."""
    return datetime.now(UTC)


def _raw(body: bytes) -> bytes:
    """Return a stored body unchanged, as the relay's serializer."""
    return body


def _added(headers: Mapping[str, str]) -> dict[str, str]:
    """Return the headers publish middleware added to a record, without its envelope's."""
    return {name: value for name, value in headers.items() if name not in ENVELOPE_HEADERS}


def _require_pubsub(event: AnyEvent) -> None:
    """Refuse an event that isn't fire-and-forget: a request needs its reply now."""
    if not isinstance(event, PubSub):
        raise TypeError(_PUBSUB_ONLY.format(event))


async def _settle[T](result: T | Awaitable[T]) -> T:
    """Return a store method's result, awaiting it if the store is async."""
    if isawaitable(result):
        return await cast(Awaitable[T], result)
    return cast(T, result)


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    """One event waiting in the outbox, as plain data any store can keep."""

    id: UUID
    """The message id, from the envelope it was emitted with. Unique per record."""

    route: str
    """The name its binding registered its event and routing config under."""

    body: bytes
    """The serialized payload."""

    headers: Mapping[str, str]
    """
    The headers it's published with: the envelope it was emitted with, and any its publish
    middleware added.
    """

    created_at: datetime
    """When it was emitted."""

    available_at: datetime
    """When it can next be claimed. Later than `created_at` after a failed attempt."""

    key: str | None = None
    """An ordering key, for a store that keeps order per key."""

    attempts: int = 0
    """How many times publishing it has failed."""

    error: str | None = None
    """Why its last attempt failed."""

    @property
    def envelope(self) -> Envelope:
        """The envelope it was emitted with, to publish it under."""
        return Envelope.from_headers(self.headers)


class OutboxStore(Protocol):
    """
    Where an outbox keeps its records.

    `add` must write in the same transaction as the data the event describes. That's what
    makes the outbox transactional. The rest are called by the relay.

    Each method may be sync or async: a store backed by an async database awaits, and one
    that doesn't need to, doesn't. The outbox awaits whichever returns an awaitable.
    """

    def add(self, record: OutboxRecord) -> Awaitable[None] | None:
        """Add a record, in the current unit of work's transaction."""
        ...

    def claim(self, limit: int) -> Sequence[OutboxRecord] | Awaitable[Sequence[OutboxRecord]]:
        """
        Claim up to `limit` records that are due, oldest first, for one relay.

        A claimed record isn't claimed again until it's marked or rescheduled, or until a
        lease the store chooses runs out, so a relay that dies doesn't strand it.
        """
        ...

    def mark_sent(self, records: Sequence[OutboxRecord]) -> Awaitable[None] | None:
        """Mark records published. The store may keep or delete them."""
        ...

    def reschedule(self, record: OutboxRecord) -> Awaitable[None] | None:
        """Store a record's new attempts, error and `available_at`, and release it."""
        ...

    def mark_failed(self, record: OutboxRecord) -> Awaitable[None] | None:
        """Park a record that won't be retried, keeping it and its `error`."""
        ...


class OutboxConfig[C]:
    """
    How an outbox binding is routed when it's relayed.

    `C` is the publisher's routing config, such as a
    {py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.RabbitMQConfig>`.
    """

    __slots__ = ("config", "key", "route")

    def __init__(
        self,
        config: C,
        *,
        route: str | None = None,
        key: Callable[[Any], str] | None = None,
    ) -> None:
        """
        Create an outbox routing config.

        :param config: The publisher's routing config, used by the relay.
        :param route: The name records store instead of the config. Defaults to the event's
            name.
        :param key: Derives an ordering key from the payload.
        """
        self.config = config
        self.route = route
        self.key = key


class OutboxProducer:
    """
    An async bus that writes events to an outbox store instead of publishing them.

    Bind {py:class}`PubSub <stratae.events.patterns.PubSub>` events to it with `bind`.
    Binding registers the route, so a relay knows every route before anything is emitted.
    Emitting writes a record. Hooks given to `bind` or `emit` run around that write, inside
    the record's envelope, and publish middleware around them. Headers the middleware adds
    are stored with the record, and the relay publishes it with them. Binding a request event
    raises `TypeError`.
    """

    def __init__(
        self,
        store: OutboxStore,
        serializer: Callable[[Any], bytes],
        *,
        middleware: Middleware = Middleware(),
    ) -> None:
        """
        Create an outbox producer.

        :param store: Where records are added.
        :param serializer: Encodes payloads to bytes. A binding's own serializer overrides it.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `publish` half runs around every write, inside the record's envelope;
            its `handle` half is a consumer's.
        :raises TypeError: If any `publish` middleware is a class that isn't a context manager.
        """
        middleware.require(type(self).__name__, asynchronous=True, handle=False)
        self._publish_middleware = middleware.publish
        self._store = store
        self._serializer = serializer
        self._routes: dict[str, tuple[AnyEvent, Any]] = {}

    @property
    def routes(self) -> Mapping[str, tuple[AnyEvent, Any]]:
        """Each route's event and publisher config, for the relay."""
        return MappingProxyType(self._routes)

    @overload
    def bind[**P, S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
        config: OutboxConfig[Any],
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[P, Awaitable[R]]: ...
    @overload
    def bind[R](
        self,
        event: SignalEvent[R, Any],
        *,
        factory: None = None,
        config: OutboxConfig[Any],
        serializer: None = None,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> Callable[[], Awaitable[R]]: ...
    @overload
    def bind[S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: None = None,
        config: OutboxConfig[Any],
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[[S], Awaitable[R]]: ...

    def bind(
        self,
        event: AnyEvent,
        *,
        factory: Callable[..., Any] | None = None,
        config: OutboxConfig[Any],
        serializer: Callable[[Any], Any] | None = None,
        hooks: Hooks[Any, Any] | None = None,
    ) -> Callable[..., Awaitable[Any]]:
        """
        Bind an event to the outbox, registering its route.

        See {py:func}`abind <stratae.events.binding.abind>`.

        :param event: The {py:class}`PubSub <stratae.events.patterns.PubSub>` event to bind.
        :param factory: Builds the payload from the call's arguments, sync or async.
        :param config: The binding's {py:class}`OutboxConfig`.
        :param serializer: Encodes the payload; defaults to the producer's.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each write.
        :returns: An async callable that writes a record.
        :raises TypeError: If the event isn't {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the route is taken by another binding.
        """
        _require_pubsub(event)
        # Registered once here, so the bound callable writes without registering again.
        emit = partial(self._emit_routed, self._register(event, config))
        return abind(
            event, emit, factory=factory, config=config, serializer=serializer, hooks=hooks
        )

    async def emit[S, R](
        self,
        event: EventType[S, R, Any, Any],
        config: OutboxConfig[Any],
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> R:
        """
        Write a payload to the outbox as a record.

        :param event: The event being emitted.
        :param config: The binding's {py:class}`OutboxConfig`.
        :param payload: The payload to write.
        :param serializer: Overrides the producer's serializer for this emit.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around the write,
            inside the record's envelope.
        :raises TypeError: If the event isn't {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the route is taken by another binding.
        """
        _require_pubsub(event)
        route = self._register(event, config)
        return await self._emit_routed(
            route, event, config, payload, serializer=serializer, hooks=hooks
        )

    async def _emit_routed[S, R](
        self,
        route: str,
        event: EventType[S, R, Any, Any],
        config: OutboxConfig[Any],
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> R:
        """Write a payload to the outbox under a route that's already registered."""
        headers = MessageHeaders()
        with Envelope.scope() as envelope:
            args = (event, config, payload, serializer, envelope, route, headers)
            if hooks is None and not self._publish_middleware:
                await self._write(*args)
            else:
                middleware = self._publish_middleware
                await run_emit_async(
                    hooks, middleware, event, config, payload, headers, self._write, *args
                )
        return cast(R, None)

    def _register(self, event: AnyEvent, config: OutboxConfig[Any]) -> str:
        """Record the route's event and publisher config, refusing a clash."""
        route = config.route or event.name
        known = self._routes.setdefault(route, (event, config.config))
        if known[0] != event or (known[1] is not config.config and known[1] != config.config):
            raise ValueError(_ROUTE_TAKEN.format(route))
        return route

    async def _write(
        self,
        event: AnyEvent,
        config: OutboxConfig[Any],
        payload: Any,
        serializer: Callable[[Any], Any] | None,
        envelope: Envelope,
        route: str,
        headers: MessageHeaders,
    ) -> None:
        """Serialize the payload and add it to the store as a record, with its headers."""
        body = b"" if event.is_signal else (serializer or self._serializer)(payload)
        now = _now()
        record = OutboxRecord(
            id=envelope.message_id,
            route=route,
            body=body,
            headers=dict(headers) | envelope.to_headers(),
            created_at=now,
            available_at=now,
            key=config.key(payload) if config.key is not None else None,
        )
        await _settle(self._store.add(record))


class OutboxPublisher(Protocol):
    """
    A publisher the relay can use: one that publishes under a given envelope and headers.

    {py:class}`RabbitMQPublisher <stratae.integrations.rabbitmq.RabbitMQPublisher>` is one.
    """

    def emit(
        self,
        event: Any,
        config: Any,
        payload: Any,
        *,
        serializer: Any = ...,
        envelope: Envelope | None = ...,
        headers: Mapping[str, str] | None = ...,
    ) -> Awaitable[Any]:
        """Publish a payload under `envelope` with `headers`, returning once the broker has it."""
        ...


class OutboxRelay:
    """
    Publishes an outbox's committed records, and marks them sent.

    Enter it with `async with` to relay in the background until it exits, or call
    `run_once` to relay one batch. It polls the store, waiting longer while it's empty, up
    to `max_poll_interval`. `wake` makes it look straight away, for example after a commit.

    A publish that fails for its own record is an attempt. The record is retried after a
    growing delay, then parked as failed once it reaches `max_attempts`. A publish that fails
    because the publisher can't reach its broker isn't an attempt: the relay puts the batch
    back as it was and waits for the broker.
    """

    def __init__(
        self,
        store: OutboxStore,
        publisher: OutboxPublisher,
        outbox: OutboxProducer,
        *,
        batch_size: int = 100,
        poll_interval: float = 0.5,
        max_poll_interval: float = 5.0,
        max_attempts: int | None = 10,
        retry_delay: float = 30.0,
        max_retry_delay: float = 3600.0,
    ) -> None:
        """
        Create a relay.

        :param store: Where records are claimed from and marked.
        :param publisher: Publishes each record under its envelope.
        :param outbox: The producer whose bindings map routes to events and configs.
        :param batch_size: Records claimed at a time.
        :param poll_interval: Seconds between polls while records keep arriving.
        :param max_poll_interval: The longest wait between polls while the store is empty.
        :param max_attempts: Failed attempts before a record is parked as failed; `None` to
            retry forever.
        :param retry_delay: Seconds before a record's first retry. Each retry doubles it.
        :param max_retry_delay: The longest wait before a retry.
        """
        self._store = store
        self._publisher = publisher
        self._routes = outbox.routes
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_poll_interval = max_poll_interval
        self._max_attempts = max_attempts
        self._retry_delay = retry_delay
        self._max_retry_delay = max_retry_delay
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> OutboxRelay:
        """
        Start relaying in the background.

        :returns: `self`.
        :raises RuntimeError: If already running.
        """
        if self._task is not None:
            raise RuntimeError(_ALREADY_RUNNING)
        self._task = asyncio.get_running_loop().create_task(self._run())
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop relaying. A publish in flight is cancelled, and its record relayed later."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    def wake(self) -> None:
        """Poll the store now, instead of at the next interval."""
        self._wake.set()

    async def run_once(self) -> int:
        """
        Claim one batch, publish it, and mark what was sent.

        :returns: How many records were sent.
        """
        return (await self._relay_batch())[1]

    async def _run(self) -> None:
        """Relay batches until cancelled, waiting longer between polls while there's none."""
        wait = self._poll_interval
        while True:
            try:
                claimed, _, outage = await self._relay_batch()
            except Exception:
                _log.exception("outbox relay failed to relay a batch")
                claimed, outage = 0, True
            if claimed == self._batch_size and not outage:
                continue
            wait = self._poll_interval if claimed and not outage else wait
            await self._sleep(wait)
            wait = min(wait * 2, self._max_poll_interval)

    async def _sleep(self, seconds: float) -> None:
        """Wait `seconds`, or until woken."""
        with suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), seconds)
        self._wake.clear()

    async def _relay_batch(self) -> tuple[int, int, bool]:
        """Relay one batch, returning how many were claimed and sent, and if it hit an outage."""
        records = await _settle(self._store.claim(self._batch_size))
        sent: list[OutboxRecord] = []
        outage = False
        try:
            for index, record in enumerate(records):
                try:
                    await self._publish(record)
                except _OUTAGES:
                    _log.warning("outbox relay can't reach the broker; retrying when it can")
                    for waiting in records[index:]:
                        await _settle(self._store.reschedule(waiting))
                    outage = True
                    break
                except Exception as exc:
                    await self._failed(record, exc)
                else:
                    sent.append(record)
        finally:
            if sent:
                await _settle(self._store.mark_sent(sent))
        return len(records), len(sent), outage

    async def _publish(self, record: OutboxRecord) -> None:
        """Publish a record under its envelope and headers, through its route's binding."""
        route = self._routes.get(record.route)
        if route is None:
            raise LookupError(_UNKNOWN_ROUTE.format(record.route))
        event, config = route
        await self._publisher.emit(
            event,
            config,
            record.body,
            serializer=_raw,
            envelope=record.envelope,
            headers=_added(record.headers),
        )

    async def _failed(self, record: OutboxRecord, exc: Exception) -> None:
        """Count a failed attempt: retry the record later, or park it once out of attempts."""
        attempts = record.attempts + 1
        failed = replace(record, attempts=attempts, error=f"{type(exc).__name__}: {exc}")
        if self._max_attempts is not None and attempts >= self._max_attempts:
            _log.error(
                "outbox record %s failed %d time(s); parked as failed",
                record.id,
                attempts,
                exc_info=exc,
            )
            await _settle(self._store.mark_failed(failed))
            return
        delay = min(self._retry_delay * 2 ** (attempts - 1), self._max_retry_delay)
        _log.warning("outbox record %s failed; retrying in %.0fs", record.id, delay, exc_info=exc)
        retry_at = _now() + timedelta(seconds=delay)
        await _settle(self._store.reschedule(replace(failed, available_at=retry_at)))


class InMemoryOutboxStore:
    """
    An {py:class}`OutboxStore` in memory, for tests and examples.

    Records added inside `transaction()` are kept only if it exits cleanly, the way a
    database transaction keeps them. Outside one, they're kept straight away. It doesn't
    survive the process, so it's no use for real delivery.
    """

    def __init__(self, lease: float = 60.0) -> None:
        """
        Create an empty store.

        :param lease: Seconds a claimed record stays claimed if it's never marked, before
            another claim can take it.
        """
        self._lease = timedelta(seconds=lease)
        self._records: dict[UUID, OutboxRecord] = {}
        self._status: dict[UUID, str] = {}
        self._leased: dict[UUID, datetime] = {}
        self._staged: ContextVar[list[OutboxRecord] | None] = ContextVar(
            f"outbox_staged_{id(self)}", default=None
        )

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None, None]:
        """Keep the records added inside only if the block exits cleanly."""
        outer = self._staged.get()
        staged: list[OutboxRecord] = []
        token = self._staged.set(staged)
        try:
            yield
        finally:
            self._staged.reset(token)
        if outer is not None:
            outer.extend(staged)
        else:
            self._keep(staged)

    def pending(self) -> list[OutboxRecord]:
        """Return the records not yet sent or failed, oldest first."""
        return self._with_status("pending")

    def sent(self) -> list[OutboxRecord]:
        """Return the records marked sent, oldest first."""
        return self._with_status("sent")

    def failed(self) -> list[OutboxRecord]:
        """Return the records parked as failed, oldest first."""
        return self._with_status("failed")

    def add(self, record: OutboxRecord) -> None:
        """Add a record, staged if inside a `transaction()`."""
        staged = self._staged.get()
        if staged is not None:
            staged.append(record)
        else:
            self._keep([record])

    def claim(self, limit: int) -> Sequence[OutboxRecord]:
        """Claim up to `limit` due, unleased, pending records, oldest first."""
        now = _now()
        due = [
            record
            for record in self.pending()
            if record.available_at <= now and self._leased.get(record.id, now) <= now
        ][:limit]
        for record in due:
            self._leased[record.id] = now + self._lease
        return due

    def mark_sent(self, records: Sequence[OutboxRecord]) -> None:
        """Mark records sent, releasing them."""
        for record in records:
            self._settle(record, "sent")

    def reschedule(self, record: OutboxRecord) -> None:
        """Store the record's new state and release it, still pending."""
        self._settle(record, "pending")

    def mark_failed(self, record: OutboxRecord) -> None:
        """Park the record as failed, keeping its error."""
        self._settle(record, "failed")

    def _keep(self, records: list[OutboxRecord]) -> None:
        """Store added records as pending."""
        for record in records:
            self._settle(record, "pending")

    def _settle(self, record: OutboxRecord, status: str) -> None:
        """Store a record under a status, releasing any lease."""
        self._records[record.id] = record
        self._status[record.id] = status
        self._leased.pop(record.id, None)

    def _with_status(self, status: str) -> list[OutboxRecord]:
        """Return the records with a status, oldest first."""
        records = [r for r in self._records.values() if self._status[r.id] == status]
        return sorted(records, key=lambda r: r.created_at)
