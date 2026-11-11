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
- **Order per key.** Records with the same {py:class}`OutboxConfig` `key` are published in
  the order they were written, across retries and several relays. A record waiting for a
  retry holds back the later ones with its key. Once it's parked as failed, it stops holding
  them back, as a dead-letter queue does, so a record that can't be sent never stops its key
  for good. Records with no key, or different keys, have no ordering promise: they're
  claimed oldest first, but retries and several relays reorder them.
- **Delivery can still reorder.** The outbox orders what it publishes, not what consumers
  see. A consumer handling several messages at once, several consumers on one queue, and
  retry queues all reorder delivery. A consumer that needs order per key has to keep it
  too.
- **Nothing lost.** A record is either sent, or parked as failed with its reason once it
  has failed `max_attempts` times. A broker outage never counts as an attempt.

````{example} Writing events in a transaction, then relaying the committed ones
```{code-block} python
import asyncio
from typing import Any
from stratae.events import Encoded, Envelope, PubSub
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
    def __init__(self) -> None:
        self.routes: dict[Any, Any] = {}

    def route(self, event: Any, config: Any) -> None:
        self.routes[event] = config

    async def emit(self, event: Any, payload: Encoded, *, envelope: Envelope | None = None,
                   headers: Any = None) -> None:
        print(f"published {payload.body!r} to {self.routes[event]} as {event.name}")

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
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import KW_ONLY, dataclass, field, replace
from datetime import UTC, datetime, timedelta
from functools import partial
from inspect import isawaitable
from types import MappingProxyType
from typing import (
    Any,
    Literal,
    Protocol,
    cast,
    overload,
)
from uuid import UUID

from stratae.events.binding import abind
from stratae.events.envelope import ENVELOPE_HEADERS, Envelope, EnvelopeConfig
from stratae.events.exceptions import NotConnectedError
from stratae.events.hooks import Hooks
from stratae.events.middleware import Middleware
from stratae.events.patterns import NoPayload, PubSub
from stratae.events.pipeline import Outgoing, Publishing, Send, publish_async, received
from stratae.events.protocols import Addressed, AnyEvent, Encoded, EventType, Serializing

_log = logging.getLogger(__name__)

_PUBSUB_ONLY = (
    "the outbox holds fire-and-forget events only, got {!r}: a request needs its reply now"
)

_ROUTE_TAKEN = (
    "outbox route {!r} is already bound to another event or config; give this binding its own route"
)

_UNKNOWN_ROUTE = "no outbox binding for route {!r}; bind it before the relay starts"

_NO_ROUTE = "no outbox route for {!r}; route or bind it to an OutboxConfig before emitting it"

_EVENT_ROUTED = "{!r} is already routed to another OutboxConfig; give it one route per outbox"

_IN_FLIGHT = "max_in_flight must be at least 1, got {}"

_ALREADY_RUNNING = "the relay is already running; exit it before entering it again"

type _Outcome = Literal["sent", "retrying", "parked", "outage"]
"""
What publishing one record came to. A record being retried holds back the later records of
its key; an outage holds back the rest of the batch.
"""

_OUTAGES = (NotConnectedError, ConnectionError)
"""Errors that mean the publisher can't reach its broker, so no record is at fault."""


def _now() -> datetime:
    """Return the current time, timezone-aware in UTC."""
    return datetime.now(UTC)


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

    address: str | None = None
    """
    Where its route addressed it, such as a routing key, if the route addresses each message
    from its payload. The relay publishes it there; see
    {py:class}`Addressed <stratae.events.protocols.Addressed>`.
    """

    key: str | None = None
    """
    An ordering key. Records with the same key are published in the order they were
    written; see {py:meth}`OutboxStore.claim`.
    """

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

        A record with a `key` is claimed only if every earlier record with its key that's
        still pending, neither sent nor parked as failed, is in the same claim. So a record
        waiting for a retry, or claimed by another relay, holds back the later ones with its
        key. Earlier means created first. A store breaks ties in creation time some fixed
        way, such as by id, which time-ordered ids make the order they were written in.
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


@dataclass(frozen=True, slots=True)
class OutboxConfig[C]:
    """
    How an outbox binding is routed when it's relayed.

    `C` is the publisher's routing config, such as a
    {py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.RabbitMQConfig>`. An immutable
    value: configs with the same fields are equal.
    """

    config: C
    """The publisher's routing config, used by the relay."""
    _: KW_ONLY
    route: str | None = None
    """The name records store instead of the config. Defaults to the event's name."""
    key: Callable[[Any], str] | None = None
    """Derives an ordering key from the payload."""


class OutboxProducer:
    """
    An async bus that writes events to an outbox store instead of publishing them.

    Bind {py:class}`PubSub <stratae.events.patterns.PubSub>` events to it with `bind`.
    Binding registers the route, so a relay knows every route before anything is emitted.
    Emitting writes a record. Hooks given to `bind` or `emit` run around that write, inside
    the record's envelope, and publish middleware around them. Headers the middleware adds
    are stored with the record, and the relay publishes it with them.

    An outbox can't wait for a reply, so it takes PubSub events only and matches
    {py:class}`PubSubProducer <stratae.events.protocols.PubSubProducer>`. Binding or emitting a
    request is a type error, and raises `TypeError`.
    """

    def __init__(
        self,
        store: OutboxStore,
        serializer: Callable[[Any], bytes],
        *,
        middleware: Middleware = Middleware(),
        envelope: EnvelopeConfig = EnvelopeConfig(),
    ) -> None:
        """
        Create an outbox producer.

        :param store: Where records are added.
        :param serializer: Encodes payloads to bytes. A route whose publisher config has its
            own serializer, such as a
            {py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.RabbitMQConfig>`, uses
            that one instead; see
            {py:class}`Serializing <stratae.events.protocols.Serializing>`.
        :param middleware: {py:class}`Middleware <stratae.events.middleware.Middleware>`, sync
            or async. Its `publish` half runs around every write, inside the record's envelope;
            its `handle` half is a consumer's.
        :param envelope: How each record's envelope is made. Its ids are random by default.
            Records are claimed oldest first, so time-ordered
            ids, such as `uuid7`'s, suit a store indexed by id.
        :raises TypeError: If any `publish` middleware is a class that isn't a context manager.
        """
        middleware.require(type(self).__name__, asynchronous=True, handle=False)
        self._publishing = Publishing(middleware.publish, envelope.new_id)
        self._store = store
        self._serializer = serializer
        self._routes: dict[str, tuple[AnyEvent, Any]] = {}
        self._writers: dict[str, Send] = {}
        self._by_event: dict[AnyEvent, tuple[str, OutboxConfig[Any]]] = {}

    @property
    def routes(self) -> Mapping[str, tuple[AnyEvent, Any]]:
        """Each route's event and publisher config, for the relay."""
        return MappingProxyType(self._routes)

    def route(self, event: PubSub[Any, Any], config: OutboxConfig[Any]) -> None:
        """
        Route an event's records, registering the route for the relay.

        `bind` routes the event it binds. Routing it again to an equal config does nothing.

        :param event: The {py:class}`PubSub <stratae.events.patterns.PubSub>` event to route.
        :param config: The route's {py:class}`OutboxConfig`.
        :raises TypeError: If the event isn't {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the event, or the route's name, is already routed elsewhere.
        """
        _require_pubsub(event)
        routed = self._by_event.get(event)
        if routed is not None:
            if routed[1] != config:
                raise ValueError(_EVENT_ROUTED.format(event.name))
            return
        name = config.route or event.name
        if self._routes.setdefault(name, (event, config.config)) != (event, config.config):
            raise ValueError(_ROUTE_TAKEN.format(name))
        target = config.config
        serializer = target.serializer if isinstance(target, Serializing) else None
        address = target.address if isinstance(target, Addressed) else None
        self._writers[name] = partial(self._write, name, serializer or self._serializer, address)
        self._by_event[event] = (name, config)

    @overload
    def bind[**P, S](
        self,
        event: PubSub[S, Literal[False]],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
        config: OutboxConfig[Any],
        hooks: Hooks[S, None] | None = None,
    ) -> Callable[P, Awaitable[None]]: ...
    @overload
    def bind(
        self,
        event: PubSub[NoPayload, Literal[True]],
        *,
        factory: None = None,
        config: OutboxConfig[Any],
        hooks: Hooks[NoPayload, None] | None = None,
    ) -> Callable[[], Awaitable[None]]: ...
    @overload
    def bind[S](
        self,
        event: PubSub[S, Literal[False]],
        *,
        factory: None = None,
        config: OutboxConfig[Any],
        hooks: Hooks[S, None] | None = None,
    ) -> Callable[[S], Awaitable[None]]: ...

    def bind(
        self,
        event: AnyEvent,
        *,
        factory: Callable[..., Any] | None = None,
        config: OutboxConfig[Any],
        hooks: Hooks[Any, Any] | None = None,
    ) -> Callable[..., Awaitable[Any]]:
        """
        Bind an event to the outbox, routing it.

        See {py:func}`abind <stratae.events.binding.abind>`.

        :param event: The {py:class}`PubSub <stratae.events.patterns.PubSub>` event to bind.
        :param factory: Builds the payload from the call's arguments, sync or async.
        :param config: The route's {py:class}`OutboxConfig`.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each write.
        :returns: An async callable that writes a record.
        :raises TypeError: If the event isn't {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the event, or the route's name, is already routed elsewhere.
        """
        self.route(cast(PubSub[Any, Any], event), config)
        name, routed = self._by_event[event]
        return abind(event, partial(self._emit_on, name, routed), factory=factory, hooks=hooks)

    async def emit[S](
        self, event: PubSub[S, Any], payload: S, *, hooks: Hooks[S, None] | None = None
    ) -> None:
        """
        Write a payload to the outbox as a record, on the event's route.

        :param event: The PubSub event being emitted.
        :param payload: The payload to write.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around the write,
            inside the record's envelope.
        :raises TypeError: If the event isn't {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises LookupError: If the event has no route.
        """
        _require_pubsub(event)
        routed = self._by_event.get(event)
        if routed is None:
            raise LookupError(_NO_ROUTE.format(event.name))
        await self._emit_on(*routed, event, payload, hooks=hooks)

    def _emit_on[S](
        self,
        name: str,
        config: OutboxConfig[Any],
        event: EventType[S, Any, Any, Any],
        payload: S,
        *,
        hooks: Hooks[S, Any] | None = None,
    ) -> Coroutine[Any, Any, None]:
        """
        Write a payload to the outbox on a route that's already registered.

        Typed for any event, so `bind` can hand it to `abind`. Only PubSub events reach it:
        `bind` and `emit` take nothing else, and refuse anything else at runtime.
        """
        return publish_async(self._publishing, self._writers[name], event, payload, config, hooks)

    async def _write(
        self,
        route: str,
        serializer: Callable[[Any], bytes],
        address: Callable[[Any], str | None] | None,
        message: Outgoing,
    ) -> None:
        """Serialize a message's payload and add it to the store as a record, with its headers."""
        event, config, payload = message.event, message.config, message.payload
        envelope = cast(Envelope, message.envelope)
        body = b"" if event.is_signal else serializer(payload)
        now = _now()
        record = OutboxRecord(
            id=envelope.message_id,
            route=route,
            body=body,
            headers={**received(message.headers), **envelope.to_headers()},
            created_at=now,
            available_at=now,
            key=config.key(payload) if config.key is not None else None,
            address=address(payload) if address is not None else None,
        )
        await _settle(self._store.add(record))


class OutboxPublisher(Protocol):
    """
    A publisher the relay can use.

    It routes events, and publishes an already-encoded payload under a given envelope and
    headers.

    {py:class}`RabbitMQPublisher <stratae.integrations.rabbitmq.RabbitMQPublisher>` is one.
    """

    def route(self, event: Any, config: Any) -> None:
        """Route an event's messages; routing it again to an equal config does nothing."""
        ...

    def emit(
        self,
        event: Any,
        payload: Any,
        *,
        envelope: Envelope | None = ...,
        headers: Mapping[str, str] | None = ...,
    ) -> Awaitable[Any]:
        """
        Publish a payload under `envelope` with `headers`, returning once the broker has it.

        The relay passes an {py:class}`Encoded <stratae.events.protocols.Encoded>` payload,
        which the publisher sends as its bytes.
        """
        ...


@dataclass(slots=True)
class _Batch:
    """What a batch's lanes share: the publish slots, what was sent, and if there's an outage."""

    slots: asyncio.Semaphore
    sent: list[OutboxRecord] = field(default_factory=list[OutboxRecord])
    outage: bool = False


def _lanes(records: Sequence[OutboxRecord]) -> list[list[OutboxRecord]]:
    """Split a batch into lanes: one per key, in claim order, and one per record with no key."""
    lanes: dict[str | UUID, list[OutboxRecord]] = {}
    for record in records:
        lanes.setdefault(record.id if record.key is None else record.key, []).append(record)
    return list(lanes.values())


class OutboxRelay:
    """
    Publishes an outbox's committed records, and marks them sent.

    Enter it with `async with` to relay in the background until it exits, or call
    `run_once` to relay one batch. It polls the store, waiting longer while it's empty, up
    to `max_poll_interval`. `wake` makes it look straight away, for example after a commit.

    A publish that fails for its own record is an attempt. The record is retried after a
    growing delay, then parked as failed once it reaches `max_attempts`. A publish that fails
    because the publisher can't reach its broker isn't an attempt: the relay puts the batch
    back as it was and waits for the broker. A record that will be retried holds back the rest
    of its key's records in the batch: they go back unchanged, without counting an attempt, so
    none is published before it. One parked as failed holds nothing back.

    By default it publishes one record at a time. With `max_in_flight` above one, it keeps up
    to that many publishes waiting on the broker at once, which lifts throughput from about
    one record per round trip. Either way, records with the same key go one at a time, in
    order. Records with different keys, or no key, may be published in any order, as the
    outbox promises them none.

    It routes each of the outbox's events on the publisher, when it's built and again before
    each publish, so routes added later are relayed too. A publisher the app also emits on
    shares those routes. If the app routes an event there to a different config, building the
    relay raises `ValueError`, or a later route's records fail.
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
        max_in_flight: int = 1,
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
        :param max_in_flight: Publishes waiting on the broker at once. At most one per key.
        :raises ValueError: If `max_in_flight` is less than one, or the publisher already
            routes one of the outbox's events to a different config.
        """
        if max_in_flight < 1:
            raise ValueError(_IN_FLIGHT.format(max_in_flight))
        for event, config in outbox.routes.values():
            publisher.route(event, config)
        self._store = store
        self._publisher = publisher
        self._routes = outbox.routes
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._max_poll_interval = max_poll_interval
        self._max_attempts = max_attempts
        self._retry_delay = retry_delay
        self._max_retry_delay = max_retry_delay
        self._max_in_flight = max_in_flight
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
        """
        Relay one batch, returning how many were claimed and sent, and if it hit an outage.

        The batch is split into lanes: one per key, in the order its records were claimed,
        and one per record with no key. Lanes run at once, with at most `max_in_flight`
        publishes in flight between them.
        """
        records = await _settle(self._store.claim(self._batch_size))
        batch = _Batch(asyncio.Semaphore(self._max_in_flight))
        try:
            async with asyncio.TaskGroup() as lanes:
                for lane in _lanes(records):
                    lanes.create_task(self._relay_lane(lane, batch))
        finally:
            if batch.sent:
                await _settle(self._store.mark_sent(batch.sent))
        return len(records), len(batch.sent), batch.outage

    async def _relay_lane(self, lane: list[OutboxRecord], batch: _Batch) -> None:
        """
        Publish a lane's records one by one, in order.

        Once the broker is out of reach, no record anywhere in the batch is attempted: each
        goes back unchanged, counting no attempt. A record that will be retried sends the rest
        of its lane back the same way, so none of its key goes first.
        """
        for index, record in enumerate(lane):
            async with batch.slots:
                outcome = "outage" if batch.outage else await self._attempt(record)
            if outcome == "sent":
                batch.sent.append(record)
            elif outcome == "outage":
                batch.outage = True
                await _settle(self._store.reschedule(record))
            elif outcome == "retrying":
                for waiting in lane[index + 1 :]:
                    await _settle(self._store.reschedule(waiting))
                return

    async def _attempt(self, record: OutboxRecord) -> _Outcome:
        """Publish one record, and return what it came to, settling a failed attempt."""
        try:
            await self._publish(record)
        except _OUTAGES:
            _log.warning("outbox relay can't reach the broker; retrying when it can")
            return "outage"
        except Exception as exc:
            return await self._failed(record, exc)
        return "sent"

    async def _publish(self, record: OutboxRecord) -> None:
        """Publish a record under its envelope and headers, through its route's binding."""
        route = self._routes.get(record.route)
        if route is None:
            raise LookupError(_UNKNOWN_ROUTE.format(record.route))
        event, config = route
        self._publisher.route(event, config)
        await self._publisher.emit(
            event,
            Encoded(record.body, record.address),
            envelope=record.envelope,
            headers=_added(record.headers),
        )

    async def _failed(self, record: OutboxRecord, exc: Exception) -> _Outcome:
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
            return "parked"
        delay = min(self._retry_delay * 2 ** (attempts - 1), self._max_retry_delay)
        _log.warning("outbox record %s failed; retrying in %.0fs", record.id, delay, exc_info=exc)
        retry_at = _now() + timedelta(seconds=delay)
        await _settle(self._store.reschedule(replace(failed, available_at=retry_at)))
        return "retrying"


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
        """
        Claim up to `limit` due, unleased, pending records, oldest first.

        A keyed record waits while an earlier pending record with its key isn't claimed with
        it. Ties in creation time keep the order the records were added in.
        """
        now = _now()
        due: list[OutboxRecord] = []
        held: set[str] = set()
        for record in self.pending():
            if len(due) == limit:
                break
            key = record.key
            if key is not None and key in held:
                continue
            if record.available_at <= now and self._leased.get(record.id, now) <= now:
                due.append(record)
            elif key is not None:
                held.add(key)
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
