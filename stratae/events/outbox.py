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
- **Nothing lost.** A record is either sent, or parked as failed with its reason once its
  `retry` backoff runs out. A broker outage never counts as an attempt.

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
import threading
from collections import deque
from collections.abc import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import KW_ONLY, dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum, auto
from functools import partial
from inspect import isawaitable
from types import MappingProxyType
from typing import (
    Any,
    ClassVar,
    Literal,
    Protocol,
    cast,
    overload,
)
from uuid import UUID

from stratae.backoff import Backoff, Exponential
from stratae.events.binding import abind
from stratae.events.envelope import ENVELOPE_HEADERS, Envelope, EnvelopeConfig
from stratae.events.exceptions import NotConnectedError
from stratae.events.failure import Retry, failure_action, failure_reason
from stratae.events.hooks import Hooks
from stratae.events.middleware import Middleware
from stratae.events.patterns import NoPayload, PubSub
from stratae.events.pipeline import Outgoing, Publishing, Send, publish_async, received
from stratae.events.protocols import (
    Addressed,
    AnyEvent,
    Destination,
    Encoded,
    EventType,
    Operation,
    Serializing,
    destination_of,
)

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

_FINITE_POLL = "the relay's poll backoff must never run out, since it polls until it exits"

_POLL = Exponential(0.5, cap=5.0)
"""How long a relay waits after each poll in a row that doesn't fill a batch, by default."""

_RETRY = Exponential(30.0, cap=3600.0, retries=9)
"""How long a relay waits before retrying a failed record, by default; parked on the 10th."""

_ALREADY_RUNNING = "the relay is already running; exit it before entering it again"

_OUTAGES = (NotConnectedError, ConnectionError)
"""Errors that mean the publisher can't reach its broker, so no record is at fault."""

_CREATE = Operation("create", "create")


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
    {py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.config.RabbitMQConfig>`. An immutable
    value: configs with the same fields are equal.

    It describes itself to observers such as tracing. Its operation is `create`, as
    {py:class}`Operating <stratae.events.protocols.Operating>` describes: an emit creates a
    record, and the relay sends it later. Its destination is the publisher's config's, as
    {py:class}`Routed <stratae.events.protocols.Routed>` describes.
    """

    config: C
    """The publisher's routing config, used by the relay."""
    _: KW_ONLY
    route: str | None = None
    """The name records store instead of the config. Defaults to the event's name."""
    key: Callable[[Any], str] | None = None
    """Derives an ordering key from the payload."""

    @property
    def messaging_operation(self) -> Operation:
        """Creating a record, which the relay sends later."""
        return _CREATE

    @property
    def messaging_destination(self) -> Destination | None:
        """Where the relay sends this binding's records, if the publisher's config says."""
        return destination_of(self.config)


@dataclass(frozen=True, slots=True)
class _Route:
    """One outbox route: its name, its event and config, and the writer that adds its records."""

    name: str
    event: AnyEvent
    config: OutboxConfig[Any]
    send: Send


@dataclass(frozen=True, slots=True)
class _Routes:
    """
    Every route of an outbox producer, by name for the relay and by event for emits.

    It's never changed. Adding a route makes a new one, so a reader sees all of one or the other.
    """

    by_name: Mapping[str, tuple[AnyEvent, Any]]
    """Each route's event and publisher config."""
    by_event: Mapping[AnyEvent, _Route]
    """Each event's route."""

    def routed(self, event: AnyEvent, config: OutboxConfig[Any]) -> _Route | None:
        """
        Return the event's route, or `None` if it has none.

        :raises ValueError: If the event is routed to a different config.
        """
        routed = self.by_event.get(event)
        if routed is not None and routed.config != config:
            raise ValueError(_EVENT_ROUTED.format(event.name))
        return routed

    def add(self, route: _Route) -> _Routes:
        """
        Return these routes with one more.

        :raises ValueError: If the route's name holds another event or publisher config.
        """
        target = (route.event, route.config.config)
        if self.by_name.get(route.name, target) != target:
            raise ValueError(_ROUTE_TAKEN.format(route.name))
        return _Routes(
            MappingProxyType({**self.by_name, route.name: target}),
            MappingProxyType({**self.by_event, route.event: route}),
        )


_NO_ROUTES = _Routes(MappingProxyType({}), MappingProxyType({}))


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
            {py:class}`RabbitMQConfig <stratae.integrations.rabbitmq.config.RabbitMQConfig>`, uses
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
        self._routes = _NO_ROUTES
        self._routing = threading.Lock()

    @property
    def routes(self) -> Mapping[str, tuple[AnyEvent, Any]]:
        """Each route's event and publisher config, for the relay, as they are now."""
        return self._routes.by_name

    def route(self, event: PubSub[Any, Any], config: OutboxConfig[Any]) -> None:
        """
        Route an event's records, registering the route for the relay.

        `bind` routes the event it binds. Routing it again to an equal config does nothing.

        :param event: The {py:class}`PubSub <stratae.events.patterns.PubSub>` event to route.
        :param config: The route's {py:class}`OutboxConfig`.
        :raises TypeError: If the event isn't {py:class}`PubSub <stratae.events.patterns.PubSub>`.
        :raises ValueError: If the event, or the route's name, is already routed elsewhere.
        """
        self._route(event, config)

    def _route(self, event: AnyEvent, config: OutboxConfig[Any]) -> _Route:
        """Return the event's route, adding it if the event has none."""
        _require_pubsub(event)
        routed = self._routes.routed(event, config)
        if routed is not None:
            return routed
        name = config.route or event.name
        route = _Route(name, event, config, self._writer(name, config.config))
        with self._routing:
            routes = self._routes
            routed = routes.routed(event, config)
            if routed is not None:
                return routed
            self._routes = routes.add(route)
        return route

    def _writer(self, name: str, target: Any) -> Send:
        """Return what writes a route's records, with its config's serializer and address."""
        serializer = target.serializer if isinstance(target, Serializing) else None
        address = target.address if isinstance(target, Addressed) else None
        return partial(self._write, name, serializer or self._serializer, address)

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
        route = self._route(event, config)
        return abind(event, partial(self._emit_on, route), factory=factory, hooks=hooks)

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
        route = self._routes.by_event.get(event)
        if route is None:
            raise LookupError(_NO_ROUTE.format(event.name))
        await self._emit_on(route, event, payload, hooks=hooks)

    def _emit_on[S](
        self,
        route: _Route,
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
        return publish_async(self._publishing, route.send, event, payload, route.config, hooks)

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

    {py:class}`RabbitMQPublisher <stratae.integrations.rabbitmq.publisher.RabbitMQPublisher>` is
    one.
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


class _Hold(Enum):
    """What a publish's outcome holds back in its batch."""

    NOTHING = auto()
    """The rest of its lane goes on."""
    KEY = auto()
    """The rest of its lane goes back unchanged."""
    BATCH = auto()
    """Nothing more in the batch is attempted."""


@dataclass(frozen=True, slots=True)
class _Sent:
    """A record its publisher took."""

    record: OutboxRecord
    holds: ClassVar[_Hold] = _Hold.NOTHING


@dataclass(frozen=True, slots=True)
class _Park:
    """Park a record as failed, so it stops holding back its key."""

    level: ClassVar[int] = logging.ERROR

    def describe(self) -> str:
        """Word the park for a log line."""
        return "parked as failed"


@dataclass(frozen=True, slots=True)
class _Failed:
    """A record whose publish failed, in its new state, and what to do with it."""

    record: OutboxRecord
    action: Retry | _Park

    @property
    def holds(self) -> _Hold:
        """A record to be retried holds back its key, so none of its key goes first."""
        return _Hold.KEY if isinstance(self.action, Retry) else _Hold.NOTHING


@dataclass(frozen=True, slots=True)
class _Outage:
    """The publisher couldn't reach its broker, so `record` isn't at fault."""

    record: OutboxRecord
    holds: ClassVar[_Hold] = _Hold.BATCH


type _Outcome = _Sent | _Failed | _Outage
"""What publishing one record came to, and what it holds back."""


def _failed_attempt(
    record: OutboxRecord, exc: Exception, retry: Backoff, now: datetime
) -> _Failed | _Outage:
    """
    Return what a publish that raised `exc` at `now` came to.

    An outage isn't the record's fault, and counts no attempt. Any other error counts one,
    and keeps the error on the record. The record is retried the `retry` delay after `now`,
    or parked once `retry` runs out.
    """
    if isinstance(exc, _OUTAGES):
        return _Outage(record)
    attempts = record.attempts + 1
    action = failure_action(retry, _Park(), attempts, retryable=True)
    failed = replace(record, attempts=attempts, error=failure_reason(exc))
    if isinstance(action, Retry):
        failed = replace(failed, available_at=now + timedelta(seconds=action.delay))
    return _Failed(failed, action)


@dataclass(frozen=True, slots=True)
class _Settlement:
    """The store writes that end a batch."""

    sent: tuple[OutboxRecord, ...]
    parked: tuple[OutboxRecord, ...]
    back: tuple[OutboxRecord, ...]


def _settlement(records: Sequence[OutboxRecord], outcomes: Mapping[UUID, _Outcome]) -> _Settlement:
    """
    Return the store writes that end a batch, from its claimed records and their outcomes.

    A record sent is marked sent. One that failed is parked or rescheduled in its new state.
    The rest go back unchanged, counting no attempt: one hit by an outage, and one an outage,
    a retry of its key or a cancellation stopped before it started.
    """
    sent: list[OutboxRecord] = []
    parked: list[OutboxRecord] = []
    back: list[OutboxRecord] = []
    for record in records:
        match outcomes.get(record.id):
            case _Sent():
                sent.append(record)
            case _Failed(failed, _Park()):
                parked.append(failed)
            case _Failed(failed):
                back.append(failed)
            case _:
                back.append(record)
    return _Settlement(tuple(sent), tuple(parked), tuple(back))


def _lanes(records: Sequence[OutboxRecord]) -> list[list[OutboxRecord]]:
    """Split a batch into lanes: one per key, in claim order, and one per record with no key."""
    lanes: dict[str | UUID, list[OutboxRecord]] = {}
    for record in records:
        lanes.setdefault(record.id if record.key is None else record.key, []).append(record)
    return list(lanes.values())


class _Lanes:
    """
    A batch's lanes waiting to publish their next record, and whether the batch has stopped.

    Only the coroutine relaying the batch uses it. A lane is out while its record publishes,
    and goes back by what that record's outcome holds back.
    """

    def __init__(self, records: Sequence[OutboxRecord]) -> None:
        """Queue a batch's lanes, in claim order."""
        self._waiting = deque(deque(lane) for lane in _lanes(records))
        self.stopped = False

    @property
    def ready(self) -> bool:
        """Whether a lane has a record to start."""
        return bool(self._waiting) and not self.stopped

    def take(self) -> tuple[OutboxRecord, deque[OutboxRecord]]:
        """Take the next waiting lane's next record, with the rest of its lane."""
        lane = self._waiting.popleft()
        return lane.popleft(), lane

    def follow(self, outcome: _Outcome, lane: deque[OutboxRecord]) -> None:
        """Put a lane back after its record's outcome, unless the outcome holds it back."""
        if outcome.holds is _Hold.BATCH:
            self.stopped = True
        elif outcome.holds is _Hold.NOTHING and lane:
            self._waiting.append(lane)


async def _cancel(running: Iterable[asyncio.Task[_Outcome]]) -> list[_Outcome]:
    """Cancel publishes in flight, returning the outcomes of any that finished first."""
    tasks = list(running)
    for task in tasks:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [result for result in results if isinstance(result, _Sent | _Failed | _Outage)]


class OutboxRelay:
    """
    Publishes an outbox's committed records, and marks them sent.

    Enter it with `async with` to relay in the background until it exits, or call
    `run_once` to relay one batch. A poll that fills a batch is followed at once. After any
    other, it waits by its `poll` backoff, counting the polls since records last arrived, so
    it waits longer while the store stays empty. `wake` makes it look straight away, for
    example after a commit.

    A publish that fails for its own record is an attempt. The record is retried after each
    delay of its `retry` backoff, then parked as failed once the backoff runs out. A publish
    that fails because the publisher can't reach its broker isn't an attempt: the relay puts
    the batch back as it was and waits for the broker. A record that will be retried holds
    back the rest of its key's records in the batch: they go back unchanged, without counting
    an attempt, so none is published before it. One parked as failed holds nothing back.

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
        poll: Backoff = _POLL,
        retry: Backoff = _RETRY,
        max_in_flight: int = 1,
    ) -> None:
        """
        Create a relay.

        :param store: Where records are claimed from and marked.
        :param publisher: Publishes each record under its envelope.
        :param outbox: The producer whose bindings map routes to events and configs.
        :param batch_size: Records claimed at a time.
        :param poll: The wait after each poll in a row that doesn't fill a batch, counted from
            the last that found records. It must never run out. By default 0.5 s, doubling up
            to 5 s.
        :param retry: The wait before each retry of a record that failed. Once it runs out,
            the record is parked as failed. By default 30 s, doubling up to an hour, with the
            record parked on its 10th failure.
        :param max_in_flight: Publishes waiting on the broker at once. At most one per key.
        :raises ValueError: If `poll` runs out, `max_in_flight` is less than one, or the
            publisher already routes one of the outbox's events to a different config.
        """
        if poll.retries is not None:
            raise ValueError(_FINITE_POLL)
        if max_in_flight < 1:
            raise ValueError(_IN_FLIGHT.format(max_in_flight))
        for event, config in outbox.routes.values():
            publisher.route(event, config)
        self._store = store
        self._publisher = publisher
        self._outbox = outbox
        self._batch_size = batch_size
        self._poll = poll
        self._retry = retry
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
        """
        Stop relaying.

        A publish in flight is cancelled. Its record, and those of its batch not yet
        published, go back to the store, counting no attempt.
        """
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
        idle = 0
        while True:
            try:
                claimed, _, outage = await self._relay_batch()
            except Exception:
                _log.exception("outbox relay failed to relay a batch")
                claimed, outage = 0, True
            if claimed and not outage:
                idle = 0
                if claimed == self._batch_size:
                    continue
            idle += 1
            await self._sleep(cast(float, self._poll.delay(idle)))

    async def _sleep(self, seconds: float) -> None:
        """Wait `seconds`, or until woken."""
        with suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), seconds)
        self._wake.clear()

    async def _relay_batch(self) -> tuple[int, int, bool]:
        """
        Relay one batch, returning how many were claimed and sent, and if it hit an outage.

        The batch is split into lanes: one per key, in the order its records were claimed,
        and one per record with no key. Each lane publishes one record at a time, with at most
        `max_in_flight` publishes in flight between lanes. Each publish returns its outcome
        here, and what the outcome holds back decides whether its lane goes on. However the
        batch ends, even cancelled, what its publishes came to is written to the store.
        """
        records = await _settle(self._store.claim(self._batch_size))
        lanes = _Lanes(records)
        running: dict[asyncio.Task[_Outcome], deque[OutboxRecord]] = {}
        outcomes: dict[UUID, _Outcome] = {}
        try:
            while running or lanes.ready:
                self._start(lanes, running)
                done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    outcome = task.result()
                    outcomes[outcome.record.id] = outcome
                    lanes.follow(outcome, running.pop(task))
        finally:
            outcomes.update((outcome.record.id, outcome) for outcome in await _cancel(running))
            settlement = _settlement(records, outcomes)
            await self._write(settlement)
        return len(records), len(settlement.sent), lanes.stopped

    def _start(
        self, lanes: _Lanes, running: dict[asyncio.Task[_Outcome], deque[OutboxRecord]]
    ) -> None:
        """Start the next records of waiting lanes, up to `max_in_flight` publishes at once."""
        while lanes.ready and len(running) < self._max_in_flight:
            record, lane = lanes.take()
            running[asyncio.create_task(self._attempt(record))] = lane

    async def _write(self, settlement: _Settlement) -> None:
        """Write a batch's settlement to the store, marking what was sent first."""
        if settlement.sent:
            await _settle(self._store.mark_sent(settlement.sent))
        for record in settlement.parked:
            await _settle(self._store.mark_failed(record))
        for record in settlement.back:
            await _settle(self._store.reschedule(record))

    async def _attempt(self, record: OutboxRecord) -> _Outcome:
        """Publish one record, and return what it came to."""
        try:
            await self._publish(record)
        except Exception as exc:
            outcome = _failed_attempt(record, exc, self._retry, _now())
            if isinstance(outcome, _Outage):
                _log.warning("outbox relay can't reach the broker; retrying when it can")
            else:
                failed, action = outcome.record, outcome.action
                _log.log(
                    action.level,
                    "outbox record %s failed after %d attempt(s); %s",
                    failed.id,
                    failed.attempts,
                    action.describe(),
                    exc_info=exc,
                )
            return outcome
        return _Sent(record)

    async def _publish(self, record: OutboxRecord) -> None:
        """Publish a record under its envelope and headers, through its route's binding."""
        route = self._outbox.routes.get(record.route)
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
