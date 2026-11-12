"""
Unit tests for the transactional outbox.

This test suite verifies the following behaviors:

OutboxProducer:
- bind registers each binding's route before anything is emitted, under the event's name or
  a given route.
- Emitting writes a record carrying the envelope's message id and headers, the serialized
  body, an ordering key from the payload, and its creation time.
- A publisher config's serializer overrides the producer's; a payload-less event writes an
  empty body.
- Hooks run around the write, inside the record's envelope.
- Publish middleware runs around the write, and the headers it adds are stored with the
  record's envelope headers; middleware setting an envelope header writes nothing.
- A middleware class that isn't a context manager is refused when the producer is built.
- Emitting returns None, and publishes nothing.
- Binding or emitting a request event raises TypeError.
- Emitting an event with no route raises LookupError and writes nothing; route registers one,
  and refuses a route name bound to another event, or a second config for the same event.
- A route bound to another event or config raises ValueError; the same config object, or an
  equal one, binds again.
- OutboxConfig is an immutable value: assignment is refused, and configs with the same fields
  are equal.
- An equal event, built separately, binds an existing route; a different payload type under
  the same name raises ValueError.

InMemoryOutboxStore:
- Records added outside a transaction are kept; inside one, only if it exits cleanly.
- A nested transaction's records join the outer one, kept or dropped with it.
- claim returns due, unclaimed, pending records oldest first, up to the limit.
- A claimed record isn't claimed again until marked, rescheduled, or its lease runs out.
- mark_sent, reschedule and mark_failed move a record to sent, pending and failed.

Relay rules:
- A failed publish counts one more attempt and keeps its error; while the backoff lasts, the
  record is retried the delay after the failure.
- Once the backoff runs out, the record is parked, its availability unchanged.
- A failed publish is logged at its action's level, worded by the action.
- An outage counts no attempt.
- A batch's settlement marks sent records sent, parks or reschedules failed ones in their
  new state, and puts back unchanged every record hit by an outage or with no outcome.
- A retry holds back its key, an outage the batch, and a send or park nothing.
- Lanes start in claim order; a lane goes on after a send or park, stops after a retry, and
  an outage stops every lane.

OutboxRelay:
- Built, it routes each of the outbox's events on its publisher, raising ValueError if the
  publisher routes one elsewhere. An event bound after it's built is relayed too.
- run_once publishes each claimed record under its envelope, with its route's event and
  config and its stored body, then marks them sent.
- A record is published with the headers its middleware added, without its envelope's.
- A route that addresses each payload stores each record's address, and the relay
  publishes the record as an Encoded payload carrying it.
- A failed publish is an attempt: the record is rescheduled with its error, after a delay
  that doubles up to a cap, and isn't claimed before then.
- Once out of attempts, a record is parked as failed with its error; without a limit, never.
- A failed publish that will be retried holds back the rest of its key's records in the
  batch, unchanged and counting no attempt; once the retry is due, the key's records go out
  in order. A record parked as failed holds nothing back.
- An unknown route fails the record's attempt.
- A publisher outage isn't an attempt: the rest of the batch is put back unchanged, and
  what was sent before it is marked sent.
- A relay that stops after publishing but before marking sent publishes again, under the
  same message id.
- Entered with async with, it relays in the background, and wake makes it look at once.
- Entering it twice raises RuntimeError; exiting stops it.
- In the background, a store failure is logged and relaying carries on.
- A full batch is followed straight away by the next.
- With max_in_flight above one, records of different keys, and records with no key, are
  published concurrently up to the limit; records of one key never overlap and go in order.
- With max_in_flight above one, an outage puts back the failing record and every record not
  started, counting no attempt, while publishes already in flight finish and are marked sent;
  a record being retried holds back its key's later records.
- A batch cancelled mid-flight cancels its publishes and marks sent what finished, including
  a key's records sent before the one cancelled.
- max_in_flight below one raises ValueError.
- A poll backoff that runs out raises ValueError.
- By default, a relay retries a record after 30 s, doubling up to an hour, and parks it on
  its 10th failure; it polls after 0.5 s, doubling up to 5 s.
- Polling waits longer while the store stays empty, and starts over once records arrive.
"""

import asyncio
import dataclasses
import inspect
import logging
from collections.abc import Awaitable, Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, assert_type, cast, override
from uuid import UUID

import pytest
from pytest_mock import MockerFixture

from stratae.backoff import Delays, Exponential
from stratae.events import (
    Encoded,
    Envelope,
    EnvelopeConfig,
    Hooks,
    Middleware,
    Producer,
    PublishCall,
    PubSub,
    PubSubProducer,
    Request,
)
from stratae.events.exceptions import NotConnectedError
from stratae.events.failure import Retry
from stratae.events.outbox import (
    InMemoryOutboxStore,
    OutboxConfig,
    OutboxProducer,
    OutboxRecord,
    OutboxRelay,
    _Failed,  # pyright: ignore[reportPrivateUsage]
    _failed_attempt,  # pyright: ignore[reportPrivateUsage]
    _Hold,  # pyright: ignore[reportPrivateUsage]
    _Lanes,  # pyright: ignore[reportPrivateUsage]
    _Outage,  # pyright: ignore[reportPrivateUsage]
    _Park,  # pyright: ignore[reportPrivateUsage]
    _Sent,  # pyright: ignore[reportPrivateUsage]
    _Settlement,  # pyright: ignore[reportPrivateUsage]
    _settlement,  # pyright: ignore[reportPrivateUsage]
)


class _InnerRolledBack(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("inner rolled back")


class _OuterRolledBack(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("outer rolled back")


class _RolledBack(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("rolled back")


class _Order:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


_order_placed = PubSub(_Order)
_heartbeat = PubSub(name="heartbeat")


def _encode(order: _Order) -> bytes:
    return str(order.order_id).encode()


_ROUTED_ELSEWHERE = "{} is routed elsewhere"


class _Publisher:
    """Records each publish. Queued outcomes come first: None publishes, an error raises."""

    def __init__(self, *outcomes: Exception | None) -> None:
        self.published: list[tuple[Any, Any, bytes, Envelope | None]] = []
        self.headers: list[Mapping[str, str] | None] = []
        self.addresses: list[str | None] = []
        self.routes: dict[Any, Any] = {}
        self.outcomes = list(outcomes)

    def route(self, event: Any, config: Any) -> None:
        if self.routes.setdefault(event, config) != config:
            raise ValueError(_ROUTED_ELSEWHERE.format(event.name))

    async def emit(
        self,
        event: Any,
        payload: Encoded,
        *,
        envelope: Envelope | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if outcome is not None:
            raise outcome
        self.published.append((event, self.routes[event], payload.body, envelope))
        self.headers.append(headers)
        self.addresses.append(payload.address)


def _record(store: InMemoryOutboxStore, order_id: int = 1, **changes: Any) -> OutboxRecord:
    """Add a record straight to the store, as the producer would, and return it."""
    envelope = Envelope()
    now = datetime.now(UTC)
    record = OutboxRecord(
        id=envelope.message_id,
        route="_Order",
        body=str(order_id).encode(),
        headers=envelope.to_headers(),
        created_at=now,
        available_at=now,
    )
    record = replace(record, **changes)
    store.add(record)
    return record


class _AsyncStore:
    """
    An async store over an in-memory one, as a database-backed store would be.

    Each method yields to the event loop, as a driver awaiting the database does. A failure
    queued for a method raises on its next call.
    """

    def __init__(self, inner: InMemoryOutboxStore, **failures: Exception) -> None:
        self.inner = inner
        self.failures = failures

    async def _query(self, method: str) -> None:
        await asyncio.sleep(0)
        failure = self.failures.pop(method, None)
        if failure is not None:
            raise failure

    async def add(self, record: OutboxRecord) -> None:
        await self._query("add")
        self.inner.add(record)

    async def claim(self, limit: int) -> Sequence[OutboxRecord]:
        await self._query("claim")
        return self.inner.claim(limit)

    async def mark_sent(self, records: Sequence[OutboxRecord]) -> None:
        await self._query("mark_sent")
        self.inner.mark_sent(records)

    async def reschedule(self, record: OutboxRecord) -> None:
        await self._query("reschedule")
        self.inner.reschedule(record)

    async def mark_failed(self, record: OutboxRecord) -> None:
        await self._query("mark_failed")
        self.inner.mark_failed(record)


class _WatchedStore(InMemoryOutboxStore):
    """An in-memory store a test can wait on, woken each time records are marked sent."""

    def __init__(self) -> None:
        super().__init__()
        self._marked = asyncio.Event()

    @override
    def mark_sent(self, records: Sequence[OutboxRecord]) -> None:
        super().mark_sent(records)
        self._marked.set()
        self._marked = asyncio.Event()

    async def wait_for_sent(self, count: int) -> None:
        """Wait until `count` records are marked sent, failing after a second."""
        async with asyncio.timeout(1):
            while len(self.sent()) < count:
                await self._marked.wait()


@pytest.fixture
def store() -> _WatchedStore:
    """Return an empty in-memory outbox store."""
    return _WatchedStore()


@pytest.fixture
def outbox(store: InMemoryOutboxStore) -> OutboxProducer:
    """Return an outbox producer writing to the store."""
    return OutboxProducer(store, serializer=_encode)


# region: OutboxProducer


def test_bind_registers_routes(outbox: OutboxProducer):
    """
    ``bind`` should register each binding's route before anything is emitted.

    Given: An outbox producer
    When: One event is bound with the default route, and another with a named route
    Then: Both routes map to their event and publisher config
    """
    # Act
    outbox.bind(_order_placed, config=OutboxConfig("orders"))
    outbox.bind(_heartbeat, config=OutboxConfig("pings", route="ping"))

    # Assert
    assert dict(outbox.routes) == {
        "_Order": (_order_placed, "orders"),
        "ping": (_heartbeat, "pings"),
    }


async def test_emit_writes_record(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    Emitting should write a record of the payload and its envelope.

    Given: An event bound with an ordering key
    When: It's emitted inside an envelope scope
    Then: One record holds the envelope's message id and headers, the serialized body and the
          key, created and available at the same moment, and emit returns None
    """
    # Arrange
    place = outbox.bind(
        _order_placed,
        factory=_Order,
        config=OutboxConfig("orders", key=lambda order: f"order-{order.order_id}"),
    )

    # Act
    with Envelope.scope() as parent:
        result = await place(order_id=7)

    # Assert
    (record,) = store.pending()
    assert result is None
    assert record.envelope.causation_id == parent.message_id
    assert record.id == record.envelope.message_id
    assert (record.route, record.body, record.key) == ("_Order", b"7", "order-7")
    assert record.created_at == record.available_at
    assert record.attempts == 0


@dataclasses.dataclass(frozen=True)
class _Encoding:
    """A publisher config with its own serializer."""

    serializer: Callable[[Any], bytes] | None


async def test_route_config_serializer_and_signal_body(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    A publisher config's serializer should override the producer's; a signal has no body.

    Given: An event routed by a publisher config with its own serializer, one routed by a
        config whose serializer is None, and a payload-less event
    When: Each is emitted
    Then: The first body comes from the config's serializer, the second from the producer's,
        and the third is empty
    """
    # Arrange
    place = outbox.bind(
        _order_placed, factory=_Order, config=OutboxConfig(_Encoding(lambda order: b"custom"))
    )
    other = PubSub(_Order, name="other")
    place_other = outbox.bind(other, factory=_Order, config=OutboxConfig(_Encoding(None)))
    ping = outbox.bind(_heartbeat, config=OutboxConfig("pings"))

    # Act
    await place(order_id=1)
    await place_other(order_id=2)
    await ping()

    # Assert
    assert [record.body for record in store.pending()] == [b"custom", b"2", b""]


async def test_hooks_run_inside_record_envelope(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    Hooks should run around the write, inside the record's envelope.

    Given: An event bound with a before hook recording the current envelope
    When: It's emitted
    Then: The hook saw the envelope the record was written with
    """
    # Arrange
    seen: list[Envelope | None] = []
    place = outbox.bind(
        _order_placed,
        factory=_Order,
        config=OutboxConfig("orders"),
        hooks=Hooks(before=(lambda event, payload: seen.append(Envelope.current()),)),
    )

    # Act
    await place(order_id=1)

    # Assert
    (record,) = store.pending()
    assert seen == [record.envelope]


@contextmanager
def _tag_tenant(call: PublishCall) -> Generator[None, None, None]:
    """Publish middleware adding the tenant header, inside the record's envelope."""
    call.headers["x-tenant"] = f"acme {Envelope.current() is not None}"
    yield


async def test_middleware_headers_stored(store: InMemoryOutboxStore):
    """
    Headers publish middleware adds should be stored with the record's envelope headers.

    Given: A producer whose publish middleware adds a tenant header
    When: An event is emitted
    Then: The record holds the tenant header, added inside its envelope, and the envelope's
    """
    # Arrange
    outbox = OutboxProducer(store, _encode, middleware=Middleware(publish=(_tag_tenant,)))
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))

    # Act
    await place(order_id=1)

    # Assert
    (record,) = store.pending()
    assert record.headers == {"x-tenant": "acme True"} | record.envelope.to_headers()


async def test_middleware_reserved_header_writes_nothing(store: InMemoryOutboxStore):
    """
    Publish middleware setting an envelope header should refuse the write.

    Given: A producer whose publish middleware sets the message id header
    When: An event is emitted
    Then: ValueError names the header, and no record is written
    """

    # Arrange
    @contextmanager
    def forge(call: PublishCall) -> Generator[None, None, None]:
        call.headers["x-message-id"] = "forged"
        yield

    outbox = OutboxProducer(store, _encode, middleware=Middleware(publish=(forge,)))
    outbox.route(_order_placed, OutboxConfig("orders"))

    # Act / Assert
    with pytest.raises(ValueError, match="'x-message-id' is a header the bus sets itself"):
        await outbox.emit(_order_placed, _Order(1))
    assert store.pending() == []


def test_non_manager_middleware_refused(store: InMemoryOutboxStore):
    """
    The producer should refuse a middleware class that isn't a context manager.

    Given: Publish middleware that's a plain class
    When: A producer is built with it
    Then: A TypeError says OutboxProducer can't enter it
    """

    # Arrange
    class Plain:
        def __init__(self, call: PublishCall) -> None: ...

    # Act / Assert
    with pytest.raises(TypeError, match="OutboxProducer can't enter"):
        OutboxProducer(store, _encode, middleware=Middleware(publish=(cast(Any, Plain),)))


async def test_request_events_refused(outbox: OutboxProducer):
    """
    The outbox should refuse a request event, which needs its reply straight away.

    Each `# pyright: ignore[...]` marks an expected type error: the project enables
    `reportUnnecessaryTypeIgnoreComment`, so if one stops being an error, the type check fails.

    Given: A request event, and a command (a request without a reply)
    When: Each is bound, routed, and emitted directly
    Then: Each call is a type error, and raises TypeError at runtime
    """
    # Arrange
    request = Request(_Order, reply=str)
    command = Request(_Order)

    # Act / Assert
    for event in (request, command):
        with pytest.raises(TypeError, match="fire-and-forget"):
            outbox.bind(  # pyright: ignore[reportCallIssue]
                event,  # pyright: ignore[reportArgumentType]
                config=OutboxConfig("orders"),
            )
        with pytest.raises(TypeError, match="fire-and-forget"):
            outbox.route(event, OutboxConfig("orders"))  # pyright: ignore[reportArgumentType]
        with pytest.raises(TypeError, match="fire-and-forget"):
            await outbox.emit(event, _Order(1))  # pyright: ignore[reportArgumentType]


def test_outbox_matches_pubsub_producer_only(outbox: OutboxProducer):
    """
    OutboxProducer should match PubSubProducer, and not Producer, which emits any event.

    The `# pyright: ignore[...]` marks an expected type error.

    Given: An outbox producer
    When: It's assigned to PubSubProducer, and to Producer
    Then: The first type-checks, and the second is a type error
    """
    # Act
    pubsub_producer: PubSubProducer = outbox
    producer: Producer = outbox  # pyright: ignore[reportAssignmentType]

    # Assert
    assert isinstance(pubsub_producer, PubSubProducer)
    assert producer is outbox


def test_bound_pubsub_events_are_typed_as_giving_none(outbox: OutboxProducer):
    """
    A PubSub event bound to the outbox should be typed as giving `None`.

    Given: A PubSub event with a payload, and one without
    When: Each is bound to the outbox
    Then: The bound callables are typed as taking the payload, or nothing, and giving `None`
    """
    # Act
    bound = outbox.bind(PubSub(_Order), config=OutboxConfig("orders"))
    signal = outbox.bind(PubSub(name="ping"), config=OutboxConfig("pings"))

    # Assert
    assert_type(bound, Callable[[_Order], Awaitable[None]])
    assert_type(signal, Callable[[], Awaitable[None]])


def test_outbox_config_is_an_immutable_value():
    """
    OutboxConfig should be an immutable value.

    Given: Two configs with the same fields, and one with another route
    When: They're compared, and a field is assigned
    Then: The first two are equal, the third isn't, and assignment raises
    """
    config = OutboxConfig("orders-config", route="orders")

    assert config == OutboxConfig("orders-config", route="orders")
    assert config != OutboxConfig("orders-config", route="other")
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.route = "other"  # pyright: ignore[reportAttributeAccessIssue]


def test_route_clash_refused(outbox: OutboxProducer):
    """
    A route bound to another event or config should be refused.

    Given: An event bound to route "orders" with a config
    When: Another event, or the same event with a different config, binds the same route;
          and the same event binds again with the same config object and an equal one
    Then: The clashes raise ValueError, and the rebinds are accepted
    """
    # Arrange
    config = OutboxConfig("orders-config", route="orders")
    outbox.bind(_order_placed, config=config)

    # Act / Assert
    with pytest.raises(ValueError, match="'orders' is already bound"):
        outbox.bind(_heartbeat, config=OutboxConfig("orders-config", route="orders"))
    with pytest.raises(ValueError, match="already routed to another OutboxConfig"):
        outbox.bind(_order_placed, config=OutboxConfig("other-config", route="orders"))
    outbox.bind(_order_placed, config=config)
    outbox.bind(_order_placed, config=OutboxConfig("orders-config", route="orders"))


async def test_emit_needs_a_route(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    Emitting should write on the event's route, and refuse an event with none.

    Given: An outbox producer with nothing routed
    When: An event is emitted, then routed and emitted again
    Then: The first emit raises LookupError and writes nothing; the second registers the route
        and writes a record
    """
    # Act / Assert
    with pytest.raises(LookupError, match="no outbox route for '_Order'"):
        await outbox.emit(_order_placed, _Order(1))
    assert store.pending() == []

    outbox.route(_order_placed, OutboxConfig("orders", route="orders"))
    await outbox.emit(_order_placed, _Order(1))

    assert dict(outbox.routes) == {"orders": (_order_placed, "orders")}
    assert len(store.pending()) == 1


def test_equal_event_binds_existing_route(outbox: OutboxProducer):
    """
    A route should accept an equal event built separately, and refuse a different one.

    Given: An event bound to its default route
    When: An equal definition built separately binds with the same config, and an event with
          the same name but a different payload type binds the same route
    Then: The equal definition is accepted and the route keeps one event; the different one
          raises ValueError
    """

    # Arrange
    class _OtherOrder:
        pass

    config = OutboxConfig("orders-config")
    outbox.bind(PubSub(_Order), config=config)

    # Act
    outbox.bind(PubSub(_Order), config=config)

    # Assert
    assert dict(outbox.routes) == {"_Order": (_order_placed, "orders-config")}
    with pytest.raises(ValueError, match="'_Order' is already bound"):
        outbox.bind(PubSub(_OtherOrder, name="_Order"), config=config)


# region: InMemoryOutboxStore


async def test_transaction_keeps_or_drops(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    Records should be kept outside a transaction, and inside one only if it exits cleanly.

    Given: An outbox binding
    When: One emit is outside a transaction, one inside a clean one, and one inside a
          failing one
    Then: The first two records are kept, and the third is dropped
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))

    # Act
    await place(order_id=1)
    async with store.transaction():
        await place(order_id=2)

    async def act() -> None:
        async with store.transaction():
            await place(order_id=3)
            raise _RolledBack()

    with pytest.raises(_RolledBack):
        await act()

    # Assert
    assert [record.body for record in store.pending()] == [b"1", b"2"]


async def test_nested_transactions(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    A nested transaction's records should join the outer one, kept or dropped with it.

    Given: An outbox binding
    When: A clean inner transaction runs inside an outer one that fails, then a failing inner
          one inside an outer one that's clean
    Then: Only the clean outer's own record is kept
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))

    async def fail_outer() -> None:
        async with store.transaction():
            async with store.transaction():
                await place(order_id=1)
            raise _OuterRolledBack()

    async def fail_inner() -> None:
        async with store.transaction():
            await place(order_id=3)
            raise _InnerRolledBack()

    # Act
    with pytest.raises(_OuterRolledBack):
        await fail_outer()
    async with store.transaction():
        await place(order_id=2)
        with pytest.raises(_InnerRolledBack):
            await fail_inner()

    # Assert
    assert [record.body for record in store.pending()] == [b"2"]


def test_claim_due_unclaimed_oldest_first(store: InMemoryOutboxStore):
    """
    ``claim`` should return due, unclaimed, pending records oldest first, up to the limit.

    Given: Three due records, one not due until later, and one already sent
    When: Two are claimed, then the rest
    Then: The two oldest come first, then the third; the later and sent ones never do
    """
    # Arrange
    first, second, third = [_record(store, n) for n in (1, 2, 3)]
    later = datetime.now(UTC) + timedelta(hours=1)
    _record(store, 4, available_at=later)
    store.mark_sent([_record(store, 5)])

    # Act
    claimed = [store.claim(2), store.claim(10)]

    # Assert
    assert claimed == [[first, second], [third]]


def test_claimed_record_leased_until_released(store: InMemoryOutboxStore):
    """
    A claimed record shouldn't be claimed again until it's released or its lease runs out.

    Given: A store with a record, and one with a zero lease
    When: The record is claimed, then claimed again, then rescheduled and claimed
    Then: The second claim gets nothing until it's rescheduled; a zero lease expires at once
    """
    # Arrange
    record = _record(store)
    expiring = InMemoryOutboxStore(lease=0)
    _record(expiring)

    # Act
    first = store.claim(10)
    again = store.claim(10)
    store.reschedule(record)
    released = store.claim(10)
    expiring.claim(10)
    expired = expiring.claim(10)

    # Assert
    assert (first, again, released) == ([record], [], [record])
    assert len(expired) == 1


def test_marking_moves_records(store: InMemoryOutboxStore):
    """
    mark_sent, reschedule and mark_failed should move records between states.

    Given: Three records
    When: One is marked sent, one rescheduled with an error, and one marked failed
    Then: Each is listed under its new state, the rescheduled one with its error
    """
    # Arrange
    sent, retried, failed = [_record(store, n) for n in (1, 2, 3)]
    retried = replace(retried, attempts=1, error="boom")

    # Act
    store.mark_sent([sent])
    store.reschedule(retried)
    store.mark_failed(failed)

    # Assert
    assert (store.sent(), store.pending(), store.failed()) == ([sent], [retried], [failed])


# region: OutboxRelay


async def test_run_once_publishes_and_marks_sent(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    run_once should publish each record under its envelope, then mark them sent.

    Given: Two events written to the outbox
    When: The relay runs once
    Then: Each is published with its route's event and config, its stored body, and the
          envelope it was written with; both are marked sent, and it returns 2
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    await place(order_id=1)
    await place(order_id=2)
    publisher = _Publisher()
    records = store.pending()

    # Act
    sent = await OutboxRelay(store, publisher, outbox).run_once()

    # Assert
    assert sent == 2
    assert publisher.published == [
        (_order_placed, "orders", b"1", records[0].envelope),
        (_order_placed, "orders", b"2", records[1].envelope),
    ]
    assert store.sent() == records


async def test_relay_publishes_at_each_records_address(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    A route that addresses each payload should have its records relayed to their addresses.

    Given: An event routed by a config that addresses each payload, and one with a plain config
    When: A payload of each is written, and the relay runs once
    Then: The first record stores its payload's address and the second none, and each is
          published as an Encoded payload carrying its address
    """

    # Arrange
    class _ByOrder:
        def address(self, payload: _Order, /) -> str:
            return f"order.{payload.order_id}"

    by_order = _ByOrder()
    ping = outbox.bind(_heartbeat, config=OutboxConfig("pings"))
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig(by_order))
    await place(order_id=7)
    await ping()
    publisher = _Publisher()

    # Act
    await OutboxRelay(store, publisher, outbox).run_once()

    # Assert
    assert [record.address for record in store.sent()] == ["order.7", None]
    assert publisher.addresses == ["order.7", None]


async def test_relay_publishes_added_headers(store: InMemoryOutboxStore):
    """
    The relay should publish a record with the headers its middleware added.

    Given: A record written by a producer whose publish middleware adds a tenant header
    When: The relay runs once
    Then: The record is published under its envelope, with the tenant header alone
    """
    # Arrange
    outbox = OutboxProducer(store, _encode, middleware=Middleware(publish=(_tag_tenant,)))
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    await place(order_id=1)
    (record,) = store.pending()
    publisher = _Publisher()

    # Act
    await OutboxRelay(store, publisher, outbox).run_once()

    # Assert
    assert publisher.published == [(_order_placed, "orders", b"1", record.envelope)]
    assert publisher.headers == [{"x-tenant": "acme True"}]


def _stored(**changes: Any) -> OutboxRecord:
    """Return a record as a store would hold it, created at the start of 2026."""
    created = datetime(2026, 1, 1, tzinfo=UTC)
    record = OutboxRecord(Envelope().message_id, "_Order", b"1", {}, created, created)
    return replace(record, **changes)


def test_failed_attempt_retries_while_backoff_lasts():
    """
    A failed publish should be retried the backoff's delay after the failure.

    Given: A record with one attempt, and a backoff of 10 then 20 seconds
    When: Its second attempt fails
    Then: It's retried in 20 seconds, with two attempts and the error
    """
    # Arrange
    record = _stored(attempts=1)
    now = record.created_at + timedelta(minutes=5)

    # Act
    outcome = _failed_attempt(record, RuntimeError("refused"), Delays(10, 20), now)

    # Assert
    retried = replace(
        record, attempts=2, error="RuntimeError: refused", available_at=now + timedelta(seconds=20)
    )
    assert outcome == _Failed(retried, Retry(20))


def test_failed_attempt_parks_once_backoff_runs_out():
    """
    A failed publish should park its record once the backoff runs out.

    Given: A record with one attempt, and a backoff of one delay
    When: Its second attempt fails
    Then: It's parked with two attempts and the error, still available when it was
    """
    # Arrange
    record = _stored(attempts=1)

    # Act
    outcome = _failed_attempt(
        record, LookupError("gone"), Delays(10), record.created_at + timedelta(minutes=5)
    )

    # Assert
    assert outcome == _Failed(replace(record, attempts=2, error="LookupError: gone"), _Park())


@pytest.mark.parametrize("exc", [ConnectionError("refused"), NotConnectedError("closed")])
def test_failed_attempt_outage_counts_no_attempt(exc: Exception):
    """
    A publish that can't reach the broker should be an outage, not an attempt.

    Given: A record, and a connection error or a closed publisher
    When: Its publish fails with it
    Then: The outcome is an outage, carrying the record unchanged
    """
    # Arrange
    record = _stored()

    # Act
    outcome = _failed_attempt(record, exc, Delays(10), record.created_at)

    # Assert
    assert outcome == _Outage(record)


def test_settlement_writes_each_outcome():
    """
    A batch's settlement should write each record by its outcome, in claim order.

    Given: Six claimed records: one sent, one retrying, one parked, one hit by an outage, and
           two with no outcome
    When: The batch is settled
    Then: The sent one is marked sent, the parked one parked in its new state, and the
          retrying one goes back in its new state, with the others unchanged
    """
    # Arrange
    sent, retrying, parked, cut_off, held, unstarted = (
        _stored(body=str(n).encode()) for n in range(6)
    )
    retried = replace(retrying, attempts=1, error="RuntimeError: refused")
    failed = replace(parked, attempts=1, error="RuntimeError: refused")
    outcomes = {
        sent.id: _Sent(sent),
        retrying.id: _Failed(retried, Retry(30)),
        parked.id: _Failed(failed, _Park()),
        cut_off.id: _Outage(cut_off),
    }

    # Act
    settlement = _settlement([sent, retrying, parked, cut_off, held, unstarted], outcomes)

    # Assert
    assert settlement == _Settlement((sent,), (failed,), (retried, cut_off, held, unstarted))


def test_outcomes_hold_back_by_kind():
    """
    Each outcome should say what it holds back in its batch.

    Given: A sent record, a retrying one, a parked one, and an outage
    When: What each holds back is read
    Then: A retry holds back its key, an outage the batch, and the rest nothing
    """
    record = _stored()

    assert _Sent(record).holds is _Hold.NOTHING
    assert _Failed(record, Retry(30)).holds is _Hold.KEY
    assert _Failed(record, _Park()).holds is _Hold.NOTHING
    assert _Outage(record).holds is _Hold.BATCH


def test_lanes_follow_what_outcomes_hold_back():
    """
    Lanes should go on, stop, or stop the batch, by what each outcome holds back.

    Given: Lanes for keys a, a, b, b, c, c, d, d, each lane's first record taken
    When: a's record is sent, b's retried, c's parked, then d's hits an outage
    Then: a and c go on in turn, b goes no further, and after the outage nothing is ready
    """
    # Arrange
    records = [_stored(key=key) for key in "aabbccdd"]
    lanes = _Lanes(records)
    taken = [lanes.take() for _ in range(4)]
    (_, a), (_, b), (_, c), (_, d) = taken

    # Act
    lanes.follow(_Sent(records[0]), a)
    lanes.follow(_Failed(records[2], Retry(30)), b)
    lanes.follow(_Failed(records[4], _Park()), c)
    following = [lanes.take()[0] for _ in range(2)]
    lanes.follow(_Outage(records[6]), d)

    # Assert
    assert [record for record, _ in taken] == records[::2]
    assert following == [records[1], records[5]]
    assert (lanes.ready, lanes.stopped) == (False, True)


async def test_failed_publish_retried_after_growing_delay(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    A failed publish should be an attempt, retried after a delay that doubles up to a cap.

    Given: A relay with a 10 second retry delay capped at 15, and a publisher that fails
           twice
    When: The relay runs twice
    Then: The record is rescheduled with its error and attempts, about 10 then 15 seconds out,
          and isn't claimed in between
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    await place(order_id=1)
    publisher = _Publisher(RuntimeError("refused"), RuntimeError("refused again"))
    relay = OutboxRelay(store, publisher, outbox, retry=Exponential(10, cap=15))

    # Act
    await relay.run_once()
    (first,) = store.pending()
    early = await relay.run_once()
    store.reschedule(replace(first, available_at=first.created_at))
    await relay.run_once()
    (second,) = store.pending()

    # Assert
    assert early == 0
    assert (first.attempts, first.error) == (1, "RuntimeError: refused")
    assert (second.attempts, second.error) == (2, "RuntimeError: refused again")
    now = datetime.now(UTC)
    assert timedelta(seconds=14) < second.available_at - now <= timedelta(seconds=15)
    assert timedelta(seconds=9) < first.available_at - first.created_at <= timedelta(seconds=11)
    assert publisher.published == []


async def test_out_of_attempts_parked_as_failed(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    A record out of attempts should be parked as failed; without a limit, never.

    Given: Records failing every publish, under a relay with a limit of one attempt and one
           with no limit
    When: Each relay runs once
    Then: The first record is parked as failed with its error; the second stays pending
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    await place(order_id=1)
    limited = OutboxRelay(store, _Publisher(RuntimeError("refused")), outbox, retry=Delays())
    unlimited_store = InMemoryOutboxStore()
    unlimited_outbox = OutboxProducer(unlimited_store, serializer=_encode)
    await unlimited_outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))(
        order_id=2
    )
    unlimited = OutboxRelay(
        unlimited_store,
        _Publisher(RuntimeError("refused")),
        unlimited_outbox,
        retry=Exponential(0),
    )

    # Act
    await limited.run_once()
    await unlimited.run_once()

    # Assert
    (failed,) = store.failed()
    assert (failed.attempts, failed.error) == (1, "RuntimeError: refused")
    assert [r.attempts for r in unlimited_store.pending()] == [1]
    assert unlimited_store.failed() == []


async def test_failed_publish_logged_by_its_action(
    outbox: OutboxProducer, store: InMemoryOutboxStore, caplog: pytest.LogCaptureFixture
):
    """
    A failed publish should be logged at its action's level, worded by the action.

    Given: A relay retrying once after 5 seconds, and a publisher that fails twice
    When: The relay runs, and runs again once the retry is due
    Then: The retry is a warning and the park an error, each naming the record and attempts
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    await place(order_id=1)
    publisher = _Publisher(RuntimeError("refused"), RuntimeError("refused"))
    relay = OutboxRelay(store, publisher, outbox, retry=Delays(5))

    # Act
    with caplog.at_level(logging.WARNING):
        await relay.run_once()
        (retried,) = store.pending()
        store.reschedule(replace(retried, available_at=retried.created_at))
        await relay.run_once()

    # Assert
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.WARNING, f"outbox record {retried.id} failed after 1 attempt(s); retrying in 5 s"),
        (logging.ERROR, f"outbox record {retried.id} failed after 2 attempt(s); parked as failed"),
    ]


async def test_failure_holds_back_rest_of_its_key(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    A failed publish should hold back the rest of its key's records until it's retried.

    Given: Records for keys a, a, b, a in that order, and a publisher that fails the first
    When: The relay runs, runs again before the retry is due, then once it's due
    Then: Only b is published at first, and the held records count no attempt; nothing more
        goes out early; once due, the three a records are published in order
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    start = datetime.now(UTC) - timedelta(seconds=10)
    keys = ["a", "a", "b", "a"]
    records = [
        _record(store, index, key=key, created_at=start + timedelta(seconds=index))
        for index, key in enumerate(keys)
    ]
    publisher = _Publisher(RuntimeError("refused"))
    relay = OutboxRelay(store, publisher, outbox, retry=Exponential(60))

    # Act
    await relay.run_once()
    first_round = [payload for _, _, payload, _ in publisher.published]
    held = {record.id: record.attempts for record in store.pending()}
    await relay.run_once()
    early = len(publisher.published)
    failed = next(record for record in store.pending() if record.id == records[0].id)
    store.reschedule(replace(failed, available_at=failed.created_at))
    await relay.run_once()

    # Assert
    assert first_round == [b"2"]
    assert held == {records[0].id: 1, records[1].id: 0, records[3].id: 0}
    assert early == 1
    assert [payload for _, _, payload, _ in publisher.published] == [b"2", b"0", b"1", b"3"]


async def test_parked_failure_holds_nothing_back(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    A record parked as failed shouldn't hold back the later records with its key.

    Given: Two records with the same key, a relay allowing one attempt, and a publisher that
        fails the first
    When: The relay runs
    Then: The first is parked as failed, and the second is published in the same batch
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    start = datetime.now(UTC) - timedelta(seconds=10)
    first = _record(store, 1, key="a", created_at=start)
    _record(store, 2, key="a", created_at=start + timedelta(seconds=1))
    publisher = _Publisher(RuntimeError("refused"))
    relay = OutboxRelay(store, publisher, outbox, retry=Delays())

    # Act
    await relay.run_once()

    # Assert
    assert [record.id for record in store.failed()] == [first.id]
    assert [payload for _, _, payload, _ in publisher.published] == [b"2"]


def test_relay_routes_the_outbox_when_built(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    A relay should route each of the outbox's events on its publisher when it's built.

    Given: An outbox with a bound event, and a publisher routing it to a different config
    When: A relay is built on that publisher, and on a fresh one
    Then: The first raises ValueError; the second publisher routes the event, before any
          record is relayed
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    taken, fresh = _Publisher(), _Publisher()
    taken.route(_order_placed, "audit")

    # Act / Assert
    with pytest.raises(ValueError, match="routed elsewhere"):
        OutboxRelay(store, taken, outbox)
    OutboxRelay(store, fresh, outbox)
    assert fresh.routes == {_order_placed: "orders"}


async def test_relay_routes_events_bound_after_it(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    A relay should relay an event bound after it was built.

    Given: A relay built before an event is bound
    When: The event is bound and emitted, and the relay runs once
    Then: The record is published on the event's route
    """
    # Arrange
    publisher = _Publisher()
    relay = OutboxRelay(store, publisher, outbox)
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    await place(order_id=1)

    # Act
    sent = await relay.run_once()

    # Assert
    assert sent == 1
    assert publisher.published[0][:3] == (_order_placed, "orders", b"1")


async def test_unknown_route_fails_attempt(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    A record whose route has no binding should fail its attempt.

    Given: A record on a route nothing bound
    When: The relay runs once
    Then: The record is rescheduled with a LookupError naming the route
    """
    # Arrange
    _record(store, route="nowhere")

    # Act
    await OutboxRelay(store, _Publisher(), outbox).run_once()

    # Assert
    (record,) = store.pending()
    assert (
        record.error
        == "LookupError: no outbox binding for route 'nowhere'; bind it before the relay starts"
    )


@pytest.mark.parametrize("outage", [NotConnectedError("reconnecting"), ConnectionError("reset")])
async def test_outage_is_not_an_attempt(
    outbox: OutboxProducer, store: InMemoryOutboxStore, outage: Exception
):
    """
    A publisher outage shouldn't count as an attempt.

    Given: Three records, and a publisher that sends the first, then can't reach its broker
    When: The relay runs once
    Then: The first is marked sent; the other two are put back unchanged and claimable
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    for n in (1, 2, 3):
        await place(order_id=n)
    records = store.pending()
    publisher = _Publisher(None, outage)

    # Act
    sent = await OutboxRelay(store, publisher, outbox).run_once()

    # Assert
    assert sent == 1
    assert store.sent() == records[:1]
    assert store.pending() == records[1:]
    assert store.claim(10) == records[1:]


async def test_crash_before_marking_sent_resends_same_id():
    """
    A relay that stops after publishing but before marking sent should publish again.

    Given: An async store with a zero lease whose first mark_sent fails, as a crashed relay
           would
    When: The relay runs, fails to mark the record sent, and runs again
    Then: The record is published twice under the same message id, then marked sent
    """
    # Arrange
    memory = InMemoryOutboxStore(lease=0)
    store = _AsyncStore(memory, mark_sent=RuntimeError("relay died"))
    producer = OutboxProducer(store, serializer=_encode)
    await producer.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))(order_id=1)
    publisher = _Publisher()
    relay = OutboxRelay(store, publisher, producer)

    # Act
    with pytest.raises(RuntimeError, match="relay died"):
        await relay.run_once()
    await relay.run_once()

    # Assert
    ids = [envelope.message_id for *_, envelope in publisher.published if envelope]
    assert len(ids) == 2
    assert ids[0] == ids[1]
    assert len(memory.sent()) == 1


async def test_background_relay_woken(outbox: OutboxProducer, store: _WatchedStore):
    """
    Entered with async with, the relay should relay in the background, at once when woken.

    Given: A relay running in the background with a long poll interval
    When: A record is written, and the relay woken
    Then: It's published well before the poll interval, and exiting stops the relay
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    publisher = _Publisher()
    relay = OutboxRelay(store, publisher, outbox, poll=Exponential(60, factor=1))

    # Act
    async with relay:
        await asyncio.sleep(0)
        await place(order_id=1)
        relay.wake()
        await store.wait_for_sent(1)

    # Assert
    assert len(store.sent()) == 1
    await relay.__aexit__(None, None, None)


async def test_relay_entered_twice_refused(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    Entering a running relay again should raise.

    Given: A running relay
    When: It's entered again
    Then: RuntimeError is raised
    """
    # Arrange
    relay = OutboxRelay(store, _Publisher(), outbox)

    # Act / Assert
    async with relay:
        with pytest.raises(RuntimeError, match="already running"):
            await relay.__aenter__()


async def test_background_store_failure_logged(caplog: pytest.LogCaptureFixture):
    """
    In the background, a store failure should be logged, and relaying carry on.

    Given: An async store whose first claim fails, and a pending record
    When: The relay runs in the background with a short poll interval
    Then: The failure is logged, and the record is published on a later poll
    """
    # Arrange
    memory = _WatchedStore()
    store = _AsyncStore(memory, claim=RuntimeError("database gone"))
    producer = OutboxProducer(store, serializer=_encode)
    await producer.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))(order_id=1)
    publisher = _Publisher()

    # Act
    with caplog.at_level(logging.ERROR):
        async with OutboxRelay(store, publisher, producer, poll=Exponential(0.001, factor=1)):
            await memory.wait_for_sent(1)

    # Assert
    assert "outbox relay failed to relay a batch" in caplog.text
    assert len(memory.sent()) == 1


async def test_full_batch_followed_at_once(outbox: OutboxProducer, store: _WatchedStore):
    """
    A full batch should be followed straight away by the next.

    Given: Three records, and a relay with a batch size of two and a long poll interval
    When: The relay runs in the background
    Then: All three are published well before the poll interval
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    for n in (1, 2, 3):
        await place(order_id=n)
    publisher = _Publisher()

    # Act
    async with OutboxRelay(store, publisher, outbox, batch_size=2, poll=Exponential(60, factor=1)):
        await store.wait_for_sent(3)

    # Assert
    assert len(store.sent()) == 3


class _Ids:
    """An id source handing out UUID(int=1), UUID(int=2), and so on, in order."""

    def __init__(self) -> None:
        self.made: list[UUID] = []

    def __call__(self) -> UUID:
        made = UUID(int=len(self.made) + 1)
        self.made.append(made)
        return made


async def test_record_ids_come_from_producer_source(store: InMemoryOutboxStore):
    """
    An outbox producer should draw each record's envelope ids from its id source.

    Given: A producer with a counting id source
    When: An event is written outside any envelope
    Then: The record's id and correlation id are the source's first two
    """
    # Arrange
    outbox = OutboxProducer(store, serializer=_encode, envelope=EnvelopeConfig(new_id=_Ids()))
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))

    # Act
    await place(order_id=7)

    # Assert
    (record,) = store.pending()
    assert record.id == UUID(int=1)
    assert record.envelope.correlation_id == UUID(int=2)


class _OverlapPublisher:
    """
    Publishes after yielding to the loop a few times, recording how many publishes overlap.

    Queued outcomes, by payload, raise instead of publishing; one that's an Event blocks the
    publish until it's set.
    """

    def __init__(self, outcomes: dict[bytes, BaseException | asyncio.Event] | None = None) -> None:
        self.outcomes = outcomes or {}
        self.published: list[bytes] = []
        self.cancelled: list[bytes] = []
        self.active: dict[str | None, int] = {}
        self.peak = 0
        self.peak_per_key = 0
        self.first_published = asyncio.Event()

    def route(self, event: Any, config: Any) -> None:
        return None

    async def emit(self, event: Any, payload: Encoded, **kwargs: Any) -> None:
        body = payload.body
        key = body.decode().split(":")[0] or None
        self.active[key] = self.active.get(key, 0) + 1
        self.peak = max(self.peak, sum(self.active.values()))
        if key is not None:
            self.peak_per_key = max(self.peak_per_key, self.active[key])
        try:
            outcome = self.outcomes.get(body)
            if isinstance(outcome, asyncio.Event):
                await outcome.wait()
            for _ in range(3):
                await asyncio.sleep(0)
            if isinstance(outcome, BaseException):
                raise outcome
            self.published.append(body)
            self.first_published.set()
        except asyncio.CancelledError:
            self.cancelled.append(body)
            raise
        finally:
            self.active[key] -= 1


def _keyed(store: InMemoryOutboxStore, *keys: str | None) -> list[OutboxRecord]:
    """Add one record per key, in order, each with body `key:index`."""
    start = datetime.now(UTC) - timedelta(seconds=10)
    return [
        _record(
            store,
            index,
            key=key,
            body=f"{key or ''}:{index}".encode(),
            created_at=start + timedelta(seconds=index),
        )
        for index, key in enumerate(keys)
    ]


async def test_concurrent_publishes_keep_each_key_in_order(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    With max_in_flight above one, different keys should publish concurrently, each in order.

    Given: Records for keys a, b, a, c, a, and three with no key, under a limit of 4
    When: The relay runs once
    Then: Up to four publish at once, never two of one key, and a's records go in order
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    _keyed(store, "a", "b", "a", "c", "a", None, None, None)
    publisher = _OverlapPublisher()
    relay = OutboxRelay(store, publisher, outbox, max_in_flight=4)

    # Act
    sent = await relay.run_once()

    # Assert
    assert sent == 8
    assert publisher.peak == 4
    assert publisher.peak_per_key == 1
    assert [body for body in publisher.published if body.startswith(b"a:")] == [
        b"a:0",
        b"a:2",
        b"a:4",
    ]
    assert store.pending() == []


async def test_unkeyed_records_publish_concurrently(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    Records with no key should publish concurrently, up to the limit.

    Given: Five records with no key, under a limit of 3
    When: The relay runs once
    Then: Three publish at once, and all five are sent
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    _keyed(store, None, None, None, None, None)
    publisher = _OverlapPublisher()
    relay = OutboxRelay(store, publisher, outbox, max_in_flight=3)

    # Act
    sent = await relay.run_once()

    # Assert
    assert (sent, publisher.peak) == (5, 3)


async def test_concurrent_outage_puts_back_what_hasnt_finished(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    An outage should put back what hasn't started, while publishes in flight finish.

    Given: Four records with different keys under a limit of 2, the first failing with an
        outage
    When: The relay runs once
    Then: The second, in flight with it, is sent; the first, third and fourth go back with
        no attempt counted
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    records = _keyed(store, "a", "b", "c", "d")
    publisher = _OverlapPublisher({b"a:0": ConnectionError("broker gone")})
    relay = OutboxRelay(store, publisher, outbox, max_in_flight=2)

    # Act
    sent = await relay.run_once()

    # Assert
    assert sent == 1
    assert publisher.published == [b"b:1"]
    assert {(r.id, r.attempts) for r in store.pending()} == {
        (records[0].id, 0),
        (records[2].id, 0),
        (records[3].id, 0),
    }


async def test_concurrent_retry_holds_back_its_key(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    With max_in_flight above one, a record being retried should still hold back its key.

    Given: Records for keys a, a, b under a limit of 3, the first failing
    When: The relay runs once
    Then: b is published; the second a goes back with no attempt counted
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    records = _keyed(store, "a", "a", "b")
    publisher = _OverlapPublisher({b"a:0": RuntimeError("refused")})
    relay = OutboxRelay(store, publisher, outbox, max_in_flight=3)

    # Act
    await relay.run_once()

    # Assert
    assert publisher.published == [b"b:2"]
    assert {(r.id, r.attempts) for r in store.pending()} == {
        (records[0].id, 1),
        (records[1].id, 0),
    }


async def test_cancelled_batch_cancels_publishes_in_flight(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    Cancelling a batch should cancel its publishes in flight, and mark sent what finished.

    Given: Two records under a limit of 2, the first blocked until released
    When: The second has been published, and the batch is cancelled
    Then: The first publish is cancelled and stays pending; the second is marked sent
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    records = _keyed(store, "a", "b")
    publisher = _OverlapPublisher({b"a:0": asyncio.Event()})
    relay = OutboxRelay(store, publisher, outbox, max_in_flight=2)
    batch = asyncio.create_task(relay.run_once())
    async with asyncio.timeout(1):
        await publisher.first_published.wait()

    # Act
    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await batch

    # Assert
    assert publisher.cancelled == [b"a:0"]
    assert [r.id for r in store.sent()] == [records[1].id]
    assert [r.id for r in store.pending()] == [records[0].id]


async def test_cancelled_lane_marks_sent_what_it_sent(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    Cancelling a batch should mark sent a key's records sent before the one in flight.

    Given: Two records with one key, the second blocked until released
    When: The first has been published, and the batch is cancelled
    Then: The first is marked sent; the second is cancelled and stays pending
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    records = _keyed(store, "a", "a")
    publisher = _OverlapPublisher({b"a:1": asyncio.Event()})
    relay = OutboxRelay(store, publisher, outbox)
    batch = asyncio.create_task(relay.run_once())
    async with asyncio.timeout(1):
        await publisher.first_published.wait()
    for _ in range(5):
        await asyncio.sleep(0)

    # Act
    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await batch

    # Assert
    assert publisher.cancelled == [b"a:1"]
    assert [r.id for r in store.sent()] == [records[0].id]
    assert [r.id for r in store.pending()] == [records[1].id]


async def test_cancelled_batch_puts_back_what_it_didnt_publish(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    Cancelling a batch should put back at once what it didn't publish, counting no attempt.

    Given: Three records with one key, the first blocked until released, under a long lease
    When: The batch is cancelled, and the store is claimed again
    Then: All three are claimed again straight away, with no attempt counted
    """
    # Arrange
    outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    records = _keyed(store, "a", "a", "a")
    blocked = asyncio.Event()
    publisher = _OverlapPublisher({b"a:0": blocked})
    relay = OutboxRelay(store, publisher, outbox)
    batch = asyncio.create_task(relay.run_once())
    for _ in range(5):
        await asyncio.sleep(0)

    # Act
    batch.cancel()
    with pytest.raises(asyncio.CancelledError):
        await batch
    claimed = store.claim(10)

    # Assert
    assert publisher.cancelled == [b"a:0"]
    assert [(r.id, r.attempts) for r in claimed] == [(r.id, 0) for r in records]


def test_max_in_flight_below_one_refused(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    A relay should refuse a max_in_flight below one.

    Given: A max_in_flight of 0
    When: A relay is built with it
    Then: ValueError names the value
    """
    # Act / Assert
    with pytest.raises(ValueError, match="at least 1, got 0"):
        OutboxRelay(store, _OverlapPublisher(), outbox, max_in_flight=0)


def test_poll_that_runs_out_refused(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    A relay should refuse a poll backoff that runs out, since it polls until it exits.

    Given: A poll backoff with a retry limit
    When: A relay is built with it
    Then: ValueError says it must never run out
    """
    with pytest.raises(ValueError, match="never run out"):
        OutboxRelay(store, _Publisher(), outbox, poll=Exponential(1, retries=3))


def test_relay_default_backoffs():
    """
    A relay should retry after 30 s, doubling to an hour, parking on the 10th failure.

    Given: OutboxRelay's signature
    When: Its retry and poll defaults are read
    Then: Retries wait 30 s, doubling to 3600 s, and run out on the 10th failure; polls wait
          0.5 s, doubling to 5 s, without end
    """
    parameters = inspect.signature(OutboxRelay).parameters
    retry, poll = parameters["retry"].default, parameters["poll"].default

    assert [retry.delay(n) for n in range(1, 11)] == [
        30.0,
        60.0,
        120.0,
        240.0,
        480.0,
        960.0,
        1920.0,
        3600.0,
        3600.0,
        None,
    ]
    assert poll == Exponential(0.5, cap=5.0)


async def test_poll_waits_longer_while_empty_and_starts_over(
    outbox: OutboxProducer, store: InMemoryOutboxStore, mocker: MockerFixture
):
    """
    Polling should wait longer while the store stays empty, and start over once records arrive.

    Given: A relay with the default poll backoff, and an order placed during its third wait
    When: It runs in the background through five waits
    Then: It waits 0.5, 1 and 2 s while empty, then 0.5 s after relaying the order, then 1 s
    """
    # Arrange
    place = outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))
    relay = OutboxRelay(store, _Publisher(), outbox)
    waits: list[float] = []
    fifth = asyncio.Event()

    async def sleep(seconds: float) -> None:
        waits.append(seconds)
        if len(waits) == 3:
            await place(order_id=1)
        if len(waits) == 5:
            fifth.set()
            await asyncio.Event().wait()

    mocker.patch.object(relay, "_sleep", sleep)

    # Act
    async with relay:
        async with asyncio.timeout(1):
            await fifth.wait()

    # Assert
    assert waits == [0.5, 1.0, 2.0, 0.5, 1.0]
    assert len(store.sent()) == 1
