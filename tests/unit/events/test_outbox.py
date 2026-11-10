"""
Unit tests for the transactional outbox.

This test suite verifies the following behaviors:

OutboxProducer:
- bind registers each binding's route before anything is emitted, under the event's name or
  a given route.
- Emitting writes a record carrying the envelope's message id and headers, the serialized
  body, an ordering key from the payload, and its creation time.
- A binding's serializer overrides the producer's; a payload-less event writes an empty body.
- Hooks run around the write, inside the record's envelope.
- Publish middleware runs around the write, and the headers it adds are stored with the
  record's envelope headers; middleware setting an envelope header writes nothing.
- A middleware class that isn't a context manager is refused when the producer is built.
- Emitting returns None, and publishes nothing.
- Binding or emitting a request event raises TypeError.
- Emitting without binding registers the route, and refuses a route bound to another event.
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

OutboxRelay:
- run_once publishes each claimed record under its envelope, with its route's event and
  config and its stored body, then marks them sent.
- A record is published with the headers its middleware added, without its envelope's.
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
- A batch cancelled mid-flight cancels its publishes and marks sent what finished.
- max_in_flight below one raises ValueError.
"""

import asyncio
import dataclasses
import logging
from collections.abc import Awaitable, Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, assert_type, cast, override
from uuid import UUID

import pytest

from stratae.events import (
    Envelope,
    EnvelopeConfig,
    Hooks,
    Middleware,
    NoPayload,
    Producer,
    PublishCall,
    PubSub,
    PubSubProducer,
    Request,
)
from stratae.events.exceptions import NotConnectedError
from stratae.events.outbox import (
    InMemoryOutboxStore,
    OutboxConfig,
    OutboxProducer,
    OutboxRecord,
    OutboxRelay,
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


class _Publisher:
    """Records each publish. Queued outcomes come first: None publishes, an error raises."""

    def __init__(self, *outcomes: Exception | None) -> None:
        self.published: list[tuple[Any, Any, bytes, Envelope | None]] = []
        self.headers: list[Mapping[str, str] | None] = []
        self.outcomes = list(outcomes)

    async def emit(
        self,
        event: Any,
        config: Any,
        payload: Any,
        *,
        serializer: Any = None,
        envelope: Envelope | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if outcome is not None:
            raise outcome
        self.published.append((event, config, serializer(payload), envelope))
        self.headers.append(headers)


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


async def test_binding_serializer_and_signal_body(
    outbox: OutboxProducer, store: InMemoryOutboxStore
):
    """
    A binding's serializer should override the producer's; a payload-less event has no body.

    Given: An event bound with its own serializer, and a payload-less event
    When: Both are emitted
    Then: The first body comes from the binding's serializer, and the second is empty
    """
    # Arrange
    place = outbox.bind(
        _order_placed,
        factory=_Order,
        config=OutboxConfig("orders"),
        serializer=lambda order: b"custom",
    )
    ping = outbox.bind(_heartbeat, config=OutboxConfig("pings"))

    # Act
    await place(order_id=1)
    await ping()

    # Assert
    assert [record.body for record in store.pending()] == [b"custom", b""]


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

    # Act / Assert
    with pytest.raises(ValueError, match="'x-message-id' is a header the bus sets itself"):
        await outbox.emit(_order_placed, OutboxConfig("orders"), _Order(1))
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
    When: Each is bound, and emitted directly
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
        await outbox.emit(
            request,  # pyright: ignore[reportArgumentType]
            OutboxConfig("orders"),
            _Order(1),
        )
    with pytest.raises(TypeError, match="fire-and-forget"):
        await outbox.emit(
            command,  # pyright: ignore[reportArgumentType]
            OutboxConfig("orders"),
            _Order(1),
        )


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
    with pytest.raises(ValueError, match="'orders' is already bound"):
        outbox.bind(_order_placed, config=OutboxConfig("other-config", route="orders"))
    outbox.bind(_order_placed, config=config)
    outbox.bind(_order_placed, config=OutboxConfig("orders-config", route="orders"))


async def test_direct_emit_registers_route(outbox: OutboxProducer, store: InMemoryOutboxStore):
    """
    Emitting without binding should still register the route, and refuse a clashing one.

    Given: An outbox producer with nothing bound
    When: An event is emitted directly, then another event is emitted to the same route
    Then: The first registers its route and writes a record; the second raises ValueError
        and writes nothing
    """
    # Act
    await outbox.emit(_order_placed, OutboxConfig("orders", route="orders"), _Order(1))

    # Assert
    assert dict(outbox.routes) == {"orders": (_order_placed, "orders")}
    with pytest.raises(ValueError, match="'orders' is already bound"):
        await outbox.emit(_heartbeat, OutboxConfig("orders", route="orders"), NoPayload())
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
    relay = OutboxRelay(store, publisher, outbox, retry_delay=10, max_retry_delay=15)

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
    limited = OutboxRelay(store, _Publisher(RuntimeError("refused")), outbox, max_attempts=1)
    unlimited_store = InMemoryOutboxStore()
    unlimited_outbox = OutboxProducer(unlimited_store, serializer=_encode)
    await unlimited_outbox.bind(_order_placed, factory=_Order, config=OutboxConfig("orders"))(
        order_id=2
    )
    unlimited = OutboxRelay(
        unlimited_store,
        _Publisher(RuntimeError("refused")),
        unlimited_outbox,
        max_attempts=None,
        retry_delay=0,
    )

    # Act
    await limited.run_once()
    await unlimited.run_once()

    # Assert
    (failed,) = store.failed()
    assert (failed.attempts, failed.error) == (1, "RuntimeError: refused")
    assert [r.attempts for r in unlimited_store.pending()] == [1]
    assert unlimited_store.failed() == []


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
    relay = OutboxRelay(store, publisher, outbox, retry_delay=60)

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
    relay = OutboxRelay(store, publisher, outbox, max_attempts=1)

    # Act
    await relay.run_once()

    # Assert
    assert [record.id for record in store.failed()] == [first.id]
    assert [payload for _, _, payload, _ in publisher.published] == [b"2"]


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
    relay = OutboxRelay(store, publisher, outbox, poll_interval=60, max_poll_interval=60)

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
        async with OutboxRelay(
            store, publisher, producer, poll_interval=0.001, max_poll_interval=0.001
        ):
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
    async with OutboxRelay(
        store, publisher, outbox, batch_size=2, poll_interval=60, max_poll_interval=60
    ):
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

    async def emit(self, event: Any, config: Any, payload: Any, **kwargs: Any) -> None:
        body = kwargs["serializer"](payload)
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
