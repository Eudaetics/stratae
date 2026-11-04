"""
Store contract tests, run against every outbox store Stratae ships or documents.

Each test runs against the in-memory store, and against the SQLAlchemy store from the
"An outbox store on SQLAlchemy" tutorial. That store is loaded from the tutorial's own code
blocks, so the recipe readers copy is the code under test.

This test suite verifies the following behaviors:

- A record added in a unit of work that commits is claimable, with every field as written.
- A record added in a unit of work that rolls back is never kept.
- A claim returns due records oldest first, up to its limit; a record not yet due waits.
- A claimed record isn't claimed again until it's rescheduled.
- A claimed record is claimable again once its lease runs out.
- A sent record is never claimed again.
- A rescheduled record keeps its new attempts and error, and is claimable once due.
- A failed record is never claimed again.
"""

from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from inspect import isawaitable
from pathlib import Path
from typing import Any, Protocol

import pytest
from sybil import Sybil
from sybil.parsers.myst import PythonCodeBlockParser

from stratae.events import Envelope
from stratae.events.outbox import InMemoryOutboxStore, OutboxRecord, OutboxStore

_RECIPE = Path(__file__).parents[3] / "docs" / "tutorials" / "outbox.md"


class _RolledBack(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("rolled back")


class _Harness(Protocol):
    """Builds a store, and opens the unit of work its records are added in."""

    def store(self, lease: float = 60.0) -> OutboxStore: ...

    def unit(self, store: OutboxStore) -> AbstractAsyncContextManager[None]: ...


class _MemoryHarness:
    """The in-memory store, whose transaction stands in for a unit of work."""

    def store(self, lease: float = 60.0) -> OutboxStore:
        return InMemoryOutboxStore(lease=lease)

    def unit(self, store: OutboxStore) -> AbstractAsyncContextManager[None]:
        assert isinstance(store, InMemoryOutboxStore)
        return store.transaction()


class _RecipeHarness:
    """The tutorial's SQLAlchemy store, inside the tutorial's own application scope."""

    def __init__(self, recipe: dict[str, Any]) -> None:
        self._recipe = recipe

    def store(self, lease: float = 60.0) -> OutboxStore:
        recipe = self._recipe
        return recipe["SqlAlchemyOutboxStore"](
            recipe["get_session"], recipe["get_sessionmaker"], lease=lease
        )

    def unit(self, store: OutboxStore) -> AbstractAsyncContextManager[None]:
        return self._recipe["work"]


def _load_recipe() -> dict[str, Any]:
    """Evaluate the tutorial's setup blocks with Sybil, as its doc tests do; skip its examples."""
    document = Sybil(parsers=[PythonCodeBlockParser()]).parse(_RECIPE)
    for example in document.examples():
        if "asyncio.run(" not in str(example.parsed):
            example.evaluate()
    return document.namespace


@pytest.fixture(params=["memory", "sqlalchemy"])
async def harness(request: pytest.FixtureRequest) -> AsyncIterator[_Harness]:
    """Yield a harness for each store, with the recipe's application scope active."""
    if request.param == "memory":
        yield _MemoryHarness()
        return
    recipe = _load_recipe()
    async with recipe["application"]:
        yield _RecipeHarness(recipe)


async def _call(result: Any) -> Any:
    """Return a store method's result, awaiting it if the store is async."""
    return await result if isawaitable(result) else result


def _record(order: int, **changes: Any) -> OutboxRecord:
    """Return a record created `order` seconds after a fixed past moment."""
    envelope = Envelope()
    created = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=order)
    record = OutboxRecord(
        id=envelope.message_id,
        route="orders",
        body=f"order {order}".encode(),
        headers=envelope.to_headers(),
        created_at=created,
        available_at=created,
        key=f"key-{order}",
    )
    return replace(record, **changes)


@asynccontextmanager
async def _added(
    harness: _Harness, store: OutboxStore, *records: OutboxRecord
) -> AsyncGenerator[None, None]:
    """Add records in one unit of work, committed when the block exits cleanly."""
    async with harness.unit(store):
        for record in records:
            await _call(store.add(record))
        yield


async def _claim(store: OutboxStore, limit: int = 10) -> Sequence[OutboxRecord]:
    return await _call(store.claim(limit))


async def test_committed_record_claimable_as_written(harness: _Harness):
    """
    A record added in a unit of work that commits should be claimable, as written.

    Given: A record with every field set
    When: It's added in a unit of work that commits, then claimed
    Then: The claimed record equals the one added
    """
    # Arrange
    store = harness.store()
    record = _record(1)

    # Act
    async with _added(harness, store, record):
        pass

    # Assert
    assert await _claim(store) == [record]


async def test_rolled_back_record_never_kept(harness: _Harness):
    """
    A record added in a unit of work that rolls back should never be kept.

    Given: A record
    When: It's added in a unit of work that raises
    Then: Nothing can be claimed
    """
    # Arrange
    store = harness.store()

    # Act
    with pytest.raises(_RolledBack):
        async with _added(harness, store, _record(1)):
            raise _RolledBack()

    # Assert
    assert await _claim(store) == []


async def test_claim_oldest_first_and_due_only(harness: _Harness):
    """
    A claim should return due records oldest first, up to its limit.

    Given: Three due records added out of order, and one not due for an hour
    When: Two are claimed, then the rest
    Then: The two oldest come first, then the third; the one not yet due is never claimed
    """
    # Arrange
    store = harness.store()
    first, second, third = _record(1), _record(2), _record(3)
    later = _record(0, available_at=datetime.now(UTC) + timedelta(hours=1))
    async with _added(harness, store, third, later, first, second):
        pass

    # Act
    claims = [await _claim(store, 2), await _claim(store, 10)]

    # Assert
    assert claims == [[first, second], [third]]


async def test_claimed_record_held_until_rescheduled(harness: _Harness):
    """
    A claimed record shouldn't be claimed again until it's rescheduled.

    Given: A store with a long lease, holding a record
    When: The record is claimed twice, then rescheduled and claimed
    Then: The second claim gets nothing, and the record is claimed again once rescheduled
    """
    # Arrange
    store = harness.store()
    record = _record(1)
    async with _added(harness, store, record):
        pass

    # Act
    claimed = await _claim(store)
    again = await _claim(store)
    await _call(store.reschedule(record))
    released = await _claim(store)

    # Assert
    assert (claimed, again, released) == ([record], [], [record])


async def test_claim_released_when_lease_runs_out(harness: _Harness):
    """
    A claimed record should be claimable again once its lease runs out.

    Given: A store with a zero lease, holding a record
    When: The record is claimed twice, never marked
    Then: The second claim gets it again, as a relay that died would leave it
    """
    # Arrange
    store = harness.store(lease=0)
    record = _record(1)
    async with _added(harness, store, record):
        pass

    # Act
    first = await _claim(store)
    second = await _claim(store)

    # Assert
    assert first == second == [record]


async def test_sent_record_never_claimed_again(harness: _Harness):
    """
    A sent record should never be claimed again.

    Given: A record, claimed
    When: It's marked sent
    Then: Nothing can be claimed, even once its lease would have run out
    """
    # Arrange
    store = harness.store(lease=0)
    record = _record(1)
    async with _added(harness, store, record):
        pass
    await _claim(store)

    # Act
    await _call(store.mark_sent([record]))

    # Assert
    assert await _claim(store) == []


async def test_rescheduled_record_keeps_attempt(harness: _Harness):
    """
    A rescheduled record should keep its new attempts and error, and wait until due.

    Given: A record, claimed
    When: It's rescheduled with an attempt and error, first for an hour from now, then for now
    Then: It isn't claimed while it waits, and is claimed with its attempt and error once due
    """
    # Arrange
    store = harness.store()
    record = _record(1)
    async with _added(harness, store, record):
        pass
    await _claim(store)
    failed = replace(record, attempts=1, error="RuntimeError: refused")

    # Act
    in_an_hour = datetime.now(UTC) + timedelta(hours=1)
    await _call(store.reschedule(replace(failed, available_at=in_an_hour)))
    waiting = await _claim(store)
    await _call(store.reschedule(failed))
    due = await _claim(store)

    # Assert
    assert waiting == []
    assert due == [failed]


async def test_failed_record_never_claimed_again(harness: _Harness):
    """
    A failed record should never be claimed again.

    Given: A record, claimed
    When: It's marked failed
    Then: Nothing can be claimed, even once its lease would have run out
    """
    # Arrange
    store = harness.store(lease=0)
    record = _record(1)
    async with _added(harness, store, record):
        pass
    await _claim(store)

    # Act
    await _call(store.mark_failed(replace(record, attempts=10, error="gave up")))

    # Assert
    assert await _claim(store) == []
