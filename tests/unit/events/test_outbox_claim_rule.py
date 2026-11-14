"""
Tests for the outbox claim rule, as a function.

This test suite verifies the following behaviors:

What a claim takes:
- Due, unleased records, oldest first, up to the limit.
- A record not yet due, or still leased, is left out. A lease that has run out frees it.
- The limit counts records claimed, so a record left out doesn't use up the limit.

Order per key:
- A keyed record left out holds back the later ones with its key, but not other keys.
- A keyed record is taken with the earlier ones of its key when they're all claimable.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from stratae.events import Envelope
from stratae.events.outbox import OutboxRecord
from stratae.events.outbox import _claimable as claimable  # pyright: ignore[reportPrivateUsage]

_NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)
_LATER = _NOW + timedelta(hours=1)


def _record(order: int, key: str | None = None, **changes: object) -> OutboxRecord:
    envelope = Envelope()
    created = _NOW - timedelta(hours=1) + timedelta(seconds=order)
    record = OutboxRecord(
        id=envelope.message_id,
        route="orders",
        body=b"",
        headers=envelope.to_headers(),
        created_at=created,
        available_at=created,
        key=key,
    )
    return replace(record, **changes)


def test_due_unleased_records_oldest_first_up_to_the_limit():
    """
    A claim should take due, unleased records, oldest first, up to the limit.

    Given: Three due records
    When: Two are claimed
    Then: The two oldest are taken
    """
    first, second, third = _record(1), _record(2), _record(3)

    assert claimable([first, second, third], {}, _NOW, 2) == [first, second]


def test_not_due_or_leased_records_left_out():
    """
    A record not yet due, or still leased, should be left out, and freed once its lease ends.

    Given: A record not due for an hour, one leased for an hour, one whose lease has run out
    When: They're claimed
    Then: Only the one whose lease has run out is taken
    """
    waiting = _record(1, available_at=_LATER)
    leased, expired = _record(2), _record(3)
    leases = {leased.id: _LATER, expired.id: _NOW}

    assert claimable([waiting, leased, expired], leases, _NOW, 10) == [expired]


def test_left_out_record_does_not_use_up_the_limit():
    """
    The limit should count records claimed, not records looked at.

    Given: Two records not yet due, then two due ones
    When: Two are claimed
    Then: Both due records are taken
    """
    records = [_record(1, available_at=_LATER), _record(2, available_at=_LATER)]
    due = [_record(3), _record(4)]

    assert claimable([*records, *due], {}, _NOW, 2) == due


def test_left_out_keyed_record_holds_back_its_key_only():
    """
    A keyed record left out should hold back the later ones with its key, not other keys.

    Given: A keyed record waiting for a retry, a later one with its key, and one with another
    When: They're claimed
    Then: Only the record with the other key is taken
    """
    retrying = _record(1, "a", available_at=_LATER)
    later = _record(2, "a")
    other = _record(3, "b")

    assert claimable([retrying, later, other], {}, _NOW, 10) == [other]


def test_leased_keyed_record_holds_back_its_key():
    """
    A keyed record leased by another claim should hold back the later ones with its key.

    Given: A keyed record leased by an earlier claim, and a later one with its key
    When: They're claimed
    Then: Nothing is taken
    """
    leased, later = _record(1, "a"), _record(2, "a")

    assert claimable([leased, later], {leased.id: _LATER}, _NOW, 10) == []


def test_keyed_records_taken_together_in_order():
    """
    Keyed records should be taken together, in creation order, when all are claimable.

    Given: Three due records with one key
    When: They're claimed
    Then: All three are taken, oldest first
    """
    records = [_record(1, "a"), _record(2, "a"), _record(3, "a")]

    assert claimable(records, {}, _NOW, 10) == records
