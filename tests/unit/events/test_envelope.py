"""Unit test suite for EventEnvelope and scoped_envelope."""

import asyncio
from datetime import timezone

import pytest

from stratae.events.envelope import EventEnvelope, scoped_envelope


def test_envelope_default_fields():
    """
    Default fields are populated with unique identifiers and a UTC timestamp.

    Given: No arguments.
    When: An EventEnvelope is created.
    Then: message_id and correlation_id are set, causation_id is None, timestamp is UTC.
    """
    # Act
    envelope = EventEnvelope()

    # Assert
    assert envelope.message_id is not None
    assert envelope.correlation_id is not None
    assert envelope.causation_id is None
    assert envelope.timestamp.tzinfo == timezone.utc


def test_envelope_unique_message_ids():
    """
    Each envelope receives a distinct message_id.

    Given: No arguments.
    When: Two EventEnvelopes are created.
    Then: Their message_ids differ.
    """
    # Act
    a = EventEnvelope()
    b = EventEnvelope()

    # Assert
    assert a.message_id != b.message_id


def test_envelope_unique_correlation_ids():
    """
    Independent envelopes each start their own correlation chain.

    Given: No arguments.
    When: Two EventEnvelopes are created independently.
    Then: Their correlation_ids differ.
    """
    # Act
    a = EventEnvelope()
    b = EventEnvelope()

    # Assert
    assert a.correlation_id != b.correlation_id


def test_child_inherits_correlation_id():
    """
    A child envelope stays in the same correlation chain as its parent.

    Given: An EventEnvelope.
    When: A child envelope is created from it.
    Then: The child's correlation_id matches the parent's.
    """
    # Arrange
    parent = EventEnvelope()

    # Act
    child = parent.child()

    # Assert
    assert child.correlation_id == parent.correlation_id


def test_child_causation_id_is_parent_message_id():
    """
    A child envelope records its parent as the cause.

    Given: An EventEnvelope.
    When: A child envelope is created from it.
    Then: The child's causation_id equals the parent's message_id.
    """
    # Arrange
    parent = EventEnvelope()

    # Act
    child = parent.child()

    # Assert
    assert child.causation_id == parent.message_id


def test_child_has_distinct_message_id():
    """
    A child envelope is its own message, not a copy of its parent.

    Given: An EventEnvelope.
    When: A child envelope is created from it.
    Then: The child's message_id differs from the parent's.
    """
    # Arrange
    parent = EventEnvelope()

    # Act
    child = parent.child()

    # Assert
    assert child.message_id != parent.message_id


def test_current_returns_none_outside_context():
    """
    current() returns None when no envelope is active.

    Given: No active scoped_envelope context.
    When: EventEnvelope.current() is called.
    Then: None is returned.
    """
    # Act & Assert
    assert EventEnvelope.current() is None


def test_scoped_envelope_sets_current():
    """
    The active envelope is accessible via current() inside the context block.

    Given: A scoped_envelope context.
    When: EventEnvelope.current() is called inside it.
    Then: The returned envelope matches the one yielded by the context manager.
    """
    # Act & Assert
    with scoped_envelope() as envelope:
        assert EventEnvelope.current() is envelope


def test_scoped_envelope_root_has_no_causation():
    """
    A root context has no prior cause.

    Given: No existing context.
    When: A scoped_envelope is entered without arguments.
    Then: The yielded envelope has causation_id of None.
    """
    # Act & Assert
    with scoped_envelope() as envelope:
        assert envelope.causation_id is None


def test_scoped_envelope_nested_creates_child():
    """
    Nesting a scoped_envelope automatically creates a child of the current context.

    Given: An active scoped_envelope context.
    When: A second scoped_envelope is entered without arguments.
    Then: The inner envelope's causation_id equals the outer envelope's message_id.
    """
    # Act & Assert
    with scoped_envelope() as outer:
        with scoped_envelope() as inner:
            assert inner.correlation_id == outer.correlation_id
            assert inner.causation_id == outer.message_id


def test_scoped_envelope_restores_context_on_exit():
    """
    Exiting a nested context restores the enclosing envelope as current.

    Given: An active scoped_envelope context.
    When: A nested scoped_envelope is entered and exited.
    Then: The outer envelope is current again after the inner block exits.
    """
    # Act & Assert
    with scoped_envelope() as outer:
        with scoped_envelope() as inner:
            assert inner is not outer
        assert EventEnvelope.current() is outer


def test_scoped_envelope_clears_context_on_exit():
    """
    Exiting the outermost context leaves no current envelope behind.

    Given: A scoped_envelope context that has exited.
    When: EventEnvelope.current() is called.
    Then: None is returned.
    """
    # Arrange
    with scoped_envelope() as envelope:
        assert EventEnvelope.current() is envelope

    # Act & Assert
    assert EventEnvelope.current() is None


def test_scoped_envelope_explicit_envelope():
    """
    An explicitly provided envelope is used as-is rather than generating a new one.

    Given: A pre-constructed EventEnvelope.
    When: It is passed to scoped_envelope.
    Then: current() returns that exact envelope inside the block.
    """
    # Arrange
    envelope = EventEnvelope()

    # Act & Assert
    with scoped_envelope(envelope) as ctx:
        assert ctx is envelope
        assert EventEnvelope.current() is envelope


def test_scoped_envelope_explicit_with_existing_context():
    """
    An explicit envelope is installed as-is even when a context already exists.

    Given: An active scoped_envelope context.
    When: A second scoped_envelope is entered with an explicit envelope.
    Then: The explicit envelope is current (not a child of the outer one),
          and the outer envelope is restored on exit.
    """
    # Arrange
    explicit = EventEnvelope()

    # Act & Assert
    with scoped_envelope() as outer:
        with scoped_envelope(explicit) as ctx:
            assert ctx is explicit
            assert ctx.causation_id is None
            assert ctx.correlation_id != outer.correlation_id
        assert EventEnvelope.current() is outer


def test_scoped_envelope_restores_context_on_exception():
    """
    The outer envelope is restored even when an exception escapes the inner block.

    Given: An active scoped_envelope context.
    When: A nested scoped_envelope block raises an exception.
    Then: The outer envelope is still current after the exception is caught.
    """
    # Act & Assert
    with scoped_envelope() as outer:
        with pytest.raises(RuntimeError):
            with scoped_envelope():
                raise RuntimeError("boom")
        assert EventEnvelope.current() is outer


async def test_async_context_isolation():
    """
    Concurrent tasks each maintain their own envelope without bleeding into each other.

    Given: Two tasks that each enter their own scoped_envelope.
    When: Both run concurrently and yield between reads.
    Then: Each task observes only its own envelope via current().
    """
    # Arrange
    results: dict[str, EventEnvelope] = {}

    async def run(name: str) -> None:
        with scoped_envelope():
            await asyncio.sleep(0)
            envelope = EventEnvelope.current()
            assert envelope is not None
            results[name] = envelope

    # Act
    await asyncio.gather(asyncio.create_task(run("a")), asyncio.create_task(run("b")))

    # Assert
    assert results["a"] is not results["b"]
    assert results["a"].correlation_id != results["b"].correlation_id


async def test_async_spawned_task_inherits_parent_envelope():
    """
    A task spawned inside a scoped_envelope sees the parent's envelope at creation time.

    Given: An active scoped_envelope context.
    When: A child task is created inside that context.
    Then: The child task's initial current() is the parent's envelope.
    """
    # Arrange
    seen: list[EventEnvelope] = []

    async def child() -> None:
        await asyncio.sleep(0)
        envelope = EventEnvelope.current()
        assert envelope is not None
        seen.append(envelope)

    # Act
    with scoped_envelope() as parent:
        task = asyncio.create_task(child())
        await task

    # Assert
    assert seen[0] is parent
