"""Unit test suite for Envelope."""

import asyncio
from datetime import timezone
from uuid import uuid4

import pytest

from stratae.events import (
    CAUSATION_ID_HEADER,
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    Envelope,
)


def test_envelope_default_fields():
    """
    Default fields are populated with unique identifiers and a UTC timestamp.

    Given: No arguments.
    When: An Envelope is created.
    Then: message_id and correlation_id are set, causation_id is None, timestamp is UTC.
    """
    # Act
    envelope = Envelope()

    # Assert
    assert envelope.message_id is not None
    assert envelope.correlation_id is not None
    assert envelope.causation_id is None
    assert envelope.timestamp.tzinfo == timezone.utc


def test_envelope_unique_message_ids():
    """
    Each envelope receives a distinct message_id.

    Given: No arguments.
    When: Two Envelopes are created.
    Then: Their message_ids differ.
    """
    # Act
    a = Envelope()
    b = Envelope()

    # Assert
    assert a.message_id != b.message_id


def test_envelope_unique_correlation_ids():
    """
    Independent envelopes each start their own correlation chain.

    Given: No arguments.
    When: Two Envelopes are created independently.
    Then: Their correlation_ids differ.
    """
    # Act
    a = Envelope()
    b = Envelope()

    # Assert
    assert a.correlation_id != b.correlation_id


def test_child_inherits_correlation_id():
    """
    A child envelope stays in the same correlation chain as its parent.

    Given: An Envelope.
    When: A child envelope is created from it.
    Then: The child's correlation_id matches the parent's.
    """
    # Arrange
    parent = Envelope()

    # Act
    child = parent.child()

    # Assert
    assert child.correlation_id == parent.correlation_id


def test_child_causation_id_is_parent_message_id():
    """
    A child envelope records its parent as the cause.

    Given: An Envelope.
    When: A child envelope is created from it.
    Then: The child's causation_id equals the parent's message_id.
    """
    # Arrange
    parent = Envelope()

    # Act
    child = parent.child()

    # Assert
    assert child.causation_id == parent.message_id


def test_child_has_distinct_message_id():
    """
    A child envelope is its own message, not a copy of its parent.

    Given: An Envelope.
    When: A child envelope is created from it.
    Then: The child's message_id differs from the parent's.
    """
    # Arrange
    parent = Envelope()

    # Act
    child = parent.child()

    # Assert
    assert child.message_id != parent.message_id


def test_headers_round_trip():
    """
    from_headers reconstructs the envelope to_headers serialized.

    Given: An envelope with a causation id.
    When: It is serialized to headers and rebuilt.
    Then: All fields survive the round trip.
    """
    # Arrange
    envelope = Envelope(causation_id=uuid4())

    # Act
    rebuilt = Envelope.from_headers(envelope.to_headers())

    # Assert
    assert rebuilt is not None
    assert rebuilt.message_id == envelope.message_id
    assert rebuilt.correlation_id == envelope.correlation_id
    assert rebuilt.causation_id == envelope.causation_id
    assert rebuilt.timestamp == envelope.timestamp


def test_to_headers_omits_absent_causation():
    """
    A root envelope serializes without a causation header.

    Given: An envelope with no causation id.
    When: It is serialized to headers.
    Then: The causation header is absent.
    """
    # Act
    headers = Envelope().to_headers()

    # Assert
    assert CAUSATION_ID_HEADER not in headers


def test_from_headers_defaults_absent_fields():
    """
    Fields absent from the headers are minted rather than rejected.

    Given: Headers carrying only a message id.
    When: from_headers is called.
    Then: The message id is preserved and the rest are defaulted.
    """
    # Arrange
    message_id = uuid4()

    # Act
    rebuilt = Envelope.from_headers({MESSAGE_ID_HEADER: str(message_id)})

    # Assert
    assert rebuilt.message_id == message_id
    assert rebuilt.correlation_id is not None
    assert rebuilt.causation_id is None
    assert rebuilt.timestamp.tzinfo == timezone.utc


def test_from_headers_treats_none_values_as_absent():
    """
    Ids present as None values read as absent, not as corruption.

    Given: Headers whose identifying keys exist but hold None.
    When: from_headers is called.
    Then: A fresh envelope is minted without raising.
    """
    # Arrange
    headers = {MESSAGE_ID_HEADER: None, CORRELATION_ID_HEADER: None}

    # Act
    envelope = Envelope.from_headers(headers)

    # Assert
    assert envelope.message_id is not None
    assert envelope.correlation_id is not None
    assert envelope.causation_id is None


def test_from_headers_with_invalid_ids_raises():
    """
    Unparseable ids raise rather than silently dropping the trace.

    Given: Headers whose message id is not a UUID.
    When: from_headers is called.
    Then: ValueError is raised.
    """
    # Arrange
    headers = {MESSAGE_ID_HEADER: "not-a-uuid", CORRELATION_ID_HEADER: str(uuid4())}

    # Act & Assert
    with pytest.raises(ValueError, match="badly formed"):
        Envelope.from_headers(headers)


def test_current_returns_none_outside_context():
    """
    current() returns None when no envelope is active.

    Given: No active scope context.
    When: Envelope.current() is called.
    Then: None is returned.
    """
    # Act & Assert
    assert Envelope.current() is None


def test_scope_sets_current():
    """
    The active envelope is accessible via current() inside the context block.

    Given: An Envelope.scope() context.
    When: Envelope.current() is called inside it.
    Then: The returned envelope matches the one yielded by the context manager.
    """
    # Act & Assert
    with Envelope.scope() as envelope:
        assert Envelope.current() is envelope


def test_scope_root_has_no_causation():
    """
    A root context has no prior cause.

    Given: No existing context.
    When: Envelope.scope() is entered without arguments.
    Then: The yielded envelope has causation_id of None.
    """
    # Act & Assert
    with Envelope.scope() as envelope:
        assert envelope.causation_id is None


def test_scope_nested_creates_child():
    """
    Nesting a scope automatically creates a child of the current context.

    Given: An active Envelope.scope() context.
    When: A second scope is entered without arguments.
    Then: The inner envelope's causation_id equals the outer envelope's message_id.
    """
    # Act & Assert
    with Envelope.scope() as outer:
        with Envelope.scope() as inner:
            assert inner.correlation_id == outer.correlation_id
            assert inner.causation_id == outer.message_id


def test_scope_restores_context_on_exit():
    """
    Exiting a nested context restores the enclosing envelope as current.

    Given: An active Envelope.scope() context.
    When: A nested scope is entered and exited.
    Then: The outer envelope is current again after the inner block exits.
    """
    # Act & Assert
    with Envelope.scope() as outer:
        with Envelope.scope() as inner:
            assert inner is not outer
        assert Envelope.current() is outer


def test_scope_clears_context_on_exit():
    """
    Exiting the outermost context leaves no current envelope behind.

    Given: An Envelope.scope() context that has exited.
    When: Envelope.current() is called.
    Then: None is returned.
    """
    # Arrange
    with Envelope.scope() as envelope:
        assert Envelope.current() is envelope

    # Act & Assert
    assert Envelope.current() is None


def test_scope_explicit_envelope():
    """
    An explicitly provided envelope is used as-is rather than generating a new one.

    Given: A pre-constructed Envelope.
    When: It is passed to Envelope.scope().
    Then: current() returns that exact envelope inside the block.
    """
    # Arrange
    envelope = Envelope()

    # Act & Assert
    with Envelope.scope(envelope) as ctx:
        assert ctx is envelope
        assert Envelope.current() is envelope


def test_scope_explicit_with_existing_context():
    """
    An explicit envelope is installed as-is even when a context already exists.

    Given: An active Envelope.scope() context.
    When: A second scope is entered with an explicit envelope.
    Then: The explicit envelope is current (not a child of the outer one),
          and the outer envelope is restored on exit.
    """
    # Arrange
    explicit = Envelope()

    # Act & Assert
    with Envelope.scope() as outer:
        with Envelope.scope(explicit) as ctx:
            assert ctx is explicit
            assert ctx.causation_id is None
            assert ctx.correlation_id != outer.correlation_id
        assert Envelope.current() is outer


def test_scope_restores_context_on_exception():
    """
    The outer envelope is restored even when an exception escapes the inner block.

    Given: An active Envelope.scope() context.
    When: A nested scope block raises an exception.
    Then: The outer envelope is still current after the exception is caught.
    """
    # Act & Assert
    with Envelope.scope() as outer:
        with pytest.raises(RuntimeError):
            with Envelope.scope():
                raise RuntimeError("boom")
        assert Envelope.current() is outer


async def test_async_context_isolation():
    """
    Concurrent tasks each maintain their own envelope without bleeding into each other.

    Given: Two tasks that each enter their own Envelope.scope().
    When: Both run concurrently and yield between reads.
    Then: Each task observes only its own envelope via current().
    """
    # Arrange
    results: dict[str, Envelope] = {}

    async def run(name: str) -> None:
        with Envelope.scope():
            await asyncio.sleep(0)
            envelope = Envelope.current()
            assert envelope is not None
            results[name] = envelope

    # Act
    await asyncio.gather(asyncio.create_task(run("a")), asyncio.create_task(run("b")))

    # Assert
    assert results["a"] is not results["b"]
    assert results["a"].correlation_id != results["b"].correlation_id


async def test_async_spawned_task_inherits_parent_envelope():
    """
    A task spawned inside a scope sees the parent's envelope at creation time.

    Given: An active Envelope.scope() context.
    When: A child task is created inside that context.
    Then: The child task's initial current() is the parent's envelope.
    """
    # Arrange
    seen: list[Envelope] = []

    async def child() -> None:
        await asyncio.sleep(0)
        envelope = Envelope.current()
        assert envelope is not None
        seen.append(envelope)

    # Act
    with Envelope.scope() as parent:
        task = asyncio.create_task(child())
        await task

    # Assert
    assert seen[0] is parent
