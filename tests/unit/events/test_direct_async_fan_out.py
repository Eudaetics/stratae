"""
Unit tests for how an AsyncDirectBus runs an event's async handlers.

This test suite verifies the following behaviors:

AsyncDirectBus (default, one at a time):
- Two async handlers sharing a resource that refuses concurrent use succeed by default, and
  fail under fan_out_concurrent.
- No two async handlers of one emit run at the same time.
- Context variables a handler sets reach neither the other handlers nor the emitter.
- Every handler runs, and their failures are raised together in a HandlerErrors group.
- A handler's CancelledError is raised with the other failures in a BaseHandlerErrors group
  that isn't a HandlerErrors.
- Cancelling an emit cancels the running handler, starts no other, and raises
  CancelledError, ungrouped.

AsyncDirectBus (fan_out_concurrent):
- Several async handlers run concurrently, each in its own context.
- Every handler runs, and their failures are raised together, a CancelledError among them in
  a BaseHandlerErrors group.
"""

import asyncio
from contextvars import ContextVar
from unittest.mock import AsyncMock, Mock

import pytest

from stratae.events import AsyncDirectBus, Dispatch, PubSub, fan_out_concurrent
from stratae.events.exceptions import BaseHandlerErrors, HandlerErrors


class _Placed:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


_placed = PubSub(_Placed)

_marker: ContextVar[str] = ContextVar("_marker", default="unset")


class _InUseError(RuntimeError):
    """Raised by a _Connection called while another call is in progress."""


class _Connection:
    """A resource that refuses a call while another is in progress."""

    def __init__(self) -> None:
        self.busy = False
        self.rows: list[str] = []

    async def write(self, row: str) -> None:
        if self.busy:
            raise _InUseError
        self.busy = True
        await asyncio.sleep(0)
        self.busy = False
        self.rows.append(row)


def _writers(bus: AsyncDirectBus, connection: _Connection) -> None:
    """Register two async handlers that each write through `connection`."""

    async def audit(placed: _Placed) -> None:
        await connection.write(f"audit {placed.order_id}")

    async def count(placed: _Placed) -> None:
        await connection.write(f"count {placed.order_id}")

    bus.handle(_placed, audit)
    bus.handle(_placed, count)


@pytest.fixture
def bus() -> AsyncDirectBus:
    """Return an AsyncDirectBus with the default dispatchers."""
    return AsyncDirectBus()


@pytest.fixture
def concurrent_bus() -> AsyncDirectBus:
    """Return an AsyncDirectBus running several async handlers concurrently."""
    return AsyncDirectBus(dispatchers=[Dispatch(PubSub, fan_out_concurrent)])


async def test_shared_resource_succeeds_by_default_and_fails_concurrently(
    bus: AsyncDirectBus, concurrent_bus: AsyncDirectBus
) -> None:
    """
    Handlers sharing a resource should succeed by default, and fail when run concurrently.

    Given: Two async handlers writing through one connection that refuses overlapping calls,
        on a default bus and on a bus with fan_out_concurrent
    When: The event is emitted on each
    Then: The default bus writes both rows, and the concurrent bus raises the connection's
        error
    """
    # Arrange
    sequential, concurrent = _Connection(), _Connection()
    _writers(bus, sequential)
    _writers(concurrent_bus, concurrent)

    # Act
    await bus.emit(_placed, _Placed(1))
    with pytest.raises(HandlerErrors) as exc_info:
        await concurrent_bus.emit(_placed, _Placed(1))

    # Assert
    assert sorted(sequential.rows) == ["audit 1", "count 1"]
    assert [type(exc) for exc in exc_info.value.exceptions] == [_InUseError]


async def test_async_handlers_never_overlap(bus: AsyncDirectBus) -> None:
    """
    No two async handlers of one emit should run at the same time.

    Given: Three async handlers that each count themselves in, suspend, and count out
    When: The event is emitted
    Then: All three ran, and at most one was running at any moment
    """
    # Arrange
    running: list[int] = []
    most = 0

    async def handler(placed: _Placed) -> None:
        nonlocal most
        running.append(placed.order_id)
        most = max(most, len(running))
        await asyncio.sleep(0)
        running.pop()

    handlers = [AsyncMock(side_effect=handler) for _ in range(3)]
    for registered in handlers:
        bus.handle(_placed, registered)

    # Act
    await bus.emit(_placed, _Placed(2))

    # Assert
    assert all(registered.await_count == 1 for registered in handlers)
    assert most == 1


async def test_handler_context_changes_stay_with_handler(bus: AsyncDirectBus) -> None:
    """
    Context variables a handler sets should reach neither the other handlers nor the emitter.

    Given: A sync handler and two async handlers, each reading a context variable and then
        setting it, the async ones suspending before reading it again
    When: The event is emitted
    Then: Every handler first read the unset value, each async handler read its own value
        after suspending, and the emitter still reads the unset value
    """
    # Arrange
    before: list[str] = []
    after: list[str] = []

    def sync_handler(placed: _Placed) -> None:
        before.append(_marker.get())
        _marker.set("sync")

    async def async_handler(placed: _Placed) -> None:
        before.append(_marker.get())
        _marker.set(f"async {len(before)}")
        await asyncio.sleep(0)
        after.append(_marker.get())

    bus.handle(_placed, sync_handler)
    bus.handle(_placed, async_handler)
    bus.handle(_placed, async_handler)

    # Act
    await bus.emit(_placed, _Placed(3))

    # Assert
    assert before == ["unset", "unset", "unset"]
    assert after == ["async 2", "async 3"]
    assert _marker.get() == "unset"


async def test_failures_raised_together_after_every_handler(bus: AsyncDirectBus) -> None:
    """
    Every handler should run, and their failures should be raised together.

    Given: A sync handler raising ValueError, an async handler raising KeyError, and an
        async handler that succeeds
    When: The event is emitted
    Then: Every handler was called, and a HandlerErrors holding both errors is raised
    """
    # Arrange
    value_error, key_error = ValueError("sync"), KeyError("async")
    succeeding = AsyncMock()
    bus.handle(_placed, Mock(side_effect=value_error))
    bus.handle(_placed, AsyncMock(side_effect=key_error))
    bus.handle(_placed, succeeding)

    # Act
    with pytest.raises(HandlerErrors) as exc_info:
        await bus.emit(_placed, _Placed(4))

    # Assert
    succeeding.assert_awaited_once()
    assert set(exc_info.value.exceptions) == {value_error, key_error}


async def test_cancelled_error_is_collected_with_the_other_failures(
    bus: AsyncDirectBus,
) -> None:
    """
    A handler's CancelledError should be raised with the other failures, not swallowed.

    Given: Two async handlers, one raising CancelledError and one ValueError
    When: The event is emitted
    Then: A BaseHandlerErrors holding both is raised, and it isn't a HandlerErrors
    """
    # Arrange
    bus.handle(_placed, AsyncMock(side_effect=asyncio.CancelledError()))
    bus.handle(_placed, AsyncMock(side_effect=ValueError("failed")))

    # Act
    with pytest.raises(BaseHandlerErrors) as exc_info:
        await bus.emit(_placed, _Placed(5))

    # Assert
    assert not isinstance(exc_info.value, HandlerErrors)
    assert {type(exc) for exc in exc_info.value.exceptions} == {
        asyncio.CancelledError,
        ValueError,
    }


async def test_cancelling_emit_stops_at_the_running_handler(bus: AsyncDirectBus) -> None:
    """
    Cancelling an emit should cancel the running handler and start no other.

    Given: Two async handlers that each wait forever, recording when they start and when
        they're cancelled
    When: The task emitting the event is cancelled while the first handler waits
    Then: Only one handler started, it saw the cancellation, and the emit raises a plain
        CancelledError
    """
    # Arrange
    started = asyncio.Event()
    seen: list[str] = []

    async def handler(placed: _Placed) -> None:
        seen.append("started")
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            seen.append("cancelled")
            raise

    bus.handle(_placed, handler)
    bus.handle(_placed, handler)
    emitting = asyncio.create_task(bus.emit(_placed, _Placed(6)))
    await started.wait()

    # Act
    emitting.cancel()

    # Assert
    with pytest.raises(asyncio.CancelledError):
        await emitting
    assert seen == ["started", "cancelled"]


async def test_concurrent_handlers_run_together_in_own_contexts(
    concurrent_bus: AsyncDirectBus,
) -> None:
    """
    Under fan_out_concurrent, async handlers should run together, each in its own context.

    Given: Two async handlers that each set a context variable, then wait for the other at a
        barrier
    When: The event is emitted
    Then: Both pass the barrier, each first read the unset value, and the emitter still
        reads the unset value
    """
    # Arrange
    both = asyncio.Barrier(2)
    before: list[str] = []

    async def handler(placed: _Placed) -> None:
        before.append(_marker.get())
        _marker.set("handler")
        await asyncio.wait_for(both.wait(), 1)

    concurrent_bus.handle(_placed, handler)
    concurrent_bus.handle(_placed, handler)

    # Act
    await concurrent_bus.emit(_placed, _Placed(8))

    # Assert
    assert before == ["unset", "unset"]
    assert _marker.get() == "unset"


async def test_concurrent_failures_raised_together(concurrent_bus: AsyncDirectBus) -> None:
    """
    Under fan_out_concurrent, every handler should run and failures be raised together.

    Given: A sync handler raising ValueError, an async handler raising CancelledError, and an
        async handler that succeeds
    When: The event is emitted
    Then: Every handler was called, and a BaseHandlerErrors that isn't a HandlerErrors holds
        both failures
    """
    # Arrange
    succeeding = AsyncMock()
    concurrent_bus.handle(_placed, Mock(side_effect=ValueError("sync")))
    concurrent_bus.handle(_placed, AsyncMock(side_effect=asyncio.CancelledError()))
    concurrent_bus.handle(_placed, succeeding)

    # Act
    with pytest.raises(BaseHandlerErrors) as exc_info:
        await concurrent_bus.emit(_placed, _Placed(9))

    # Assert
    succeeding.assert_awaited_once()
    assert not isinstance(exc_info.value, HandlerErrors)
    assert {type(exc) for exc in exc_info.value.exceptions} == {
        ValueError,
        asyncio.CancelledError,
    }
