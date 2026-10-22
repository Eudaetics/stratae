"""Tests for shared-scope cache races - concurrent tasks computing the same cached slot."""

import asyncio
import threading
from typing import Any

import pytest

from stratae.lifecycle.exceptions import CacheCycleError
from stratae.lifecycle.resource import async_resource
from stratae.lifecycle.scope import AsyncScope


async def test_shared_scope_slot_eligible_computed_once_under_task_contention(
    async_scope: AsyncScope,
):
    """
    Verify a no-arg cached function in a shared scope is computed exactly once.

    Given: several tasks all calling a shared-scope cached function for the first time,
    When: they race to fill the same slot,
    Then: the underlying function should run exactly once, and every task should see the
          same cached value.
    """
    # Arrange
    call_count = 0
    task_count = 20

    @async_scope.cache()
    async def get_thing() -> object:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return object()

    # Act
    async with async_scope:
        results = await asyncio.gather(*(get_thing() for _ in range(task_count)))

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


async def test_shared_scope_keyed_computed_once_per_key_under_task_contention(
    async_scope: AsyncScope,
):
    """
    Verify a keyed cached function in a shared scope computes each key exactly once.

    Given: several tasks all calling a shared-scope cached function with the same argument,
    When: they race to fill the same cache entry,
    Then: the underlying function should run exactly once for that key.
    """
    # Arrange
    call_count = 0
    task_count = 20

    @async_scope.cache()
    async def get_thing(x: int) -> object:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return object()

    # Act
    async with async_scope:
        results = await asyncio.gather(*(get_thing(1) for _ in range(task_count)))

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


async def test_shared_scope_resource_entered_once_under_task_contention(
    async_scope: AsyncScope,
):
    """
    Verify an async-resource-tagged cached function in a shared scope is entered exactly once.

    Given: several tasks all calling a shared-scope cached async resource for the first time,
    When: they race to enter and cache it,
    Then: the resource should be entered exactly once, and cleaned up exactly once when the
          scope deactivates.
    """
    # Arrange
    enter_count = 0
    cleanup_count = 0
    task_count = 20

    @async_scope.cache()
    @async_resource
    async def get_resource():
        nonlocal enter_count, cleanup_count
        enter_count += 1
        await asyncio.sleep(0.01)
        yield object()
        cleanup_count += 1

    async with async_scope:
        results = await asyncio.gather(*(get_resource() for _ in range(task_count)))

        # Assert - not cleaned up until the scope deactivates.
        assert cleanup_count == 0

    # Assert
    assert enter_count == 1
    assert cleanup_count == 1
    assert len({id(value) for value in results}) == 1


async def test_shared_scope_reentrant_dependency_does_not_deadlock(
    async_scope: AsyncScope,
):
    """
    Cached function awaiting another cached function in the same shared scope doesn't deadlock.

    Given: a cached function whose first computation awaits a second cached function in the
           same shared scope,
    When: the outer function is called for the first time,
    Then: both should compute without deadlocking.
    """

    # Arrange
    @async_scope.cache()
    async def inner() -> int:
        return 1

    @async_scope.cache()
    async def outer() -> int:
        return await inner() + 1

    # Act - a timeout turns a regression into a fast failure instead of hanging the suite.
    async with async_scope:
        result = await asyncio.wait_for(outer(), timeout=2)

    # Assert
    assert result == 2


async def test_shared_scope_keyed_recursion_does_not_deadlock(async_scope: AsyncScope):
    """
    A keyed cached coroutine function awaiting itself with other keys doesn't deadlock.

    Given: a shared-scope cached fib(n) whose first computation awaits fib(n - 1) and fib(n - 2),
    When: it's awaited for the first time,
    Then: every key should compute without deadlocking, each exactly once.
    """
    # Arrange
    computed: list[int] = []

    @async_scope.cache()
    async def fib(n: int) -> int:
        computed.append(n)
        return n if n < 2 else await fib(n - 1) + await fib(n - 2)

    # Act - a timeout turns a regression into a fast failure instead of hanging the suite.
    async with async_scope:
        result = await asyncio.wait_for(fib(10), timeout=2)

    # Assert
    assert result == 55
    assert sorted(computed) == list(range(11))


async def test_shared_scope_mutual_recursion_does_not_deadlock(async_scope: AsyncScope):
    """
    Two keyed cached coroutine functions awaiting each other with different keys don't deadlock.

    Given: shared-scope cached functions a(n) -> b(n - 1) -> a(n - 1),
    When: a is awaited for the first time,
    Then: the chain should resolve without deadlocking.
    """

    # Arrange
    @async_scope.cache()
    async def a(n: int) -> int:
        return 0 if n == 0 else await b(n - 1) + 1

    @async_scope.cache()
    async def b(n: int) -> int:
        return await a(n)

    # Act
    async with async_scope:
        result = await asyncio.wait_for(a(3), timeout=2)

    # Assert
    assert result == 3


async def test_shared_scope_self_dependent_slot_raises_cycle_error(async_scope: AsyncScope):
    """
    A slot-eligible cached coroutine function awaiting itself raises instead of deadlocking.

    Given: a shared-scope ignore_params function whose computation awaits itself,
    When: it's awaited for the first time,
    Then: the inner call should raise CacheCycleError, and the slot should stay unclaimed.
    """
    # Arrange
    recurse = True

    @async_scope.cache(ignore_params=True)
    async def countdown(n: int) -> int:
        return await countdown(n - 1) if recurse else n

    # Act
    async with async_scope:
        with pytest.raises(CacheCycleError):
            await asyncio.wait_for(countdown(3), timeout=2)
        recurse = False
        result = await countdown(5)

    # Assert
    assert result == 5


async def test_shared_scope_waiter_recomputes_after_owner_fails(async_scope: AsyncScope):
    """
    A task waiting on another task's failed computation computes the value itself.

    Given: one task computing a shared-scope cached function, and a second task waiting on
           that same entry,
    When: the first task's computation raises,
    Then: the first should see its exception, and the waiter should compute and cache the
          value itself.
    """
    # Arrange
    owner_started = asyncio.Event()
    release_owner = asyncio.Event()
    calls = 0

    @async_scope.cache()
    async def get_thing() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            owner_started.set()
            await release_owner.wait()
            raise RuntimeError("first computation fails")
        return 42

    # Act
    async with async_scope:
        owner = asyncio.ensure_future(get_thing())
        await owner_started.wait()
        waiter = asyncio.ensure_future(get_thing())
        await asyncio.sleep(0)  # let the waiter reach the claimed entry before the owner fails
        release_owner.set()
        owner_outcome, waiter_outcome = await asyncio.wait_for(
            asyncio.gather(owner, waiter, return_exceptions=True), timeout=2
        )
        cached = await get_thing()

    # Assert
    assert isinstance(owner_outcome, RuntimeError)
    assert waiter_outcome == 42
    assert cached == 42
    assert calls == 2


@pytest.mark.parametrize("debug", [False, True], ids=["normal", "asyncio_debug"])
def test_shared_scope_waiter_on_another_threads_loop_is_woken(debug: bool):
    """
    A waiter on another thread's event loop is woken as soon as the owner releases.

    Given: A shared AsyncScope used from two threads, each running its own event loop,
        with thread B's call waiting on thread A's in-progress computation
    When: Thread A's computation finishes
    Then: B gets the value at once, without its loop being woken from outside, and in
        asyncio debug mode neither call fails on a cross-thread loop operation
    """
    # Arrange
    scope = AsyncScope("application", "shared")
    claimed = threading.Event()

    @scope.cache()
    async def get_value() -> str:
        claimed.set()
        await asyncio.sleep(0.05)
        return "value"

    results: dict[str, str] = {}

    def run(name: str) -> None:
        async def main() -> None:
            if name == "waiter":
                claimed.wait()
            results[name] = await get_value()

        asyncio.run(main(), debug=debug)

    activation = scope.activate()
    threads = [
        threading.Thread(target=run, args=(name,), daemon=True) for name in ("owner", "waiter")
    ]

    # Act
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=2)

    # Assert
    assert not any(thread.is_alive() for thread in threads)
    assert results == {"owner": "value", "waiter": "value"}
    asyncio.run(scope.deactivate(activation))


async def test_shared_scope_cancelled_waiter_is_skipped_on_release(async_scope: AsyncScope):
    """
    A waiter cancelled before the computation finishes doesn't break its release.

    Given: A shared scope whose cached computation another task is waiting on
    When: The waiting task is cancelled, then the computation finishes
    Then: The owner gets its value without error, and the value is cached
    """
    # Arrange
    proceed = asyncio.Event()
    calls: list[int] = []

    @async_scope.cache()
    async def get_value() -> int:
        calls.append(1)
        await proceed.wait()
        return 5

    async with async_scope:
        owner = asyncio.ensure_future(get_value())
        await asyncio.sleep(0)
        waiter = asyncio.ensure_future(get_value())
        await asyncio.sleep(0)

        # Act
        waiter.cancel()
        await asyncio.sleep(0)
        proceed.set()
        result = await owner

        # Assert
        assert waiter.cancelled()
        assert result == 5
        assert await get_value() == 5
        assert calls == [1]


async def test_shared_scope_waiter_cancelled_after_release_is_not_woken(async_scope: AsyncScope):
    """
    A waiter cancelled after its wake-up is scheduled, but before it runs, isn't woken.

    Given: A shared scope's computation with a task waiting on it, whose cancellation is
        queued ahead of the wake-up its release will schedule
    When: The computation finishes and the loop runs both callbacks
    Then: The waiter ends cancelled, and waking it reports no error to the loop
    """
    # Arrange
    loop = asyncio.get_running_loop()
    reported: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _, context: reported.append(context))
    proceed = asyncio.Event()

    @async_scope.cache()
    async def get_value() -> int:
        await proceed.wait()
        # Queued now, so it runs after this computation's release, but before the wake-up
        # release schedules - asyncio runs callbacks in the order they were queued.
        loop.call_soon(waiter.cancel)
        return 5

    try:
        async with async_scope:
            owner = asyncio.ensure_future(get_value())
            await asyncio.sleep(0)
            waiter = asyncio.ensure_future(get_value())
            await asyncio.sleep(0)

            # Act
            proceed.set()
            result = await owner
            await asyncio.sleep(0)
            await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(None)

    # Assert
    assert result == 5
    assert waiter.cancelled()
    assert reported == []
