"""Tests for shared-scope cache races - concurrent tasks computing the same cached slot."""

import asyncio

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
    async with async_scope.activate():
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
    async with async_scope.activate():
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

    async with async_scope.activate():
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
    async with async_scope.activate():
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
    async with async_scope.activate():
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
    async with async_scope.activate():
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
    async with async_scope.activate():
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
    async with async_scope.activate():
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
