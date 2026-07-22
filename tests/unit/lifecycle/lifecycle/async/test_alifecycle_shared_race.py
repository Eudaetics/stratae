"""Tests for shared-scope cache races - concurrent tasks computing the same cached slot."""

import asyncio

from stratae.lifecycle import AsyncLifecycle, async_resource


async def test_shared_scope_slot_eligible_computed_once_under_task_contention(
    async_lifecycle: AsyncLifecycle,
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

    @async_lifecycle.cache("application")
    async def get_thing() -> object:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return object()

    # Act
    async with async_lifecycle.start("application"):
        results = await asyncio.gather(*(get_thing() for _ in range(task_count)))

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


async def test_shared_scope_keyed_computed_once_per_key_under_task_contention(
    async_lifecycle: AsyncLifecycle,
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

    @async_lifecycle.cache("application")
    async def get_thing(x: int) -> object:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return object()

    # Act
    async with async_lifecycle.start("application"):
        results = await asyncio.gather(*(get_thing(1) for _ in range(task_count)))

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


async def test_shared_scope_resource_entered_once_under_task_contention(
    async_lifecycle: AsyncLifecycle,
):
    """
    Verify an async-resource-tagged cached function in a shared scope is entered exactly once.

    Given: several tasks all calling a shared-scope cached async resource for the first time,
    When: they race to enter and cache it,
    Then: the resource should be entered exactly once, and cleaned up exactly once when the
          scope exits.
    """
    # Arrange
    enter_count = 0
    cleanup_count = 0
    task_count = 20

    @async_lifecycle.cache("application")
    @async_resource
    async def get_resource():
        nonlocal enter_count, cleanup_count
        enter_count += 1
        await asyncio.sleep(0.01)
        yield object()
        cleanup_count += 1

    async with async_lifecycle.start("application"):
        results = await asyncio.gather(*(get_resource() for _ in range(task_count)))

        # Assert - not cleaned up until the scope exits.
        assert cleanup_count == 0

    # Assert
    assert enter_count == 1
    assert cleanup_count == 1
    assert len({id(value) for value in results}) == 1


async def test_shared_scope_reentrant_dependency_does_not_deadlock(
    async_lifecycle: AsyncLifecycle,
):
    """
    Cached function awaiting another cached function in the same shared scope doesn't deadlock.

    Given: a cached function whose first computation awaits a second cached function in the
           same shared scope,
    When: the outer function is called for the first time,
    Then: both should compute without deadlocking.
    """

    # Arrange
    @async_lifecycle.cache("application")
    async def inner() -> int:
        return 1

    @async_lifecycle.cache("application")
    async def outer() -> int:
        return await inner() + 1

    # Act - a timeout turns a regression into a fast failure instead of hanging the suite.
    async with async_lifecycle.start("application"):
        result = await asyncio.wait_for(outer(), timeout=2)

    # Assert
    assert result == 2
