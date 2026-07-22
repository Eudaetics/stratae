"""
Test suite for verifying concurrent context isolation and resource management in lifecycle.

This module contains asynchronous tests to ensure that:
- Concurrent execution contexts maintain isolated REQUEST scope caches.
- REQUEST scoped cached functions behave correctly under concurrent access.
- Multiple REQUEST scoped calls within each context do not share cache across requests.
- Scoped resources are managed independently and cleaned up correctly in concurrent contexts.
"""

import asyncio
from typing import Any

from stratae.lifecycle import AsyncLifecycle, async_resource


async def test_concurrent_cache_isolation(async_lifecycle: AsyncLifecycle):
    """
    Verify concurrent CONTEXTS have isolated REQUEST scope caches.

    Given: Two separate execution contexts
    When: Each context uses a REQUEST scoped cached function
    Then: Each context should have its own isolated cache
    """
    # Arrange
    call_count = 0
    iterations = 5
    lock = asyncio.Lock()

    @async_lifecycle.cache("request")
    async def get_user_id():
        """Simulate fetching a user ID with some delay."""
        async with lock:
            nonlocal call_count
            call_count += 1
            result = call_count
        await asyncio.sleep(0.001)
        return result

    results_a: list[int] = []
    results_b: list[int] = []

    async def task_a():
        async with async_lifecycle.start("request"):
            for _ in range(iterations):
                result = await get_user_id()
                results_a.append(result)

    async def task_b():
        async with async_lifecycle.start("request"):
            for _ in range(iterations):
                result = await get_user_id()
                results_b.append(result)

    task_a_handle = asyncio.create_task(task_a())
    task_b_handle = asyncio.create_task(task_b())

    # Act
    await asyncio.gather(task_a_handle, task_b_handle)

    # Assert
    # Each task should get the same value every time (cached within request)
    assert len(set(results_a)) == 1, f"Task A got different values: {results_a}"
    assert len(set(results_b)) == 1, f"Task B got different values: {results_b}"

    # But the two tasks should get different values (isolated)
    assert results_a[0] != results_b[0], "Tasks got the same cached value!"

    # Total calls should be 2 (once per task, cached within task)
    assert call_count == 2, f"Expected 2 calls, got {call_count}"


async def test_concurrent_cache_isolation_multiple_requests(async_lifecycle: AsyncLifecycle):
    """
    Verify concurrent CONTEXTS have isolated REQUEST scope caches.

    This test extends the previous one to simulate multiple REQUEST scoped calls within each
    context to ensure that caching works correctly across multiple calls. The goal is to confirm
    that each context maintains its own isolated cache even when making several requests,
    similar to real-world API usage patterns.

    Given: Two separate execution contexts
    When: Each context uses multiple request lifecycle REQUEST scoped cached function
    Then: Each context should have its own isolated cache
    """
    # Arrange
    call_count = 0
    iterations = 5
    lock = asyncio.Lock()

    @async_lifecycle.cache("request")
    async def get_user_id():
        """Simulate fetching a user ID with some delay."""
        async with lock:
            nonlocal call_count
            call_count += 1
            result = call_count
        await asyncio.sleep(0.001)
        return result

    results_a: list[int] = []
    results_b: list[int] = []

    async def task_a():
        for _ in range(iterations):
            async with async_lifecycle.start("request"):
                result = await get_user_id()
                results_a.append(result)

    async def task_b():
        for _ in range(iterations):
            async with async_lifecycle.start("request"):
                result = await get_user_id()
                results_b.append(result)

    task_a_handle = asyncio.create_task(task_a())
    task_b_handle = asyncio.create_task(task_b())

    # Act
    await asyncio.gather(task_a_handle, task_b_handle)

    # Assert
    # Each task should get unique values across the calls (not cached across requests)
    assert len(results_a) == len(set(results_a)), f"Task A got a duplicate value: {results_a}"
    assert len(results_b) == len(set(results_b)), f"Task B got a duplicate value: {results_b}"

    # The two tasks should get completely different values (isolated)
    assert len(set(results_b) | set(results_a)) == len(results_a) + len(results_b)

    # Total calls should be once per request, no caching across requests
    assert call_count == iterations * 2, f"Expected {iterations * 2} calls, got {call_count}"


async def test_concurrent_resource_management(async_lifecycle: AsyncLifecycle):
    """
    Verify concurrent contexts manage scoped resources independently.

    Given: Two separate execution contexts
    When: Each context uses a REQUEST scoped resource
    Then: Each context should manage its own resource lifecycle independently
    """
    # Arrange
    init_count = 0
    cleanup: list[asyncio.Task[Any]] = []
    lock = asyncio.Lock()

    @async_lifecycle.cache("request")
    @async_resource
    async def resource_generator():
        """Simulate a resource with setup and teardown."""
        nonlocal init_count, cleanup
        async with lock:
            init_count += 1
        try:
            yield init_count
        finally:
            async with lock:
                task = asyncio.current_task()
                assert task is not None, "current_task() returned None during cleanup"
                cleanup.append(task)

    results_a: list[int] = []
    results_b: list[int] = []

    async def task_a():
        async with async_lifecycle.start("request"):
            results_a.append(await resource_generator())

    async def task_b():
        async with async_lifecycle.start("request"):
            results_b.append(await resource_generator())

    task_a_handle = asyncio.create_task(task_a())
    task_b_handle = asyncio.create_task(task_b())

    # Act
    await asyncio.gather(task_a_handle, task_b_handle)

    # Assert
    # Each task should have initialized its own resource
    assert len(results_a) == 1, f"Task A should have one resource, got: {results_a}"
    assert len(results_b) == 1, f"Task B should have one resource, got: {results_b}"
    assert results_a[0] != results_b[0], "Tasks got the same resource!"

    # Total initializations and cleanups should be 2 (once per task)
    assert init_count == 2, f"Expected 2 initializations, got {init_count}"
    assert len(cleanup) == 2, f"Expected 2 cleanups, got {len(cleanup)}"
    assert len(set(cleanup)) == 2, "Cleanup was not called for both tasks!"


async def test_concurrent_resource_management_nested_scopes(async_lifecycle: AsyncLifecycle):
    """
    Verify concurrent contexts manage nested scoped resources independently.

    This test ensures that when multiple concurrent tasks access resources within
    nested lifecycle scopes (application → request), the application-scoped resources
    are properly shared while request-scoped resources remain isolated per task.

    Given: Two separate execution contexts within nested application/request scopes
    When: Each context uses both APPLICATION and REQUEST scoped resources
    Then: APPLICATION resources should be shared, REQUEST resources should be isolated,
          and all resources should be cleaned up properly for each task
    """
    # Arrange
    init_count = 0
    cleanup: list[asyncio.Task[Any]] = []
    lock = asyncio.Lock()

    app_count = 0
    app_cleanup: list[asyncio.Task[Any]] = []

    @async_lifecycle.cache("application")
    @async_resource
    async def app_resource_generator():
        """Simulate a resource with setup and teardown."""
        nonlocal app_count, app_cleanup
        async with lock:
            app_count += 1
        try:
            yield app_count
        finally:
            async with lock:
                task = asyncio.current_task()
                assert task is not None, "current_task() returned None during cleanup"
                app_cleanup.append(task)

    @async_lifecycle.cache("session")
    @async_resource
    async def resource_generator():
        """Simulate a resource with setup and teardown."""
        nonlocal init_count, cleanup

        assert await app_resource_generator() == 1, (
            "Application resource should be shared across requests"
        )

        async with lock:
            init_count += 1
        try:
            yield init_count
        finally:
            async with lock:
                task = asyncio.current_task()
                assert task is not None, "current_task() returned None during cleanup"
                cleanup.append(task)

    results_a: list[int] = []
    results_b: list[int] = []

    async def task_a():
        async with async_lifecycle.start("session"):
            first = await resource_generator()
            async with async_lifecycle.start("request"):
                second = await resource_generator()
                results_a.append(second)
                assert first == second, "Session resource should be the same within the same task"

    async def task_b():
        async with async_lifecycle.start("session"):
            first = await resource_generator()
            async with async_lifecycle.start("request"):
                second = await resource_generator()
                results_b.append(second)
                assert first == second, "Session resource should be the same within the same task"

    async with async_lifecycle.start("application"):
        task_a_handle = asyncio.create_task(task_a())
        task_b_handle = asyncio.create_task(task_b())

        # Act
        await asyncio.gather(task_a_handle, task_b_handle)

    # Assert
    # Each task should have initialized its own resource
    assert len(results_a) == 1, f"Task A should have one resource, got: {results_a}"
    assert len(results_b) == 1, f"Task B should have one resource, got: {results_b}"
    assert results_a[0] != results_b[0], "Tasks got the same resource!"

    # Total initializations and cleanups should be 2 (once per task)
    assert init_count == 2, f"Expected 2 initializations, got {init_count}"
    assert len(cleanup) == 2, f"Expected 2 cleanups, got {len(cleanup)}"
    assert len(set(cleanup)) == 2, "Cleanup was not called for both tasks!"
