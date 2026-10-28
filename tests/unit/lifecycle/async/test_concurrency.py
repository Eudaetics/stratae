"""
Test suite for concurrent context isolation and resource management in AsyncScope.

This module contains asynchronous tests to ensure that:
- Concurrent execution contexts maintain isolated context-scoped caches.
- Context-scoped cached functions behave correctly under concurrent access.
- Multiple context-scoped calls within each context do not share cache across activations.
- Scoped resources are managed independently and cleaned up correctly in concurrent contexts.
- Concurrent first calls within one activation enter a cached resource once, however many
  tasks wait on it.
- A requires chain keeps a shared parent's cache visible to every concurrent child while
  each child's own cache stays isolated per task.
"""

import asyncio
from typing import Any

import pytest

from stratae.lifecycle.exceptions import ScopeInactiveError
from stratae.lifecycle.resource import async_resource
from stratae.lifecycle.scope import AsyncScope


async def test_concurrent_cache_isolation(async_context_scope: AsyncScope):
    """
    Verify concurrent CONTEXTS have isolated caches for a context-isolated scope.

    Given: Two separate execution contexts,
    When: Each context uses a context-scoped cached function,
    Then: Each context should have its own isolated cache.
    """
    # Arrange
    call_count = 0
    iterations = 5
    lock = asyncio.Lock()

    @async_context_scope.cache()
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
        async with async_context_scope:
            for _ in range(iterations):
                result = await get_user_id()
                results_a.append(result)

    async def task_b():
        async with async_context_scope:
            for _ in range(iterations):
                result = await get_user_id()
                results_b.append(result)

    task_a_handle = asyncio.create_task(task_a())
    task_b_handle = asyncio.create_task(task_b())

    # Act
    await asyncio.gather(task_a_handle, task_b_handle)

    # Assert
    # Each task should get the same value every time (cached within its activation)
    assert len(set(results_a)) == 1, f"Task A got different values: {results_a}"
    assert len(set(results_b)) == 1, f"Task B got different values: {results_b}"

    # But the two tasks should get different values (isolated)
    assert results_a[0] != results_b[0], "Tasks got the same cached value!"

    # Total calls should be 2 (once per task, cached within task)
    assert call_count == 2, f"Expected 2 calls, got {call_count}"


async def test_concurrent_cache_isolation_multiple_requests(async_context_scope: AsyncScope):
    """
    Verify concurrent CONTEXTS have isolated caches across multiple activations each.

    This test extends the previous one to simulate multiple activations of the scope
    within each context to ensure that caching works correctly across multiple calls. The
    goal is to confirm that each context maintains its own isolated cache even when
    activating the scope several times, similar to real-world per-request API usage.

    Given: Two separate execution contexts,
    When: Each context activates the context-scoped cached function's scope multiple times,
    Then: Each activation should get its own isolated cache.
    """
    # Arrange
    call_count = 0
    iterations = 5
    lock = asyncio.Lock()

    @async_context_scope.cache()
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
            async with async_context_scope:
                result = await get_user_id()
                results_a.append(result)

    async def task_b():
        for _ in range(iterations):
            async with async_context_scope:
                result = await get_user_id()
                results_b.append(result)

    task_a_handle = asyncio.create_task(task_a())
    task_b_handle = asyncio.create_task(task_b())

    # Act
    await asyncio.gather(task_a_handle, task_b_handle)

    # Assert
    # Each task should get unique values across the calls (not cached across activations)
    assert len(results_a) == len(set(results_a)), f"Task A got a duplicate value: {results_a}"
    assert len(results_b) == len(set(results_b)), f"Task B got a duplicate value: {results_b}"

    # The two tasks should get completely different values (isolated)
    assert len(set(results_b) | set(results_a)) == len(results_a) + len(results_b)

    # Total calls should be once per activation, no caching across activations
    assert call_count == iterations * 2, f"Expected {iterations * 2} calls, got {call_count}"


async def test_concurrent_resource_management(async_context_scope: AsyncScope):
    """
    Verify concurrent contexts manage scoped resources independently.

    Given: Two separate execution contexts,
    When: Each context uses a context-scoped resource,
    Then: Each context should manage its own resource lifecycle independently.
    """
    # Arrange
    init_count = 0
    cleanup: list[asyncio.Task[Any]] = []
    lock = asyncio.Lock()

    @async_context_scope.cache()
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
        async with async_context_scope:
            results_a.append(await resource_generator())

    async def task_b():
        async with async_context_scope:
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


async def test_concurrent_resource_management_nested_scopes(
    async_scope_chain: tuple[AsyncScope, AsyncScope, AsyncScope],
):
    """
    Verify concurrent contexts manage nested scoped resources independently.

    This test ensures that when multiple concurrent tasks access resources within nested
    lifecycle scopes chained by requires (application <- session <- request), the
    application-scoped resources are properly shared while session-scoped resources
    remain isolated per task.

    Given: Two separate execution contexts within nested application/session/request scopes,
    When: Each context uses both application- and session-scoped resources,
    Then: Application resources should be shared, session resources should be isolated,
          and all resources should be cleaned up properly for each task.
    """
    # Arrange
    application, session, request = async_scope_chain

    init_count = 0
    cleanup: list[asyncio.Task[Any]] = []
    lock = asyncio.Lock()

    app_count = 0
    app_cleanup: list[asyncio.Task[Any]] = []

    @application.cache()
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

    @session.cache()
    @async_resource
    async def resource_generator():
        """Simulate a resource with setup and teardown."""
        nonlocal init_count, cleanup

        assert await app_resource_generator() == 1, (
            "Application resource should be shared across sessions"
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
        async with session:
            first = await resource_generator()
            async with request:
                second = await resource_generator()
                results_a.append(second)
                assert first == second, "Session resource should be the same within the same task"

    async def task_b():
        async with session:
            first = await resource_generator()
            async with request:
                second = await resource_generator()
                results_b.append(second)
                assert first == second, "Session resource should be the same within the same task"

    async with application:
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


async def test_concurrent_first_calls_within_one_activation_enter_resource_once(
    async_context_scope: AsyncScope,
):
    """
    Child tasks sharing one activation share a single entry of a cached resource.

    Given: An active context scope and a cached resource that suspends while entering,
    When: two child tasks of the activation (via gather) call it for the first time at once,
    Then: the resource should be entered exactly once, and both tasks should get its value.
    """
    # Arrange
    enter_count = 0

    @async_context_scope.cache()
    @async_resource
    async def get_connection():
        nonlocal enter_count
        enter_count += 1
        await asyncio.sleep(0.01)
        yield object()

    # Act
    async with async_context_scope:
        first, second = await asyncio.gather(get_connection(), get_connection())

    # Assert
    assert enter_count == 1
    assert first is second


async def test_third_concurrent_first_call_waits_on_watched_claim(
    async_context_scope: AsyncScope,
):
    """
    A third concurrent first call waits on the claim another task is already watching.

    Given: An active context scope and a cached resource that suspends while entering,
    When: three child tasks of the activation (via gather) call it for the first time at once,
    Then: the resource should be entered exactly once, and all three tasks should get its value.
    """
    # Arrange
    enter_count = 0

    @async_context_scope.cache()
    @async_resource
    async def get_connection():
        nonlocal enter_count
        enter_count += 1
        await asyncio.sleep(0.01)
        yield object()

    # Act
    async with async_context_scope:
        first, second, third = await asyncio.gather(
            get_connection(), get_connection(), get_connection()
        )

    # Assert
    assert enter_count == 1
    assert first is second is third


async def test_task_outliving_activation_cannot_use_its_cache(async_context_scope: AsyncScope):
    """
    A task that outlives its activation can't reuse or extend the ended activation's cache.

    Given: A task created inside a context scope activation, still running once it ends,
    When: that task calls one of the scope's cached functions after the activation has ended,
    Then: the call should raise ScopeInactiveError, as for any caller outside an activation.
    """

    # Arrange
    @async_context_scope.cache()
    async def get_value() -> object:
        return object()

    activation_ended = asyncio.Event()

    async def outlive_activation() -> object:
        await activation_ended.wait()
        return await get_value()

    async with async_context_scope:
        await get_value()
        task = asyncio.create_task(outlive_activation())

    # Act
    activation_ended.set()

    # Assert
    with pytest.raises(ScopeInactiveError):
        await task
