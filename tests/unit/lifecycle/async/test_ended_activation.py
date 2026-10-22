"""Unit tests for reaching an ended AsyncScope activation from a task that outlived it."""

import asyncio
import threading

import pytest

from stratae.lifecycle.exceptions import CacheCycleError, ScopeInactiveError
from stratae.lifecycle.resource import async_resource, resource
from stratae.lifecycle.scope import AsyncScope


async def test_keyed_call_from_task_outliving_activation_raises(async_context_scope: AsyncScope):
    """
    A keyed cached call from a task that outlived its activation raises.

    Given: A task created during a context scope's activation, still running once it ends,
    When: the task calls a keyed cached function with a key cached during the activation,
    Then: ScopeInactiveError should be raised rather than the ended activation's value returned.
    """

    # Arrange
    @async_context_scope.cache()
    async def get_item(key: str) -> object:
        return object()

    activation_ended = asyncio.Event()

    async def outlive_activation() -> object:
        await activation_ended.wait()
        return await get_item("a")

    async with async_context_scope:
        await get_item("a")
        task = asyncio.create_task(outlive_activation())

    # Act
    activation_ended.set()

    # Assert
    with pytest.raises(ScopeInactiveError, match="activation ended"):
        await task


async def test_resource_entered_after_activation_ends_is_exited_at_once(
    async_context_scope: AsyncScope,
):
    """
    An async resource whose entry finishes only after its activation ended is exited.

    Given: A task part-way through entering a cached async resource,
    When: the activation ends before that entry finishes,
    Then: the resource should be exited as soon as its entry finishes, and the task's call
          should raise ScopeInactiveError.
    """
    # Arrange
    entering = asyncio.Event()
    finish_entering = asyncio.Event()
    events: list[str] = []

    @async_context_scope.cache()
    @async_resource
    async def get_connection():
        entering.set()
        await finish_entering.wait()
        events.append("open")
        try:
            yield object()
        finally:
            events.append("close")

    async with async_context_scope:
        task = asyncio.ensure_future(get_connection())
        await entering.wait()

    # Act
    finish_entering.set()

    # Assert
    with pytest.raises(ScopeInactiveError):
        await task
    assert events == ["open", "close"]


async def test_sync_resource_entered_after_activation_ends_is_exited_at_once(
    async_context_scope: AsyncScope,
):
    """
    A sync resource whose entry finishes on a worker thread after its activation ended is exited.

    Given: A worker thread (via asyncio.to_thread, which copies the context) part-way through
        entering a cached sync resource,
    When: the activation ends before that entry finishes,
    Then: the resource should be exited as soon as its entry finishes, and the call should
          raise ScopeInactiveError.
    """
    # Arrange
    entering = threading.Event()
    finish_entering = threading.Event()
    events: list[str] = []

    @async_context_scope.cache()
    @resource
    def get_connection():
        entering.set()
        finish_entering.wait(timeout=2)
        events.append("open")
        try:
            yield object()
        finally:
            events.append("close")

    async with async_context_scope:
        task = asyncio.ensure_future(asyncio.to_thread(get_connection))
        await asyncio.to_thread(entering.wait, 2)

    # Act
    finish_entering.set()

    # Assert
    with pytest.raises(ScopeInactiveError):
        await task
    assert events == ["open", "close"]


async def test_context_scope_keyed_recursion_does_not_deadlock(async_context_scope: AsyncScope):
    """
    A keyed cached coroutine function in a context scope can await itself with other keys.

    Given: A context-scope cached fib(n) that awaits fib(n - 1) and fib(n - 2),
    When: it's awaited for the first time,
    Then: every key should compute exactly once, without the claims deadlocking it.
    """
    # Arrange
    computed: list[int] = []

    @async_context_scope.cache()
    async def fib(n: int) -> int:
        computed.append(n)
        return n if n < 2 else await fib(n - 1) + await fib(n - 2)

    # Act
    async with async_context_scope:
        result = await asyncio.wait_for(fib(10), timeout=2)

    # Assert
    assert result == 55
    assert sorted(computed) == list(range(11))


async def test_context_scope_self_dependent_slot_raises_cycle_error(
    async_context_scope: AsyncScope,
):
    """
    A slot-eligible coroutine function in a context scope awaiting itself raises.

    Given: A context-scope ignore_params function whose computation awaits itself,
    When: it's awaited for the first time,
    Then: the inner call should raise CacheCycleError instead of recursing forever.
    """

    # Arrange
    @async_context_scope.cache(ignore_params=True)
    async def countdown(n: int) -> int:
        return await countdown(n - 1)

    # Act & Assert
    async with async_context_scope:
        with pytest.raises(CacheCycleError):
            await asyncio.wait_for(countdown(3), timeout=2)


async def test_waiter_raises_when_activation_ends_while_it_waits(async_context_scope: AsyncScope):
    """
    A task waiting on another's computation raises if the activation ends meanwhile.

    Given: One child task computing a context-scope cached coroutine function, and a second
        child task waiting on that same entry,
    When: the activation ends before the computation finishes,
    Then: the computing task should still get its value, and the waiting task should raise
          ScopeInactiveError rather than reuse the ended activation.
    """
    # Arrange
    computing = asyncio.Event()
    finish_computing = asyncio.Event()

    @async_context_scope.cache()
    async def get_value() -> str:
        computing.set()
        await finish_computing.wait()
        return "value"

    async with async_context_scope:
        owner = asyncio.ensure_future(get_value())
        await computing.wait()
        waiter = asyncio.ensure_future(get_value())
        await asyncio.sleep(0)  # let the waiter reach the claimed entry

    # Act
    finish_computing.set()

    # Assert
    assert await owner == "value"
    with pytest.raises(ScopeInactiveError):
        await waiter


async def test_owner_awaiting_its_own_watched_claim_raises_cycle_error(
    async_context_scope: AsyncScope,
):
    """
    A task awaiting its own unfinished entry raises, even once another task waits on it.

    Given: A context-scope ignore_params function that awaits itself part-way through, and a
        second task already waiting on the first task's computation of it,
    When: the first task's computation reaches the call to itself,
    Then: both tasks should raise CacheCycleError - the waiter once it retries the entry
          itself - rather than wait forever.
    """
    # Arrange
    at_gate = asyncio.Event()
    open_gate = asyncio.Event()

    @async_context_scope.cache(ignore_params=True)
    async def countdown(n: int) -> int:
        at_gate.set()
        await open_gate.wait()
        return await countdown(n - 1)

    # Act
    async with async_context_scope:
        owner = asyncio.ensure_future(countdown(3))
        await at_gate.wait()
        waiter = asyncio.ensure_future(countdown(3))
        await asyncio.sleep(0)  # let the waiter start watching the owner's claim
        open_gate.set()
        outcomes = await asyncio.wait_for(
            asyncio.gather(owner, waiter, return_exceptions=True), timeout=2
        )

    # Assert
    assert all(isinstance(outcome, CacheCycleError) for outcome in outcomes)
