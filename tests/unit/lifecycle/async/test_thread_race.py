"""Tests for threads=True async context scopes - threads sharing one activation."""

import asyncio
import threading
import time
from contextvars import copy_context
from typing import Any, Awaitable, Callable

import pytest

from stratae.lifecycle.exceptions import LifecycleConfigurationError
from stratae.lifecycle.scope import AsyncScope


async def _gather_loops(target: Callable[[], Awaitable[Any]], count: int) -> list[Any]:
    """
    Await target on `count` threads, each running its own event loop.

    `to_thread` copies the context, so every loop shares the caller's activation.
    """
    return await asyncio.gather(
        *(asyncio.to_thread(asyncio.run, target()) for _ in range(count))  # pyright: ignore[reportArgumentType]
    )


async def test_threaded_sync_function_computed_once_across_threads(
    async_threaded_scope: AsyncScope,
):
    """
    Verify a sync cached function is computed once across threads sharing an activation.

    Given: a threads=True async context scope, and several to_thread calls sharing one
           activation,
    When: they all call a sync cached function for the first time,
    Then: the underlying function should run exactly once, and every call should see the
          same value.
    """
    # Arrange
    call_count = 0

    @async_threaded_scope.cache()
    def get_thing() -> object:
        nonlocal call_count
        call_count += 1
        time.sleep(0.01)
        return object()

    # Act
    async with async_threaded_scope:
        results = await asyncio.gather(*(asyncio.to_thread(get_thing) for _ in range(10)))

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


async def test_threaded_async_slot_eligible_computed_once_across_loops(
    async_threaded_scope: AsyncScope,
):
    """
    Verify an async cached function is computed once across event loops sharing an activation.

    Given: a threads=True async context scope, and several threads each running an event
           loop in a copy of the activation's context,
    When: they all await a cached function for the first time,
    Then: the underlying function should run exactly once, and every loop should see the
          same value.
    """
    # Arrange
    call_count = 0

    @async_threaded_scope.cache()
    async def get_thing() -> object:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return object()

    # Act
    async with async_threaded_scope:
        results = await _gather_loops(get_thing, 5)

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


async def test_threaded_async_keyed_computed_once_across_loops(
    async_threaded_scope: AsyncScope,
):
    """
    Verify a keyed async cached function computes each key once across event loops.

    Given: a threads=True async context scope, and several threads each running an event
           loop in a copy of the activation's context,
    When: they all await a keyed cached function with the same argument,
    Then: the underlying function should run exactly once for that key.
    """
    # Arrange
    call_count = 0

    @async_threaded_scope.cache()
    async def get_thing(x: int) -> object:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)
        return object()

    # Act
    async with async_threaded_scope:
        results = await _gather_loops(lambda: get_thing(1), 5)

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


async def test_threaded_async_computed_once_across_tasks(async_threaded_scope: AsyncScope):
    """
    Verify an async cached function is still computed once across tasks on one loop.

    Given: a threads=True async context scope, and several tasks sharing one activation,
    When: they all await a cached function for the first time,
    Then: the underlying function should run exactly once.
    """
    # Arrange
    call_count = 0

    @async_threaded_scope.cache()
    async def get_thing() -> object:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return object()

    # Act
    async with async_threaded_scope:
        results = await asyncio.gather(*(get_thing() for _ in range(10)))

    # Assert
    assert call_count == 1
    assert len({id(value) for value in results}) == 1


def _run_loop_in_thread(
    main: Callable[[], Awaitable[Any]], outcomes: dict[str, Any], name: str
) -> threading.Thread:
    """
    Run main on a new event loop in a daemon thread, in a copy of the calling context.

    Records its result or exception in outcomes under name. Daemon, so a hang fails the
    test instead of blocking interpreter exit.
    """

    def run() -> None:
        try:
            outcomes[name] = asyncio.run(main())  # pyright: ignore[reportArgumentType]
        except BaseException as exc:
            outcomes[name] = exc

    thread = threading.Thread(target=copy_context().run, args=(run,), daemon=True)
    thread.start()
    return thread


@pytest.mark.parametrize("watched", [False, True], ids=["task-claim", "watched-claim"])
async def test_context_scope_cross_loop_waiter_raises(
    async_context_scope: AsyncScope, watched: bool
):
    """
    Awaiting another event loop's in-progress computation raises instead of hanging.

    Given: a default context scope, and a cached function being computed on an event loop
           in another thread that shares the activation (already waited on by a task on its
           own loop, when watched),
    When: a loop on a third thread awaits the same entry,
    Then: that waiter should raise LifecycleConfigurationError, and the owner's loop should
          still compute and share the value.
    """
    # Arrange
    started = threading.Event()
    release = threading.Event()
    calls = 0

    @async_context_scope.cache()
    async def get_thing() -> object:
        nonlocal calls
        calls += 1
        started.set()
        while not release.is_set():
            await asyncio.sleep(0.005)
        return object()

    async def owner_loop() -> list[object]:
        if watched:
            return list(await asyncio.gather(get_thing(), get_thing()))
        return [await get_thing()]

    outcomes: dict[str, Any] = {}

    # Act
    async with async_context_scope:
        owner = _run_loop_in_thread(owner_loop, outcomes, "owner")
        started.wait(timeout=2)
        await asyncio.sleep(0.02)  # let a watching task reach the claim
        waiter = _run_loop_in_thread(get_thing, outcomes, "waiter")
        waiter.join(timeout=2)
        release.set()
        owner.join(timeout=2)

    # Assert
    assert not waiter.is_alive(), "cross-loop waiter hung"
    assert not owner.is_alive(), "owner loop hung"
    assert isinstance(outcomes["waiter"], LifecycleConfigurationError)
    assert len({id(value) for value in outcomes["owner"]}) == 1
    assert calls == 1
