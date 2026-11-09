"""Test suite for the override tool in the dependency injection module."""

import asyncio
import contextvars
import gc
import threading
from typing import Annotated
from unittest.mock import AsyncMock, Mock

import pytest

from stratae.depends import Depends, inject, override
from stratae.depends._provide import Provider


def test_override_returns_value_while_active():
    """
    Provide should return the override value while the override is active.

    Given: a Provider for a dependency,
    When: it is overridden inside a with block,
    Then: provide should return the override value instead of the dependency's result.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    with override(dep).to("overridden"):
        assert depends.provide() == "overridden"


async def test_override_returns_value_while_active_async():
    """
    Provide should return the override value while the override is active, for an async dependency.

    Given: a Provider for an async dependency,
    When: it is overridden inside a with block,
    Then: awaiting provide should return the override value instead of the dependency's result.
    """
    # Arrange
    dep = AsyncMock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    with override(dep).to("overridden"):
        assert await depends.provide() == "overridden"


async def test_override_supports_repeated_provide_calls_async():
    """
    Provide should return the override value on every call, not just the first, for an async dep.

    Given: a Provider for an async dependency,
    When: it is overridden inside a with block and provide is awaited more than once,
    Then: every call should return the override value.
    """
    # Arrange
    dep = AsyncMock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    with override(dep).to("overridden"):
        assert await depends.provide() == "overridden"
        assert await depends.provide() == "overridden"


def test_override_restores_dependency_after_exit():
    """
    Provide should fall back to the dependency after the override exits.

    Given: a Provider for a dependency,
    When: an override is entered and then exited,
    Then: provide should call the real dependency again.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)

    # Act
    with override(dep).to("overridden"):
        ...

    # Assert
    assert depends.provide() == "real"


def test_override_does_not_call_dependency_while_active():
    """
    The real dependency should not be invoked while an override is active.

    Given: a Provider for a dependency,
    When: it is overridden inside a with block and provide is called,
    Then: the real dependency should not be invoked.
    """
    # Arrange
    dep = Mock()
    depends = Provider(dep)

    # Act
    with override(dep).to("overridden"):
        depends.provide()

    # Assert
    dep.assert_not_called()


def test_override_returns_provide_to_the_dependency_after_exit():
    """
    Once no context can see an override, provide should be the dependency itself again.

    Given: a Provider for a dependency,
    When: an override is entered and then exited,
    Then: provide is the dependency before, not while overridden, and the dependency after.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    assert depends.provide is dep

    with override(dep).to("overridden"):
        assert depends.provide is not dep

    assert depends.provide is dep


def test_nested_override_same_dependency():
    """
    Nested overrides on the same dependency should resolve and restore correctly.

    Given: a Provider for a dependency,
    When: it is overridden inside another override with a different value,
    Then: provide should reflect the innermost value while nested, and fall back
    to the outer value and then the real dependency as each override exits.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)

    # Act & Assert
    with override(dep).to("outer"):
        assert depends.provide() == "outer"

        with override(dep).to("inner"):
            assert depends.provide() == "inner"

        assert depends.provide() == "outer"

    assert depends.provide() == "real"


def test_several_overrides_apply_and_restore_together():
    """
    Several overrides in one with statement should apply together and restore together.

    Given: two Providers for separate dependencies,
    When: both are overridden in one with statement, and provide is called on each,
    Then: each returns its own override value without calling its dependency, and both
        call their real dependencies again after the block exits.
    """
    # Arrange
    dep_a = Mock(return_value="real-a")
    dep_b = Mock(return_value="real-b")
    wrapper_a = Provider(dep_a)
    wrapper_b = Provider(dep_b)

    # Act & Assert
    with override(dep_a).to("override-a"), override(dep_b).to("override-b"):
        assert wrapper_a.provide() == "override-a"
        assert wrapper_b.provide() == "override-b"
        dep_a.assert_not_called()
        dep_b.assert_not_called()

    assert wrapper_a.provide() == "real-a"
    assert wrapper_b.provide() == "real-b"


def test_several_overrides_unwind_entered_on_partial_failure():
    """
    If one of several overrides fails to enter, those already entered should be unwound.

    Given: two Providers where the second dep's lock raises on entry,
    When: both are overridden in one with statement, and the second fails to enter,
    Then: the first dep's override should be cleaned up and provide restored.
    """
    # Arrange
    dep_a = Mock(return_value="real-a")
    dep_b = Mock(return_value="real-b")
    wrapper_a = Provider(dep_a)
    wrapper_b = Provider(dep_b)

    failing_lock = Mock()
    failing_lock.__enter__ = Mock(side_effect=RuntimeError("forced failure"))
    failing_lock.__exit__ = Mock(return_value=False)
    wrapper_b.lock = failing_lock

    # Act
    with (
        pytest.raises(RuntimeError),
        override(dep_a).to("override-a"),
        override(dep_b).to("override-b"),
    ):
        ...

    # Assert
    assert wrapper_a.provide() == "real-a"
    assert wrapper_a.provide is dep_a


def test_override_applies_before_depends_is_evaluated():
    """
    An override entered before any Depends() for its provider should still apply.

    Given: a provider no annotation has used yet,
    When: it is overridden, and a function injecting it is defined inside the override,
    Then: the function gets the override value inside, and the real value after.
    """

    # Arrange
    def get_key() -> str:
        return "real"

    # Act & Assert
    with override(get_key).to("early"):

        @inject
        def use(key: Annotated[str, Depends(get_key)]) -> str:
            return key

        assert use() == "early"

    assert use() == "real"


async def test_override_holds_in_a_task_that_outlives_it():
    """
    A task created inside an override should keep it after the block exits.

    Given: a Provider for a dependency,
    When: a task is created inside an override, and reads the dependency after the block exits,
    Then: the task gets the override value, and code outside the block gets the real one.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    block_exited = asyncio.Event()

    async def read_later() -> str:
        await block_exited.wait()
        return depends.provide()

    # Act
    with override(dep).to("task"):
        task = asyncio.create_task(read_later())
    outside = depends.provide()
    block_exited.set()

    # Assert
    assert await task == "task"
    assert outside == "real"


async def test_async_override_held_by_a_task_falls_back_outside_it():
    """
    While a task holds an async provider's override, code outside it should get the real value.

    Given: an async provider, and a task created inside its override that's still running,
    When: code outside the block awaits the provider, and then the task does,
    Then: the outside code gets the real value, and the task gets the override value.
    """
    # Arrange
    dep = AsyncMock(return_value="real")
    depends = Provider(dep)
    block_exited = asyncio.Event()

    async def read_later() -> str:
        await block_exited.wait()
        return await depends.provide()

    with override(dep).to("task"):
        task = asyncio.create_task(read_later())

    # Act
    outside = await depends.provide()
    block_exited.set()

    # Assert
    assert outside == "real"
    assert await task == "task"


async def test_override_holds_in_a_task_group_child():
    """
    A TaskGroup child created inside an override should keep it after the block exits.

    Given: a Provider for a dependency, and a TaskGroup,
    When: a child is created inside an override, and reads the dependency after the block exits,
    Then: the child gets the override value.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    block_exited = asyncio.Event()

    async def read_later() -> str:
        await block_exited.wait()
        return depends.provide()

    # Act
    async with asyncio.TaskGroup() as group:
        with override(dep).to("child"):
            child = group.create_task(read_later())
        block_exited.set()

    # Assert
    assert child.result() == "child"


async def test_override_holds_in_to_thread():
    """
    A thread started with asyncio.to_thread inside an override should keep it after the block.

    Given: a Provider for a dependency,
    When: a to_thread call is started inside an override, and reads after the block exits,
    Then: the thread gets the override value.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    block_exited = threading.Event()

    def read_later() -> str:
        block_exited.wait()
        return depends.provide()

    # Act
    with override(dep).to("thread"):
        call = asyncio.ensure_future(asyncio.to_thread(read_later))
        await asyncio.sleep(0)
    block_exited.set()

    # Assert
    assert await call == "thread"


async def test_override_returns_provide_to_the_dependency_once_a_task_finishes():
    """
    Once a task holding an override finishes, provide should be the dependency itself again.

    Given: a Provider for a dependency, and a task created inside an override,
    When: the block exits, and then the task finishes and is dropped,
    Then: provide stays on the override path while the task runs, and returns after.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    release = asyncio.Event()

    async def hold() -> None:
        await release.wait()

    with override(dep).to("task"):
        task = asyncio.create_task(hold())

    # Act & Assert
    assert depends.provide is not dep
    release.set()
    await task
    del task
    for _ in range(3):
        await asyncio.sleep(0)
    assert depends.provide is dep


def test_override_thread_isolation():
    """
    Concurrent overrides on the same dependency should be isolated per thread.

    Given: a Provider for a dependency shared across threads,
    When: two threads each override it with a different value at the same time,
    Then: each thread should only observe its own override value, and the
    dependency should be restored once both overrides have exited.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    results: dict[str, str] = {}
    barrier = threading.Barrier(2)

    def run(name: str, value: str):
        with override(dep).to(value):
            barrier.wait()
            results[name] = depends.provide()

    thread_a = threading.Thread(target=run, args=("a", "value-a"))
    thread_b = threading.Thread(target=run, args=("b", "value-b"))

    # Act
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    # Assert
    assert results["a"] == "value-a"
    assert results["b"] == "value-b"
    assert depends.provide() == "real"


def test_override_can_be_reentered_while_held():
    """
    The same override object can be entered again while it's already held.

    Given: an override context manager for a dependency,
    When: it is entered, entered again inside itself, and both are exited,
    Then: the override applies throughout, and the real dependency is restored at the end.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    ctx = override(dep).to("fake")

    # Act & Assert
    with ctx:
        with ctx:
            assert depends.provide() == "fake"
        assert depends.provide() == "fake"
    assert depends.provide() == "real"


def test_override_released_by_garbage_collection_while_lock_is_held():
    """
    A holder freed by garbage collection on a thread holding the provider's lock is released.

    Given: an override value that only a garbage reference cycle keeps alive,
    When: a thread holding the provider's lock runs garbage collection,
    Then: the release runs on that thread without deadlocking, and provide is the dependency.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    context = contextvars.copy_context()
    context.run(override(dep).to("cyclic").__enter__)
    cycle: list[object] = [context]
    cycle.append(cycle)
    del context, cycle
    assert depends.provide is not dep

    def collect_under_lock() -> None:
        with depends.lock:
            gc.collect()

    # Act
    collector = threading.Thread(target=collect_under_lock, daemon=True)
    collector.start()
    collector.join(timeout=5)

    # Assert
    assert not collector.is_alive(), "release deadlocked on the provider's lock"
    assert depends.provide is dep


def test_override_exit_in_other_context_keeps_it_where_it_was_entered():
    """
    An override exited outside the context that entered it should stay in that context.

    Given: an override entered in one context,
    When: it is exited from a copied context, where resetting its token fails,
    Then: the reset error propagates, the entering context keeps the override, and exiting
        it there afterwards restores the real dependency.
    """
    # Arrange
    dep = Mock(return_value="real")
    depends = Provider(dep)
    ctx = override(dep).to("fake")
    ctx.__enter__()

    # Act
    with pytest.raises(ValueError, match="created in a different Context"):
        contextvars.copy_context().run(ctx.__exit__)

    # Assert
    assert depends.provide() == "fake"
    ctx.__exit__()
    assert depends.provide() == "real"
    assert depends.provide is dep
