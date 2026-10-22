"""Test that a wrapper on a reused slot never reads the dead wrapper's cached value."""

import asyncio
import gc
from contextvars import Context, copy_context
from inspect import isawaitable
from typing import Any, Callable, Literal

import pytest

from stratae.lifecycle import AsyncScope, async_resource, resource
from stratae.lifecycle.scope import IsolationType, StorageType

type Kind = Literal["sync", "async", "sync_resource", "async_resource"]


def _make_cached(
    scope: AsyncScope, kind: Kind, keyed: bool, value: int, calls: list[int]
) -> Callable[[str], Any]:
    """Build a cached wrapper of `kind` that records each computation of `value`."""
    cache = scope.cache(ignore_params=not keyed)
    if kind == "sync":

        def compute_sync(key: str) -> int:
            calls.append(value)
            return value

        return cache(compute_sync)
    if kind == "async":

        async def compute_async(key: str) -> int:
            calls.append(value)
            return value

        return cache(compute_async)
    if kind == "sync_resource":

        @resource
        def compute_sync_resource(key: str):
            calls.append(value)
            yield value

        return cache(compute_sync_resource)

    @async_resource
    async def compute_async_resource(key: str):
        calls.append(value)
        yield value

    return cache(compute_async_resource)


async def _call_in(context: Context, wrapper: Callable[[str], Any]) -> Any:
    """Call `wrapper` inside `context`, awaiting its result if it's async."""

    async def call() -> Any:
        result = wrapper("k")
        return await result if isawaitable(result) else result

    return await asyncio.create_task(call(), context=context)


@pytest.mark.parametrize("storage", ["dense", "sparse"])
@pytest.mark.parametrize("isolation", ["shared", "context"])
@pytest.mark.parametrize("keyed", [False, True], ids=["slot", "keyed"])
@pytest.mark.parametrize("kind", ["sync", "async", "sync_resource", "async_resource"])
async def test_reused_slot_wrapper_ignores_dead_wrappers_value(
    kind: Kind, keyed: bool, isolation: IsolationType, storage: StorageType
):
    """
    A wrapper on a reused slot computes and caches its own value in a live activation.

    Given: An activation, live in its own context, in which a cached wrapper has cached a
        value, before that wrapper is collected outside the activation's context
    When: A replacement wrapper is decorated, reusing the freed slot, and called twice in
        that activation
    Then: It computes its own value once and returns it both times, never the dead
        wrapper's value
    """
    # Arrange
    scope = AsyncScope("request", isolation, storage)
    calls: list[int] = []
    other = copy_context()
    activation = other.run(scope.activate)
    old = _make_cached(scope, kind, keyed, 1, calls)
    assert await _call_in(other, old) == 1

    del old
    gc.collect()
    assert scope._free_slots  # pyright: ignore[reportPrivateUsage]

    # Act
    new = _make_cached(scope, kind, keyed, 2, calls)
    first = await _call_in(other, new)
    second = await _call_in(other, new)

    # Assert
    assert not scope._free_slots  # pyright: ignore[reportPrivateUsage]
    assert (first, second) == (2, 2)
    assert calls == [1, 2]
    await asyncio.create_task(scope.deactivate(activation), context=other)


@pytest.mark.parametrize(
    ("isolation", "boxed"), [("shared", False), ("context", True)], ids=["shared", "context"]
)
def test_only_context_scope_reused_slots_are_boxed(isolation: IsolationType, boxed: bool):
    """
    Only a wrapper on a reused slot in a context scope boxes its values.

    Given: A scope whose first cached wrapper has been collected, freeing its slot
    When: A second wrapper is decorated, reusing that slot
    Then: It boxes its values only in a context scope, since a shared scope's one live
        storage is cleared when the slot is released; a wrapper on a new slot never does
    """
    # Arrange
    scope = AsyncScope("request", isolation)
    calls: list[int] = []
    fresh = _make_cached(scope, "sync", False, 1, calls)
    doomed = _make_cached(scope, "sync", False, 2, calls)
    del doomed
    gc.collect()

    # Act
    reused = _make_cached(scope, "sync", False, 3, calls)

    # Assert
    assert "__Owned__" not in fresh.__globals__
    assert ("__Owned__" in reused.__globals__) is boxed
