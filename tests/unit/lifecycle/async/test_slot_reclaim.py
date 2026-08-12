"""Test suite for reclaiming dead cache-decorated functions' slots in dense/sparse scopes."""

import gc
from unittest.mock import Mock, call

from stratae.lifecycle.scope import AsyncScope


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


async def test_reclaimed_slot_does_not_leak_stale_value(async_scope: AsyncScope):
    """
    A function assigned a reclaimed slot must never see the dead function's cached value.

    Given: An async cache-decorated closure that is called, then becomes unreachable
    When: A second, unrelated closure is decorated afterward and assignee the same slot,
    Then: The second closure's first call cache check does not clash
    """
    # Arrange
    call_counter = Mock()

    def make_get_a():
        @async_scope.cache()
        async def get_a():
            call_counter("a")
            return SimpleObject(1)

        return get_a

    def make_get_b():
        @async_scope.cache()
        async def get_b():
            call_counter("b")
            return SimpleObject(2)

        return get_b

    async with async_scope.activate():
        # Act
        get_a = make_get_a()
        obj_a = await get_a()

        del get_a
        gc.collect()

        get_b = make_get_b()
        obj_b = await get_b()

    # Assert
    assert obj_a.value == 1
    assert obj_b.value == 2
    assert call_counter.call_args_list == [call("a"), call("b")]


async def test_hot_loop_template_growth_is_bounded(async_scope: AsyncScope):
    """
    Repeatedly decorating-and-discarding a closure must not grow the scope's slots forever.

    Given: A shared dense scope activated once
    When: A cache-decorated closure is defined, called, and dropped many times
    Then: The scope's live slot list only ever grows on the first iteration
    """
    async with async_scope.activate():
        for _ in range(5):

            @async_scope.cache()
            async def get_value():
                return SimpleObject(1)

            await get_value()
            del get_value
            gc.collect()

    assert len(async_scope._template) == 3  # pyright: ignore[reportPrivateUsage]


async def test_context_scope_template_growth_is_bounded(async_context_scope: AsyncScope):
    """
    A context-isolated dense scope's template must not grow forever either.

    Given: A context-isolated scope that is never activated
    When: A cache-decorated closure is defined and dropped many times
    Then: The template only ever grows on the first allocation
    """
    for _ in range(10):

        @async_context_scope.cache()
        async def foo():
            return SimpleObject(1)

        del foo
        gc.collect()

    assert len(async_context_scope._template) == 3  # pyright: ignore[reportPrivateUsage]
