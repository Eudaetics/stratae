"""Test suite for reclaiming dead cache-decorated functions' slots in dense/sparse scopes."""

import gc
from contextvars import copy_context
from unittest.mock import Mock, call

import pytest

from stratae.lifecycle.scope import Scope, StorageType


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


def test_context_scope_template_growth_is_bounded(context_scope: Scope):
    """
    A context-isolated dense scope's template must not grow forever either.

    Given: A context-isolated scope that is never activated,
    When: A cache-decorated closure is defined and dropped many times
    Then: The template only ever grows on the first allocation
    """
    for _ in range(5):

        @context_scope.cache()
        def get_value():
            return SimpleObject(1)

        del get_value
        gc.collect()

    assert len(context_scope._template) == 5  # pyright: ignore[reportPrivateUsage]


def test_reclaimed_slot_does_not_leak_stale_value(scope: Scope):
    """
    A function assigned a reclaimed slot must never see the dead function's cached value.

    Given: A cache-decorated closure that is called, then becomes unreachable
    When: A second, unrelated closure is decorated afterward and assigned the same slot,
    Then: The second closure's first call cache check does not clash
    """
    # Arrange
    call_counter = Mock()

    def make_get_a():
        @scope.cache()
        def get_a():
            call_counter("a")
            return SimpleObject(1)

        return get_a

    def make_get_b():
        @scope.cache()
        def get_b():
            call_counter("b")
            return SimpleObject(2)

        return get_b

    with scope:
        # Act
        get_a = make_get_a()
        obj_a = get_a()

        del get_a
        gc.collect()

        get_b = make_get_b()
        obj_b = get_b()

    # Assert
    assert obj_a.value == 1
    assert obj_b.value == 2
    assert call_counter.call_args_list == [call("a"), call("b")]


def test_hot_loop_template_growth_is_bounded(scope: Scope):
    """
    Repeatedly decorating-and-discarding a closure must not grow the scope's slots forever.

    Given: A shared dense scope activated once, matching how an application scope is
           typically kept active for a process's entire lifetime,
    When: A cache-decorated closure is defined, called, and dropped many times in a loop -
          the exact pattern that previously grew the scope's slot storage without bound,
    Then: The scope's live slot list only ever grows on the first iteration - every later
          iteration reuses the freed slot instead of appending a new one.
    """
    with scope:
        for _ in range(5):

            @scope.cache()
            def get_value():
                return SimpleObject(1)

            get_value()
            del get_value
            gc.collect()

    assert len(scope._template) == 5  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize("storage", ["dense", "sparse"])
def test_reclaimed_slot_is_cleared_in_another_contexts_activation(storage: StorageType):
    """
    A reclaimed slot is cleared in every live activation, not just the releasing context's.

    Given: A context-isolated scope activated in a separate context, as a concurrent
        request would be, where a cache-decorated closure has cached a value
    When: The closure is collected outside that context, and a second closure is decorated
        afterward and assigned the same slot
    Then: The second closure computes its own value in that activation, rather than
        reading the dead closure's
    """
    # Arrange
    request = Scope("request", "context", storage)

    def make_cached(value: int):
        @request.cache()
        def get_value():
            return SimpleObject(value)

        return get_value

    old = make_cached(1)
    other_context = copy_context()
    activation = other_context.run(request.activate)
    other_context.run(old)

    # Act
    del old
    gc.collect()
    new = make_cached(2)
    result = other_context.run(new)

    # Assert
    assert result.value == 2
    other_context.run(request.deactivate, activation)


def test_reclaimed_slot_is_freed_when_released_inside_an_unextended_activation(
    context_scope: Scope,
):
    """
    Releasing a slot inside an activation whose dense storage never reached it still frees it.

    Given: A context-isolated dense scope activated before a closure's slot was allocated,
        so that activation's storage was never extended to the slot
    When: The closure is collected while running inside that activation's context
    Then: The slot still returns to the free pool, and the next closure reuses it
    """
    # Arrange
    other_context = copy_context()
    activation = other_context.run(context_scope.activate)

    @context_scope.cache()
    def get_value():
        return SimpleObject(1)

    # Act
    del get_value
    other_context.run(gc.collect)

    @context_scope.cache()
    def get_other():
        return SimpleObject(2)

    result = other_context.run(get_other)

    # Assert
    assert result.value == 2
    assert len(context_scope._template) == 5  # pyright: ignore[reportPrivateUsage]
    other_context.run(context_scope.deactivate, activation)
