"""
Test suite for reclaiming dead cache-decorated functions' slots in dense/sparse scopes.

allocate_slot/release_slot themselves are BaseScope behavior and are only covered once, via
Scope, in tests/unit/lifecycle/scope2/base/test_slots.py. This file covers slot reclaim as
it interacts with the sync cache-decorator/wrapper codegen path, which is genuinely
separate code from the async path.
"""

import gc
from unittest.mock import Mock, call

from stratae.lifecycle.scope import Scope


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


def test_release_slot_deletes_unwritten_sparse_key():
    """
    Releasing a sparse-backed slot that was never written must not insert a phantom entry.

    Given: A sparse-backed shared scope with a cache-decorated closure that's allocated a
           key but never called - SlotDict.__missing__ means its key was never actually
           inserted into the live dict,
    When: That closure becomes unreachable and its key is released,
    Then: The live dict gains no entry for that key, only slot 1's always-present
          live-dependent count - inserting a bare UNSET placeholder for the released key
          would defeat sparse storage's entire point of costing O(touched), not
          O(registered).
    """
    # Arrange
    scope = Scope("sparse_scope", "shared", "sparse")

    def make_get_value():
        @scope.cache()
        def get_value():
            return SimpleObject(1)

        return get_value

    # Act
    with scope.activate():
        get_value = make_get_value()
        del get_value
        gc.collect()

        # Assert
        assert len(scope.get_slots()) == 2


def test_context_scope_template_growth_is_bounded(context_scope: Scope):
    """
    A context-isolated dense scope's template must not grow forever either.

    Given: A context-isolated scope that is never activated - decorating against it
           dynamically doesn't require an active activation,
    When: A cache-decorated closure is defined and dropped many times in a loop without
          ever being called - the pattern that previously grew the scope's template
          without bound, since only shared scopes drew from the free-slot pool,
    Then: The template only ever grows on the first allocation - every later iteration
          reuses the freed index instead of appending a new one. The template holds no
          live value at any index (only an activation's copy or a shared scope's
          permanent list ever get written into), so recycling its indices needs no extra
          care here.
    """
    for _ in range(5):

        @context_scope.cache()
        def get_value():
            return SimpleObject(1)

        del get_value
        gc.collect()

    assert len(context_scope._template) == 3  # pyright: ignore[reportPrivateUsage]


def test_reclaimed_slot_does_not_leak_stale_value(scope: Scope):
    """
    A function assigned a reclaimed slot must never see the dead function's cached value.

    Given: A cache-decorated closure that is called, then becomes unreachable while its
           shared scope stays active,
    When: A second, unrelated closure is decorated afterward and happens to be assigned
          the same now-freed slot,
    Then: The second closure's first call is a genuine cache miss - it computes and caches
          its own value rather than reading the first closure's leftover cached value.
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

    with scope.activate():
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
    with scope.activate():
        for _ in range(5):

            @scope.cache()
            def get_value():
                return SimpleObject(1)

            get_value()
            del get_value
            gc.collect()

    assert len(scope._template) == 3  # pyright: ignore[reportPrivateUsage]
