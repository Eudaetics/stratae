"""
Unit tests for BaseScope.allocate_slot/release_slot/get_slots/exit_stack_type.

These are inherited unchanged by AsyncScope (the exit stack *type* differs per flavor, but
the resolution logic is shared), so testing them through Scope covers both - there is no
separate async test module for this.

Slots 0 and 1 are reserved on every scope (the exit stack and the live-dependent count),
so the first slot allocate_slot hands out is always 2.
"""

import pytest

from stratae.lifecycle._scope2 import AsyncScope, Scope
from stratae.lifecycle._slots import UNSET, SlotDict
from stratae.lifecycle._stack import AsyncExitStack, ExitStack
from stratae.lifecycle.exceptions import ScopeInactiveError


def test_allocate_slot_sequential_dense():
    """
    allocate_slot hands out sequential ints for a dense-backed scope, skipping slots 0 and 1.

    Given: A dense-backed scope with no functions registered yet,
    When: allocate_slot is called repeatedly,
    Then: it returns 2, 3, 4, ...
    """
    # Arrange
    scope = Scope("application", "shared")

    # Act & Assert
    assert scope.allocate_slot() == 2
    assert scope.allocate_slot() == 3
    assert scope.allocate_slot() == 4


def test_allocate_slot_sequential_sparse():
    """
    allocate_slot hands out sequential ints for a sparse-backed scope, skipping slots 0 and 1.

    Given: A sparse-backed scope with no functions registered yet,
    When: allocate_slot is called repeatedly,
    Then: it returns 2, 3, 4, ...
    """
    # Arrange
    scope = Scope("application", "shared", "sparse")

    # Act & Assert
    assert scope.allocate_slot() == 2
    assert scope.allocate_slot() == 3
    assert scope.allocate_slot() == 4


def test_allocate_slot_reuses_released_slot_dense():
    """
    allocate_slot draws from the free-slot pool before growing a dense scope's template.

    Given: A dense scope with two slots already allocated,
    When: The first slot is released and a third allocation is requested,
    Then: The released slot is handed back out instead of growing past the second slot.
    """
    # Arrange
    scope = Scope("application", "shared")
    slot_a = scope.allocate_slot()
    slot_b = scope.allocate_slot()

    # Act
    scope.release_slot(slot_a)
    slot_c = scope.allocate_slot()

    # Assert
    assert slot_c == slot_a
    assert slot_c != slot_b


def test_allocate_slot_reuses_released_slot_sparse():
    """
    allocate_slot draws from the free-slot pool for sparse-backed scopes too.

    Given: A sparse-backed scope with two keys already allocated,
    When: The first key is released and a third allocation is requested,
    Then: The released key is handed back out instead of advancing the counter.
    """
    # Arrange
    scope = Scope("application", "shared", "sparse")
    key_a = scope.allocate_slot()
    key_b = scope.allocate_slot()

    # Act
    scope.release_slot(key_a)
    key_c = scope.allocate_slot()

    # Assert
    assert key_c == key_a
    assert key_c != key_b


def test_allocate_slot_dense_grows_template():
    """
    allocate_slot grows the dense template by one entry per new slot.

    Given: A fresh dense scope, whose template reserves slots 0 and 1,
    When: A new slot is allocated,
    Then: The template grows to hold the new slot too.
    """
    # Arrange
    scope = Scope("application", "shared")
    assert len(scope._template) == 2  # pyright: ignore[reportPrivateUsage]

    # Act
    scope.allocate_slot()

    # Assert
    assert len(scope._template) == 3  # pyright: ignore[reportPrivateUsage]


def test_allocate_slot_dense_grows_live_active_slots():
    """
    allocate_slot appends to the live, active slot list too, not just the template.

    Given: A dense scope active with one slot already allocated,
    When: A second slot is allocated while the scope is still active,
    Then: The live slots list grows to make room for it.
    """
    # Arrange
    scope = Scope("application", "shared")
    scope.allocate_slot()
    scope.activate()

    # Act
    scope.allocate_slot()

    # Assert
    assert len(scope.get_slots()) == 4


def test_release_slot_deletes_unwritten_sparse_key():
    """
    Releasing a sparse slot that was never written must not insert a phantom entry.

    Given: An active sparse scope with an allocated key that was never written to,
    When: That key is released,
    Then: The live dict gains no entry for that key - only slot 1's always-present
        live-dependent count remains.
    """
    # Arrange
    scope = Scope("application", "shared", "sparse")
    scope.activate()
    key = scope.allocate_slot()

    # Act
    scope.release_slot(key)

    # Assert
    assert len(scope.get_slots()) == 1


def test_get_slots_raises_when_inactive():
    """
    get_slots raises when the scope has no active activation.

    Given: A Scope that has never been activated,
    When: get_slots is called,
    Then: A ScopeInactiveError is raised.
    """
    # Arrange
    scope = Scope("request", "context")

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match="Scope 'request' is not active."):
        scope.get_slots()


def test_get_slots_returns_live_storage_when_active():
    """
    get_slots returns the live slot storage set up by activate().

    Given: An activated Scope,
    When: get_slots is called,
    Then: It returns the same storage object activate() installed.
    """
    # Arrange
    scope = Scope("request", "context")

    # Act
    activation = scope.activate()

    # Assert
    assert scope.get_slots() is activation.slots


def test_get_slots_sparse_returns_slotdict():
    """
    A sparse scope's live storage is backed by a SlotDict.

    Given: An activated sparse scope,
    When: get_slots is called,
    Then: The returned storage is a SlotDict whose unwritten slots read as UNSET.
    """
    # Arrange
    scope = Scope("application", "shared", "sparse")
    scope.activate()

    # Act
    slots = scope.get_slots()

    # Assert
    assert isinstance(slots, SlotDict)
    assert slots[99] is UNSET


def test_exit_stack_type_sync():
    """
    Scope.exit_stack_type returns the sync ExitStack class.

    Given: A Scope,
    When: exit_stack_type is called,
    Then: It returns ExitStack.
    """
    # Arrange
    scope = Scope("application", "shared")

    # Act & Assert
    assert scope.exit_stack_type() is ExitStack


def test_exit_stack_type_async():
    """
    AsyncScope.exit_stack_type returns the AsyncExitStack class.

    Given: An AsyncScope,
    When: exit_stack_type is called,
    Then: It returns AsyncExitStack.
    """
    # Arrange
    scope = AsyncScope("application", "shared")

    # Act & Assert
    assert scope.exit_stack_type() is AsyncExitStack
