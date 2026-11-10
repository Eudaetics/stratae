"""
Unit tests for SlotStore.allocate/release/get, through a Scope's store.

AsyncScope builds its store the same way, so testing through Scope covers both - there is no
separate async test module for this.

Slots 0-3 are reserved on every scope (the entered resources, the live-dependent count, the
reset token and the parent's storage), so the first slot SlotStore.allocate hands out is
always 4.
"""

import re

import pytest

from stratae.lifecycle._slots import UNSET, SlotDict, SlotStore
from stratae.lifecycle.exceptions import ScopeInactiveError
from stratae.lifecycle.scope import Scope


def _store(scope: Scope) -> SlotStore:
    """Return the scope's slot store."""
    return scope._slots  # pyright: ignore[reportPrivateUsage]


def test_allocate_sequential_dense():
    """
    Allocate hands out sequential ints for a dense-backed scope, skipping reserved slots 0-3.

    Given: A dense-backed scope with no functions registered yet,
    When: SlotStore.allocate is called repeatedly,
    Then: it returns 4, 5, 6, ..., none of them reused.
    """
    # Arrange
    scope = Scope("application", "shared")

    # Act & Assert
    assert _store(scope).allocate() == (4, False)
    assert _store(scope).allocate() == (5, False)
    assert _store(scope).allocate() == (6, False)


def test_allocate_sequential_sparse():
    """
    Allocate hands out sequential ints for a sparse-backed scope, skipping reserved slots 0-3.

    Given: A sparse-backed scope with no functions registered yet,
    When: SlotStore.allocate is called repeatedly,
    Then: it returns 4, 5, 6, ..., none of them reused.
    """
    # Arrange
    scope = Scope("application", "shared", "sparse")

    # Act & Assert
    assert _store(scope).allocate() == (4, False)
    assert _store(scope).allocate() == (5, False)
    assert _store(scope).allocate() == (6, False)


def test_allocate_reuses_released_slot_dense():
    """
    SlotStore.allocate draws from the free-slot pool before growing a dense scope's template.

    Given: A dense scope with two slots already allocated,
    When: The first slot is released and a third allocation is requested,
    Then: The released slot is handed back out, marked reused, instead of growing past the
        second slot.
    """
    # Arrange
    scope = Scope("application", "shared")
    slot_a, _ = _store(scope).allocate()
    slot_b, _ = _store(scope).allocate()

    # Act
    _store(scope).release(slot_a)
    slot_c, reused = _store(scope).allocate()

    # Assert
    assert slot_c == slot_a
    assert slot_c != slot_b
    assert reused


def test_allocate_reuses_released_slot_sparse():
    """
    SlotStore.allocate draws from the free-slot pool for sparse-backed scopes too.

    Given: A sparse-backed scope with two keys already allocated,
    When: The first key is released and a third allocation is requested,
    Then: The released key is handed back out, marked reused, instead of advancing the
        counter.
    """
    # Arrange
    scope = Scope("application", "shared", "sparse")
    key_a, _ = _store(scope).allocate()
    key_b, _ = _store(scope).allocate()

    # Act
    _store(scope).release(key_a)
    key_c, reused = _store(scope).allocate()

    # Assert
    assert key_c == key_a
    assert key_c != key_b
    assert reused


def test_allocate_dense_grows_template():
    """
    SlotStore.allocate grows the dense template by one entry per new slot.

    Given: A fresh dense scope, whose template reserves slots 0-3,
    When: A new slot is allocated,
    Then: The template grows to hold the new slot too.
    """
    # Arrange
    scope = Scope("application", "shared")
    template = _store(scope).template
    assert template is not None
    assert len(template) == 4

    # Act
    _store(scope).allocate()

    # Assert
    template = _store(scope).template
    assert template is not None
    assert len(template) == 5


def test_allocate_dense_grows_live_active_slots():
    """
    SlotStore.allocate appends to the live, active slot list too, not just the template.

    Given: A dense scope active with one slot already allocated,
    When: A second slot is allocated while the scope is still active,
    Then: The live slots list grows to make room for it.
    """
    # Arrange
    scope = Scope("application", "shared")
    _store(scope).allocate()
    scope.activate()

    # Act
    _store(scope).allocate()

    # Assert
    assert len(_store(scope).get()) == 6


def test_release_deletes_unwritten_sparse_key():
    """
    Releasing a sparse slot that was never written must not insert a phantom entry.

    Given: An active sparse scope with an allocated key that was never written to,
    When: That key is released,
    Then: The live dict gains no entry for that key - only the always-present reserved
        slots 0-3 remain.
    """
    # Arrange
    scope = Scope("application", "shared", "sparse")
    scope.activate()
    key, _ = _store(scope).allocate()

    # Act
    _store(scope).release(key)

    # Assert
    assert len(_store(scope).get()) == 4


def test_get_raises_when_inactive():
    """
    SlotStore.get raises when the scope has no active activation.

    Given: A Scope that has never been activated,
    When: SlotStore.get is called,
    Then: A ScopeInactiveError is raised.
    """
    # Arrange
    scope = Scope("request", "context")

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match=re.escape("Scope 'request' is not active.")):
        _store(scope).get()


def test_get_returns_live_storage_when_active():
    """
    SlotStore.get returns the live slot storage set up by activate().

    Given: An activated Scope,
    When: SlotStore.get is called,
    Then: It returns the same storage object activate() installed.
    """
    # Arrange
    scope = Scope("request", "context")

    # Act
    activation = scope.activate()

    # Assert
    assert _store(scope).get() is activation.slots


def test_get_sparse_returns_slotdict():
    """
    A sparse scope's live storage is backed by a SlotDict.

    Given: An activated sparse scope,
    When: SlotStore.get is called,
    Then: The returned storage is a SlotDict whose unwritten slots read as UNSET.
    """
    # Arrange
    scope = Scope("application", "shared", "sparse")
    scope.activate()

    # Act
    slots = _store(scope).get()

    # Assert
    assert isinstance(slots, SlotDict)
    assert slots[99] is UNSET
