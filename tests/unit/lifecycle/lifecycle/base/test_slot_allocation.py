"""
Test suite for BaseLifecycle.allocate_slot/release_slot, exercised via Lifecycle.

These are inherited unchanged by AsyncLifecycle, so testing them through Lifecycle
covers both - there is no separate async test module for this. Slot reclaim/allocation
as it interacts with the cache-decorator/wrapper codegen path is genuinely separate
code between sync and async and stays covered in
tests/unit/lifecycle/lifecycle/sync/test_lifecycle_slot_reclaim.py and
tests/unit/lifecycle/lifecycle/async/test_alifecycle_slot_reclaim.py.
"""

from typing import Sequence

import pytest

from stratae.lifecycle import Lifecycle, Scope


@pytest.fixture
def sparse_lifecycle():
    """Provide a Lifecycle with a sparse-backed shared scope and a sparse-backed context scope."""
    yield Lifecycle(
        [Scope("app_sparse", "shared", "sparse"), Scope("req_sparse", "context", "sparse")]
    )


def test_allocate_slot_reuses_released_slot(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    allocate_slot draws from the free-slot pool before growing a shared dense scope.

    Given: A shared dense scope ("application") with two slots already allocated,
    When: The first slot is released and a third allocation is requested,
    Then: The released slot is handed back out instead of growing past the second slot.
    """
    # Arrange
    slot_a = lifecycle.allocate_slot(scopes[0])
    slot_b = lifecycle.allocate_slot(scopes[0])

    # Act
    lifecycle.release_slot(scopes[0], slot_a)
    slot_c = lifecycle.allocate_slot(scopes[0])

    # Assert
    assert slot_c == slot_a
    assert slot_c != slot_b


def test_allocate_slot_reuses_released_slot_for_sparse_scope():
    """
    allocate_slot draws from the free-slot pool for sparse-backed scopes too.

    Given: A sparse-backed scope with two keys already allocated,
    When: The first key is released and a third allocation is requested,
    Then: The released key is handed back out instead of advancing the counter.
    """
    # Arrange
    sparse_lifecycle = Lifecycle([Scope("sparse_scope", "shared", "sparse")])
    key_a = sparse_lifecycle.allocate_slot("sparse_scope")
    key_b = sparse_lifecycle.allocate_slot("sparse_scope")

    # Act
    sparse_lifecycle.release_slot("sparse_scope", key_a)
    key_c = sparse_lifecycle.allocate_slot("sparse_scope")

    # Assert
    assert key_c == key_a
    assert key_c != key_b


@pytest.mark.parametrize("scope", ["app_sparse", "req_sparse"])
def test_allocate_slot_sequential_for_sparse_scope(sparse_lifecycle: Lifecycle, scope: str):
    """
    allocate_slot hands out sequential int keys for a sparse-backed scope, skipping slot 0.

    Given: A sparse-backed scope with no functions registered yet,
    When: allocate_slot is called repeatedly,
    Then: It returns 1, 2, 3, ... - slot 0 stays reserved for the lazily-created exit stack.
    """
    # Act & Assert
    assert sparse_lifecycle.allocate_slot(scope) == 1
    assert sparse_lifecycle.allocate_slot(scope) == 2
    assert sparse_lifecycle.allocate_slot(scope) == 3
