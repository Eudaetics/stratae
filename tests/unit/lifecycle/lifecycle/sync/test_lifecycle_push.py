"""
Unit tests for the push functionality of the Lifecycle class.

This test suite verifies the following behaviors:
- Attempting to get slots for a scope not in the stack raises a ValueError.
- Pushing an undefined scope onto the stack raises a ValueError.
- Pushing a valid scope adds it to the lifecycle stack.
- Pushing multiple valid scopes adds all of them to the stack in order.
"""

from typing import Sequence

import pytest

from stratae.lifecycle import Lifecycle


def test_get_slots_not_in_stack(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test that getting slots for a scope not in the stack raises an error.

    Given: A Lifecycle instance with no scopes pushed
    When: An attempt is made to get slots for a scope
    Then: A ValueError should be raised
    """
    # Act & Assert
    with pytest.raises(RuntimeError, match=f"Scope '{scopes[2]}' is not active."):
        lifecycle.get_slots(scopes[2])


def test_push_scope_not_defined(lifecycle: Lifecycle):
    """
    Test pushing an undefined lifecycle scope onto the stack.

    Given: A Lifecycle instance and a scope not used during initialization
    When: An attempt is made to push a scope that is not allowed
    Then: A ValueError should be raised
    """
    # Act & Assert
    with pytest.raises(ValueError, match="Unknown scope: bad"):
        lifecycle.push("bad")


def test_push_scope(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test pushing a new lifecycle scope onto the stack.

    Given: A Lifecycle instance and a LifecycleScope
    When: The scope is pushed onto the stack
    Then: The scope should be added to the lifecycle stack
    """
    # Act
    lifecycle.push(scopes[0])

    # Assert
    assert lifecycle.active_scopes() == [scopes[0]]


def test_push_multiple_scopes(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test pushing multiple lifecycle scopes onto the stack.

    Given: A Lifecycle instance and multiple LifecycleScopes
    When: The scopes are pushed onto the stack
    Then: All scopes should be added to the lifecycle stack in order
    """
    # Act
    lifecycle.push(scopes[0])
    lifecycle.push(scopes[1])
    lifecycle.push(scopes[2])

    # Assert
    assert lifecycle.active_scopes() == [scopes[0], scopes[1], scopes[2]]
