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

from stratae.lifecycle.lifecycle import AsyncLifecycle


def test_get_slots_not_in_stack(async_lifecycle: AsyncLifecycle, scopes: Sequence[str]):
    """
    Test that getting slots for a scope not in the stack raises an error.

    Given: A Lifecycle instance with no scopes pushed
    When: An attempt is made to get slots for a scope
    Then: A ValueError should be raised
    """
    # Arrange (done via fixture)

    # Act & Assert
    with pytest.raises(RuntimeError, match=f"Scope '{scopes[2]}' is not active."):
        async_lifecycle.get_slots(scopes[2])


def test_push_scope_not_defined(async_lifecycle: AsyncLifecycle):
    """
    Test pushing an undefined lifecycle scope onto the stack.

    Given: A Lifecycle instance and a scope not used during initialization
    When: An attempt is made to push a scope that is not allowed
    Then: A ValueError should be raised
    """
    # Arrange (done via fixture)

    # Act & Assert
    with pytest.raises(ValueError, match="Unknown scope: bad"):
        async_lifecycle.push("bad")


def test_push_scope(async_lifecycle: AsyncLifecycle, scopes: Sequence[str]):
    """
    Test pushing a new lifecycle scope onto the stack.

    Given: A Lifecycle instance and a LifecycleScope
    When: The scope is pushed onto the stack
    Then: The scope should be added to the lifecycle stack
    """
    # Arrange (done via fixture)

    # Act
    async_lifecycle.push(scopes[0])

    # Assert
    assert async_lifecycle.active_scopes() == [scopes[0]]


def test_push_multiple_scopes(async_lifecycle: AsyncLifecycle, scopes: Sequence[str]):
    """
    Test pushing multiple lifecycle scopes onto the stack.

    Given: A Lifecycle instance and multiple LifecycleScopes
    When: The scopes are pushed onto the stack
    Then: All scopes should be added to the lifecycle stack in order
    """
    # Arrange (done via fixture)

    # Act
    async_lifecycle.push(scopes[0])
    async_lifecycle.push(scopes[1])
    async_lifecycle.push(scopes[2])

    # Assert
    assert async_lifecycle.active_scopes() == [scopes[0], scopes[1], scopes[2]]
