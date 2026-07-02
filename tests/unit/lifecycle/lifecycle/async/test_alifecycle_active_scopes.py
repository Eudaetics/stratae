"""Test suite for verifying active_scopes method of the AsyncLifecycle class."""

from typing import Sequence

import pytest

from stratae.lifecycle import AsyncLifecycle
from stratae.lifecycle.exceptions import ScopeActivationError


def test_active_scopes_empty(async_lifecycle: AsyncLifecycle):
    """
    Test that active_scopes returns an empty list when no scopes are pushed.

    Given: An AsyncLifecycle instance with no pushed scopes
    When: active_scopes is called
    Then: An empty list should be returned
    """
    # Act (Arrange is done via fixture)
    active = async_lifecycle.active_scopes()

    # Assert
    assert active == []


def test_active_scopes_with_pushed_scopes(scopes: Sequence[str], async_lifecycle: AsyncLifecycle):
    """
    Test that active_scopes returns the correct list of pushed scopes.

    Given: An AsyncLifecycle instance with multiple pushed scopes
    When: active_scopes is called
    Then: A list of the pushed scopes should be returned in order
    """
    # Arrange
    async_lifecycle.push(scopes[0])
    async_lifecycle.push(scopes[1])

    # Act
    active = async_lifecycle.active_scopes()

    # Assert
    assert active == [scopes[0], scopes[1]]


async def test_active_scopes_after_pop(scopes: Sequence[str], async_lifecycle: AsyncLifecycle):
    """
    Test that active_scopes reflects the correct scopes after popping a scope.

    Given: An AsyncLifecycle instance with multiple pushed scopes
    When: A scope is popped and active_scopes is called
    Then: A list of the remaining pushed scopes should be returned in order
    """
    # Arrange
    t1 = async_lifecycle.push(scopes[0])
    t2 = async_lifecycle.push(scopes[1])
    t3 = async_lifecycle.push(scopes[2])

    # Act
    await async_lifecycle.pop(t3)
    active = async_lifecycle.active_scopes()

    # Assert
    assert active == [scopes[0], scopes[1]]
    await async_lifecycle.pop(t2)
    await async_lifecycle.pop(t1)


async def test_active_scopes_after_popping_all(
    scopes: Sequence[str], async_lifecycle: AsyncLifecycle
):
    """
    Test that active_scopes returns an empty list after popping all scopes.

    Given: An AsyncLifecycle instance with multiple pushed scopes
    When: All scopes are popped and active_scopes is called
    Then: An empty list should be returned
    """
    # Arrange
    t1 = async_lifecycle.push(scopes[0])
    t2 = async_lifecycle.push(scopes[1])

    # Act
    await async_lifecycle.pop(t2)
    await async_lifecycle.pop(t1)
    active = async_lifecycle.active_scopes()

    # Assert
    assert active == []


async def test_active_scopes_popped_out_of_order_raise(
    scopes: Sequence[str], async_lifecycle: AsyncLifecycle
):
    """
    AsyncLifecycle should enforce scope hierarchy.

    Given: An AsyncLifecycle instance with multiple pushed scopes.
    When: A scope is popped out of order.
    Then: A ScopeActivationError is raised.
    """
    # Arrange
    t1 = async_lifecycle.push(scopes[0])
    _ = async_lifecycle.push(scopes[1])

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="Cannot pop .* scope while .* is still active."):
        await async_lifecycle.pop(t1)
