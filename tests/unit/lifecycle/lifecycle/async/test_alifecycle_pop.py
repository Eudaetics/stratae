"""
Async tests for the AsyncLifecycle class's apop method.

This test suite verifies the behavior of popping lifecycle scopes from the stack, including popping
specific scopes, handling scopes not present in the stack, managing empty stacks, and raising errors
for invalid scopes.
"""

from typing import Sequence

import pytest

from stratae.lifecycle import AsyncLifecycle
from stratae.lifecycle.exceptions import ScopeActivationError


async def test_apop_most_recent_scope(async_lifecycle: AsyncLifecycle, scopes: Sequence[str]):
    """
    Test popping the most recent lifecycle scope from the stack.

    Given: An AsyncLifecycle instance with multiple pushed scopes
    When: The most recent scope is popped from the stack
    Then: The most recent scope should be removed from the lifecycle stack
    """
    # Arrange
    _ = async_lifecycle.push(scopes[0])
    token2 = async_lifecycle.push(scopes[1])

    # Act
    await async_lifecycle.pop(token2)

    # Assert
    assert async_lifecycle.active_scopes() == [scopes[0]]


async def test_apop_empty_stack(async_lifecycle: AsyncLifecycle, scopes: Sequence[str]):
    """
    Test popping from an empty lifecycle stack.

    Given: An AsyncLifecycle instance with no pushed scopes
    When: An attempt is made to pop a scope
    Then: the operation should not raise an exception and the stack should remain empty
    """
    # Arrange
    token = async_lifecycle.push(scopes[0])
    await async_lifecycle.pop(token)

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="Cannot pop .* while no scopes are active."):
        await async_lifecycle.pop(token)
