"""
Async tests for the AsyncLifecycle class's apop method.

This test suite verifies the behavior of popping lifecycle scopes from the stack, including popping
specific scopes, handling scopes not present in the stack, managing empty stacks, and raising errors
for invalid scopes.
"""

from typing import Sequence
from unittest.mock import Mock

import pytest

from stratae.lifecycle import AsyncLifecycle, resource
from stratae.lifecycle.exceptions import ScopeActivationError, ScopeNotFoundError


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
    with pytest.raises(ScopeActivationError, match="Cannot pop .*: scope is not active."):
        await async_lifecycle.pop(token)


async def test_pop_invalid_scope(async_lifecycle: AsyncLifecycle):
    """
    Attempting to pop an invalid scope raises.

    Given: An AsyncLifecycle instance,
    When: A name is passed to pop that doesn't correspond to a scope for that lifecycle,
    Then: A ScopeNotFoundError is raised.
    """
    with pytest.raises(ScopeNotFoundError, match="Unknown scope: bad"):
        await async_lifecycle.pop("bad")


async def test_pop_inactive_context_scope(async_lifecycle: AsyncLifecycle):
    """
    Popping an inactive context scope raises.

    Given: An AsyncLifecycle instance,
    When: A token is passed for a scope already popped,
    Then: A ScopeActivationError is raised.
    """
    # Arrange
    t0 = async_lifecycle.push("request")
    await async_lifecycle.pop(t0)

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="Cannot pop request: scope is not active."):
        await async_lifecycle.pop(t0)


async def test_pop_context_by_name_raises(async_lifecycle: AsyncLifecycle):
    """
    Popping an inactive context scope raises.

    Given: An AsyncLifecycle instance,
    When: A name is passed for a scope that must be popped with a token,
    Then: A ScopeActivationError is raised.
    """
    with pytest.raises(ScopeActivationError, match="Cannot pop request by name"):
        await async_lifecycle.pop("request")


@pytest.mark.parametrize("scope", ("application", "request"))
async def test_pop_with_used_resource(async_lifecycle: AsyncLifecycle, scope: str):
    """
    Popping a context manager with an exit stack cleans up properly.

    Given: An AsyncLifecycle instance with a registered resource for a scope,
    When: The scope is popped,
    Then: The exit stack should clean up the resource.
    """
    # Arrange
    mock = Mock()

    @async_lifecycle.cache(scope)
    @resource
    def test_resource():
        try:
            yield
        finally:
            mock()

    token = async_lifecycle.push(scope)
    test_resource()

    # Act
    await async_lifecycle.pop(token)

    # Assert
    mock.assert_called_once()
