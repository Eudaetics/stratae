"""
Unit tests for the Lifecycle class's pop functionality.

This test suite verifies the behavior of popping scope activations by the token push()
returned, including error handling for popping a scope that is not active.

Test Cases:
- test_pop_scope_by_handle: Ensure popping by push()'s token removes that scope.
- test_pop_inactive_scope: Ensure popping a scope that is not active raises an error.
"""

from typing import Sequence
from unittest.mock import Mock

import pytest

from stratae.lifecycle import Lifecycle, resource
from stratae.lifecycle.exceptions import ScopeActivationError


def test_pop_scope_by_handle(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test popping a lifecycle scope activation by the handle push() returned.

    Given: A Lifecycle instance with multiple pushed scopes
    When: The most recent scope is popped by its handle
    Then: That scope should be removed from the lifecycle stack
    """
    # Arrange
    _ = lifecycle.push(scopes[0])
    handle = lifecycle.push(scopes[1])

    # Act
    lifecycle.pop(handle)

    # Assert
    assert lifecycle.active_scopes() == [scopes[0]]


def test_pop_inactive_scope(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test popping a scope that is not active.

    Given: A Lifecycle instance whose scope has already been popped
    When: An attempt is made to pop it again
    Then: A ScopeActivationError should be raised and the stack should remain empty
    """
    # Arrange
    handle = lifecycle.push(scopes[0])
    lifecycle.pop(handle)

    # Act & Assert
    with pytest.raises(ScopeActivationError, match=f"Cannot pop {scopes[0]}"):
        lifecycle.pop(handle)
    assert lifecycle.is_empty()


def test_pop_context_scope(lifecycle: Lifecycle):
    """
    Popping an inactive context scope raises.

    Given: A Lifecycle instance,
    When: A token is passed for a scope already popped,
    Then: A ScopeActivationError is raised.
    """
    # Arrange
    t0 = lifecycle.push("request")
    lifecycle.pop(t0)

    # Act & Assert
    with pytest.raises(ScopeActivationError, match="Cannot pop request: scope is not active."):
        lifecycle.pop(t0)


@pytest.mark.parametrize("scope", ("application", "request"))
def test_pop_with_used_resource(lifecycle: Lifecycle, scope: str):
    """
    Popping a context manager with an exit stack cleans up properly.

    Given: A Lifecycle instance with a registered resource for a scope,
    When: The scope is popped,
    Then: The exit stack should clean up the resource.
    """
    # Arrange
    mock = Mock()

    @lifecycle.cache(scope)
    @resource
    def test_resource():
        try:
            yield
        finally:
            mock()

    token = lifecycle.push(scope)
    test_resource()

    # Act
    lifecycle.pop(token)

    # Assert
    mock.assert_called_once()
