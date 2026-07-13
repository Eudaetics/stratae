"""
Unit tests for the Lifecycle class's pop functionality.

This test suite verifies the behavior of popping scope activations by the handle push()
returned - for shared scopes, the scope name - including error handling for popping a
scope that is not active.

Test Cases:
- test_pop_scope_by_handle: Ensure popping by push()'s handle removes that scope.
- test_pop_inactive_scope: Ensure popping a scope that is not active raises an error.
"""

from typing import Sequence

import pytest

from stratae.lifecycle import Lifecycle
from stratae.lifecycle.exceptions import ScopeActivationError, ScopeNotFoundError


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


def test_pop_invalid_scope(lifecycle: Lifecycle):
    """
    Attempting to pop an invalid scope raises.

    Given: A Lifecycle instance,
    When: A name is passed to pop that doesn't correspond to a scope for that lifecycle,
    Then: A ScopeNotFoundError is raised.
    """
    with pytest.raises(ScopeNotFoundError, match="Unknown scope: bad"):
        lifecycle.pop("bad")


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


def test_pop_context_by_name(lifecycle: Lifecycle):
    """
    PoppiQng an inactive context scope raises.

    Given: A Lifecycle instance,
    When: A name is passed for a scope that must be popped with a token,
    Then: A ScopeActivationError is raised.
    """
    with pytest.raises(ScopeActivationError, match="Cannot pop request by name"):
        lifecycle.pop("request")
