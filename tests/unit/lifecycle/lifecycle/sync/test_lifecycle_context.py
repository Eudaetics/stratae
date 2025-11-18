"""
Test suite for the Lifecycle context management functionality.

This module contains tests for the `Lifecycle` class, specifically its behavior
when used as a context manager with various lifecycle scopes. The tests verify
correct stack management, nested scope handling, and error handling for invalid scopes.

Tests:
- test_context_lifecycle: Ensures the lifecycle stack is managed correctly with a single scope.
- test_context_lifecycle_nested_scopes: Verifies the stack reflects nested scopes when entering
                                        multiple contexts.
- test_context_lifecycle_invalid_scope: Checks that an AttributeError is raised when attempting to
                                        use an invalid scope.

"""

from typing import Sequence

import pytest

from stratae.lifecycle import Lifecycle
from stratae.lifecycle.exceptions import ScopeNotFoundError


def test_context_lifecycle(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test using Lifecycle as a context lifecycle.

    Given: A Lifecycle instance and a lifecycle scope
    When: The context lifecycle is entered and exited
    Then: The lifecycle stack should be managed correctly
    """
    # Act
    with lifecycle.start("application"):
        # Assert
        assert lifecycle.active_scopes() == [scopes[0]]
    assert lifecycle.is_empty()


def test_async_exit_stack_not_active(lifecycle: Lifecycle):
    """
    Verify calling get_exit_stack when not active raises an error.

    Given: A Lifecycle instance
    When: get_exit_stack is called outside of an active context
    Then: A RuntimeError is raised indicating the exit stack is not active
    """
    with pytest.raises(RuntimeError, match="Scope 'application' is not active."):
        lifecycle.get_exit_stack("application")


def test_async_exit_stack_active(lifecycle: Lifecycle):
    """
    Verify calling get_exit_stack when active returns the exit stack.

    Given: A Lifecycle instance
    When: get_exit_stack is called within an active context
    Then: The exit stack for the scope is returned
    """
    with lifecycle.start("application"):
        stack = lifecycle.get_exit_stack("application")
        assert stack is not None


def test_async_exit_stack_invalid_scope(lifecycle: Lifecycle):
    """
    Verify calling get_exit_stack with an invalid scope raises an error.

    Given: A Lifecycle instance
    When: get_exit_stack is called with an invalid scope
    Then: A ValueError is raised indicating the scope is unknown
    """
    with pytest.raises(ValueError, match="Unknown scope: invalid"):
        lifecycle.get_exit_stack("invalid")


def test_context_lifecycle_nested_scopes(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test using Lifecycle with nested scopes in a context lifecycle.

    Given: A Lifecycle instance and multiple lifecycle scopes
    When: The context lifecycle is entered with nested scopes
    Then: The lifecycle stack should reflect the nested structure
    """
    # Act
    with lifecycle.start("application"):
        # Assert
        assert lifecycle.active_scopes() == [scopes[0]]
        with lifecycle.start("session"):
            assert lifecycle.active_scopes() == [scopes[0], scopes[1]]
    assert lifecycle.is_empty()


def test_context_lifecycle_invalid_scope(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test using Lifecycle with an invalid scope in a context lifecycle.

    Given: A Lifecycle instance and an invalid lifecycle scope
    When: The context lifecycle is entered with an invalid scope
    Then: An AttributeError should be raised indicating the invalid scope
    """
    # Act & Assert
    with pytest.raises(ScopeNotFoundError, match="No lifecycle scope named 'invalid'."):
        with lifecycle.start("invalid"):
            ...
