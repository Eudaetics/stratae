"""
Test suite for asynchronous context management in the AsyncLifecycle class.

This module contains tests that verify the correct behavior of the AsyncLifecycle class
when used as an asynchronous context manager. The tests cover scenarios including
single and nested scope management, as well as error handling for invalid scopes.

Tests:
- test_async_context_lifecycle: Ensures the lifecycle stack is managed correctly when entering and
                                exiting a single scope.
- test_async_context_lifecycle_nested_scopes: Verifies that nested scopes are reflected accurately
                                              in the lifecycle stack.
- test_async_context_lifecycle_invalid_scope: Checks that an AttributeError is raised when
                                            attempting to use an invalid scope.
"""

from typing import Sequence

import pytest

from stratae.lifecycle import AsyncLifecycle
from stratae.lifecycle.exceptions import ScopeNotFoundError


async def test_async_context_lifecycle(async_lifecycle: AsyncLifecycle, scopes: Sequence[str]):
    """
    Test using AsyncLifecycle as a context lifecycle.

    Given: An AsyncLifecycle instance and a lifecycle scope
    When: The context lifecycle is entered and exited
    Then: The lifecycle stack should be managed correctly
    """
    # Act
    async with async_lifecycle.start("application"):
        # Assert
        assert async_lifecycle.active_scopes() == [scopes[0]]
    assert async_lifecycle.active_scopes() == []


def test_async_exit_stack_not_active(async_lifecycle: AsyncLifecycle):
    """
    Verify calling get_exit_stack when not active raises an error.

    Given: An AsyncLifecycle instance
    When: get_exit_stack is called outside of an active context
    Then: A RuntimeError is raised indicating the exit stack is not active
    """
    with pytest.raises(RuntimeError, match="Scope 'application' is not active."):
        async_lifecycle.get_exit_stack("application")


async def test_async_exit_stack_active(async_lifecycle: AsyncLifecycle):
    """
    Verify calling get_exit_stack when active returns the exit stack.

    Given: An AsyncLifecycle instance
    When: get_exit_stack is called within an active context
    Then: The exit stack for the scope is returned
    """
    async with async_lifecycle.start("application"):
        stack = async_lifecycle.get_exit_stack("application")
        assert stack is not None


def test_async_exit_stack_invalid_scope(async_lifecycle: AsyncLifecycle):
    """
    Verify calling get_exit_stack with an invalid scope raises an error.

    Given: An AsyncLifecycle instance
    When: get_exit_stack is called with an invalid scope
    Then: A ValueError is raised indicating the scope is unknown
    """
    with pytest.raises(ValueError, match="Unknown scope: invalid"):
        async_lifecycle.get_exit_stack("invalid")


async def test_async_context_lifecycle_nested_scopes(
    async_lifecycle: AsyncLifecycle, scopes: Sequence[str]
):
    """
    Test using AsyncLifecycle with nested scopes in a context lifecycle.

    Given: An AsyncLifecycle instance and multiple lifecycle scopes
    When: The context lifecycle is entered with nested scopes
    Then: The lifecycle stack should reflect the nested structure
    """
    # Act
    async with async_lifecycle.start("application"):
        # Assert
        assert async_lifecycle.active_scopes() == [scopes[0]]
        async with async_lifecycle.start("session"):
            assert async_lifecycle.active_scopes() == [scopes[0], scopes[1]]
    assert async_lifecycle.active_scopes() == []


async def test_async_context_lifecycle_invalid_scope(
    async_lifecycle: AsyncLifecycle,
):
    """
    Test using AsyncLifecycle with an invalid scope in a context lifecycle.

    Given: An AsyncLifecycle instance and an invalid lifecycle scope
    When: The context lifecycle is entered with an invalid scope
    Then: An AttributeError should be raised indicating the invalid scope
    """
    # Act & Assert
    with pytest.raises(ScopeNotFoundError, match="No lifecycle scope named 'invalid'."):
        async with async_lifecycle.start("invalid"):
            ...
