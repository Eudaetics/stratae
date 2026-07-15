"""
Test suite for BaseLifecycle.start/get_exit_stack error paths, exercised via Lifecycle.

These paths (invalid scope, inactive-scope get_exit_stack) are inherited unchanged by
AsyncLifecycle - they raise before any context manager class is even instantiated, so
there is no separate async test module for this.

Actually entering/exiting a scope's context manager exercises LifecycleContext vs
AsyncLifecycleContext, which are separate classes with separate __enter__/__exit__ vs
__aenter__/__aexit__ implementations - that coverage stays split, in
tests/unit/lifecycle/lifecycle/sync/test_lifecycle_context.py and
tests/unit/lifecycle/lifecycle/async/test_alifecycle_context.py.
"""

import pytest

from stratae.lifecycle import Lifecycle
from stratae.lifecycle.exceptions import ScopeNotFoundError


def test_exit_stack_not_active(lifecycle: Lifecycle):
    """
    Verify calling get_exit_stack when not active raises an error.

    Given: A Lifecycle instance
    When: get_exit_stack is called outside of an active context
    Then: A RuntimeError is raised indicating the exit stack is not active
    """
    with pytest.raises(RuntimeError, match="Scope 'application' is not active."):
        lifecycle.get_exit_stack("application")


def test_exit_stack_active(lifecycle: Lifecycle):
    """
    Verify calling get_exit_stack when active returns the exit stack.

    Given: A Lifecycle instance
    When: get_exit_stack is called within an active context
    Then: The exit stack for the scope is returned
    """
    with lifecycle.start("application"):
        stack = lifecycle.get_exit_stack("application")
        assert stack is not None


def test_exit_stack_invalid_scope(lifecycle: Lifecycle):
    """
    Verify calling get_exit_stack with an invalid scope raises an error.

    Given: A Lifecycle instance
    When: get_exit_stack is called with an invalid scope
    Then: A ValueError is raised indicating the scope is unknown
    """
    with pytest.raises(ValueError, match="Unknown scope: invalid"):
        lifecycle.get_exit_stack("invalid")


def test_context_lifecycle_invalid_scope(lifecycle: Lifecycle):
    """
    Test using Lifecycle with an invalid scope in a context lifecycle.

    Given: A Lifecycle instance and an invalid lifecycle scope
    When: The context lifecycle is entered with an invalid scope
    Then: An AttributeError should be raised indicating the invalid scope
    """
    # Act & Assert
    with pytest.raises(ScopeNotFoundError, match="Unknown scope: invalid"):
        with lifecycle.start("invalid"):
            ...
