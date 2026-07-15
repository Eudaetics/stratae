"""
Test suite for the Lifecycle context management functionality.

This module covers only the behavior specific to LifecycleContext's sync dunder methods
(__enter__/__exit__). Shared dispatch/error-path behavior (invalid scope, inactive-scope
get_exit_stack) is covered once, via Lifecycle, in
tests/unit/lifecycle/lifecycle/base/test_context.py - it runs through BaseLifecycle code
common to both classes.

Tests:
- test_context_lifecycle: Ensures the lifecycle stack is managed correctly with a single scope.
- test_context_lifecycle_nested_scopes: Verifies the stack reflects nested scopes when entering
                                        multiple contexts.
"""

from typing import Sequence

from stratae.lifecycle import Lifecycle


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
