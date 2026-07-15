"""
Test suite for asynchronous context management in the AsyncLifecycle class.

This module covers only the behavior specific to AsyncLifecycleContext's async dunder
methods (__aenter__/__aexit__). Shared dispatch/error-path behavior (invalid scope,
inactive-scope get_exit_stack) is covered once, via Lifecycle, in
tests/unit/lifecycle/lifecycle/sync/test_lifecycle_context.py - it runs through
BaseLifecycle code common to both classes.

Tests:
- test_async_context_lifecycle: Ensures the lifecycle stack is managed correctly when entering and
                                exiting a single scope.
- test_async_context_lifecycle_nested_scopes: Verifies that nested scopes are reflected accurately
                                              in the lifecycle stack.
"""

from typing import Sequence

from stratae.lifecycle.lifecycle import AsyncLifecycle


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
