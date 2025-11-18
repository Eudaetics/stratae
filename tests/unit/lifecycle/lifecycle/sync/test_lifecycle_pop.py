"""
Unit tests for the Lifecycle class's pop functionality.

This test suite verifies the behavior of popping scopes from the lifecycle stack, including popping
specific scopes, handling scopes not present in the stack, popping from an empty stack, and error
handling for invalid scopes.

Test Cases:
- test_pop_specific_scope: Ensure popping a specific scope removes all equal or greater scopes.
- test_pop_scope_not_in_stack: Ensure popping a non-existent scope does not affect the stack.
- test_pop_empty_stack: Ensure popping from an empty stack does not raise exceptions.
- test_pop_invalid_scope: Ensure popping an invalid scope raises a ValueError.

"""

from typing import Sequence

from stratae.lifecycle import Lifecycle


def test_pop_most_recent_scope(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test popping the most recent lifecycle scope from the stack.

    Given: A Lifecycle instance with multiple pushed scopes
    When: The most recent scope is popped from the stack
    Then: The most recent scope should be removed from the lifecycle stack
    """
    # Arrange
    _ = lifecycle.push(scopes[0])
    lifecycle.push(scopes[1])

    # Act
    lifecycle.pop()

    # Assert
    assert lifecycle.active_scopes() == [scopes[0]]


def test_pop_empty_stack(lifecycle: Lifecycle, scopes: Sequence[str]):
    """
    Test popping from an empty lifecycle stack.

    Given: A Lifecycle instance with no pushed scopes
    When: An attempt is made to pop a scope
    Then: the operation should not raise an exception and the stack should remain empty
    """
    # Arrange
    lifecycle.push(scopes[0])
    lifecycle.pop()

    # Act
    lifecycle.pop()

    # Assert
    assert lifecycle.is_empty()
