"""Test suite for verifying is_empty method of the Lifecycle class."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from stratae.lifecycle import Lifecycle


def test_is_empty(lifecycle: Lifecycle):
    """
    Test that is_empty returns True for an empty lifecycle stack.

    Given: A Lifecycle instance with no pushed scopes
    When: is_empty is called
    Then: True should be returned
    """
    assert lifecycle.is_empty()


def test_is_not_empty_after_push(scopes: Sequence[str], lifecycle: Lifecycle):
    """
    Test that is_empty returns False after pushing a scope.

    Given: A Lifecycle instance
    When: A scope is pushed onto the lifecycle stack
    Then: is_empty should return False
    """
    # Arrange
    lifecycle.push(scopes[0])

    # Act & Assert
    assert not lifecycle.is_empty()


def test_is_not_empty_after_pop(scopes: Sequence[str], lifecycle: Lifecycle):
    """
    Test that is_empty returns False if there are still scopes after popping one.

    Given: A Lifecycle instance with multiple pushed scopes
    When: One scope is popped from the lifecycle stack
    Then: is_empty should return False
    """
    # Arrange
    lifecycle.push(scopes[0])
    lifecycle.push(scopes[1])
    lifecycle.pop()

    # Act & Assert
    assert not lifecycle.is_empty()


def test_is_empty_after_popping_all(scopes: Sequence[str], lifecycle: Lifecycle):
    """
    Test that is_empty returns True after popping all pushed scopes.

    Given: A Lifecycle instance with pushed scopes
    When: All scopes are popped from the lifecycle stack
    Then: is_empty should return True
    """
    # Arrange
    lifecycle.push(scopes[0])
    lifecycle.push(scopes[1])
    lifecycle.pop()
    lifecycle.pop()

    # Act & Assert
    assert lifecycle.is_empty()
