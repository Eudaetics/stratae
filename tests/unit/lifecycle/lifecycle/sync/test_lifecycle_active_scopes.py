"""Test suite for verifying active_scopes method of the Lifecycle class."""

from typing import Sequence

from stratae.lifecycle import Lifecycle


def test_active_scopes_empty(lifecycle: Lifecycle):
    """
    Test that active_scopes returns an empty list when no scopes are pushed.

    Given: A Lifecycle instance with no pushed scopes
    When: active_scopes is called
    Then: An empty list should be returned
    """
    # Act (Arrange is done via fixture)
    active = lifecycle.active_scopes()

    # Assert
    assert active == []


def test_active_scopes_with_pushed_scopes(scopes: Sequence[str], lifecycle: Lifecycle):
    """
    Test that active_scopes returns the correct list of pushed scopes.

    Given: A Lifecycle instance with multiple pushed scopes
    When: active_scopes is called
    Then: A list of the pushed scopes should be returned in order
    """
    # Arrange
    lifecycle.push(scopes[0])
    lifecycle.push(scopes[1])

    # Act
    active = lifecycle.active_scopes()

    # Assert
    assert active == [scopes[0], scopes[1]]


def test_active_scopes_after_pop(scopes: Sequence[str], lifecycle: Lifecycle):
    """
    Test that active_scopes reflects the correct scopes after popping a scope.

    Given: A Lifecycle instance with multiple pushed scopes
    When: A scope is popped and active_scopes is called
    Then: A list of the remaining pushed scopes should be returned in order
    """
    # Arrange
    lifecycle.push(scopes[0])
    lifecycle.push(scopes[1])
    t2 = lifecycle.push(scopes[2])

    # Act
    lifecycle.pop(t2)
    active = lifecycle.active_scopes()

    # Assert
    assert active == [scopes[0], scopes[1]]


def test_active_scopes_after_popping_all(scopes: Sequence[str], lifecycle: Lifecycle):
    """
    Test that active_scopes returns an empty list after popping all scopes.

    Given: A Lifecycle instance with multiple pushed scopes
    When: All scopes are popped and active_scopes is called
    Then: An empty list should be returned
    """
    # Arrange
    t0 = lifecycle.push(scopes[0])
    t1 = lifecycle.push(scopes[1])

    # Act
    lifecycle.pop(t1)
    lifecycle.pop(t0)
    active = lifecycle.active_scopes()

    # Assert
    assert active == []
