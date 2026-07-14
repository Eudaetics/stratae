"""Test suite for verifying is_empty method of the AsyncLifecycle class."""

from typing import Sequence

from stratae.lifecycle.lifecycle import AsyncLifecycle


def test_is_empty(async_lifecycle: AsyncLifecycle):
    """
    Test that is_empty returns True for an empty async lifecycle stack.

    Given: A AsyncLifecycle instance with no pushed scopes
    When: is_empty is called
    Then: True should be returned
    """
    assert async_lifecycle.is_empty()


def test_is_not_empty_after_push(scopes: Sequence[str], async_lifecycle: AsyncLifecycle):
    """
    Test that is_empty returns False after pushing a scope.

    Given: A AsyncLifecycle instance
    When: A scope is pushed onto the async_lifecycle stack
    Then: is_empty should return False
    """
    # Arrange
    async_lifecycle.push(scopes[0])

    # Act & Assert
    assert not async_lifecycle.is_empty()


async def test_is_not_empty_after_pop(scopes: Sequence[str], async_lifecycle: AsyncLifecycle):
    """
    Test that is_empty returns False if there are still scopes after popping one.

    Given: A AsyncLifecycle instance with multiple pushed scopes
    When: One scope is popped from the async_lifecycle stack
    Then: is_empty should return False
    """
    # Arrange
    token1 = async_lifecycle.push(scopes[0])
    token2 = async_lifecycle.push(scopes[1])
    await async_lifecycle.pop(token2)

    # Act & Assert
    assert not async_lifecycle.is_empty()
    await async_lifecycle.pop(token1)


async def test_is_empty_after_popping_all(scopes: Sequence[str], async_lifecycle: AsyncLifecycle):
    """
    Test that is_empty returns True after popping all pushed scopes.

    Given: A AsyncLifecycle instance with pushed scopes
    When: All scopes are popped from the async_lifecycle stack
    Then: is_empty should return True
    """
    # Arrange
    token1 = async_lifecycle.push(scopes[0])
    token2 = async_lifecycle.push(scopes[1])
    await async_lifecycle.pop(token2)
    await async_lifecycle.pop(token1)

    # Act & Assert
    assert async_lifecycle.is_empty()
