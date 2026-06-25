"""
Unit tests for initialization and configuration for the `Lifecycle` class.

This test suite verifies the following behaviors:
- Proper initialization of the Lifecycle stack.
- Enforcement of unique and valid scope names.
- Error handling for duplicate, missing, or invalid scopes.
- Support for custom cache implementations per scope.
- Error handling for cache overrides with invalid scopes.
- Attribute and cache access for valid and invalid scopes.
"""

from typing import Sequence

import pytest

from stratae.cache.memory import MemoryCache
from stratae.lifecycle import Lifecycle


def test_initialization(lifecycle: Lifecycle):
    """
    Test that Lifecycle initializes with an empty lifecycle stack.

    Given: A new instance of Lifecycle
    When: The instance is created
    Then: The lifecycle stack should be empty
    """
    assert lifecycle.is_empty()


def test_initialization_with_duplicate_scopes():
    """
    Test that initializing Lifecycle with duplicate scopes raises an error.

    Given: A list of lifecycle scopes with duplicates
    When: An attempt is made to create a Lifecycle instance
    Then: A ValueError should be raised
    """
    # Arrange
    scopes = ["application", "request", "session", "request"]

    # Act & Assert
    with pytest.raises(ValueError, match="All scopes must be unique."):
        Lifecycle(scopes)


def test_initialization_with_no_scopes():
    """
    Test that initializing Lifecycle with no scopes raises an error.

    Given: An empty list of lifecycle scopes
    When: An attempt is made to create a Lifecycle instance
    Then: A ValueError should be raised
    """
    # Arrange
    scopes: list[str] = []

    # Act & Assert
    with pytest.raises(ValueError, match="At least one scope must be defined."):
        Lifecycle(scopes)


@pytest.mark.parametrize("invalid_scope", ["app-1", "request scope", "123scope", "scope!"])
def test_initialization_with_non_identifier_scopes(invalid_scope: str):
    """
    Test that initializing Lifecycle with non-identifier scopes raises an error.

    Given: A list of lifecycle scopes with a non-identifier entry
    When: An attempt is made to create a Lifecycle instance
    Then: A ValueError should be raised
    """
    # Arrange
    scopes = ["application", invalid_scope]

    # Act & Assert
    with pytest.raises(ValueError, match="All scopes must be valid Python identifiers."):
        Lifecycle(scopes)


def test_cache_override_for_scope(scopes: Sequence[str]):
    """
    Test that alternative cache implementations can be used for lifecycle scopes.

    Given: A Lifecycle instance with a custom cache for a scope
    When: The instance is created
    Then: The custom cache should be used for that scope
    """

    # Arrange
    class _TestCache(MemoryCache): ...

    custom_cache = _TestCache

    # Act
    lifecycle = Lifecycle(
        scopes,
        caches={scopes[2]: custom_cache},
    )

    # Assert
    with lifecycle.start("request"):
        assert isinstance(lifecycle.get_cache(scopes[2]), custom_cache)


def test_cache_override_for_wrong_scope(scopes: Sequence[str]):
    """
    Test that providing a cache override for an invalid scope raises an error.

    Given: A Lifecycle instance with a cache override for an invalid scope
    When: The instance is created
    Then: A ValueError should be raised
    """
    # Act & Assert
    with pytest.raises(ValueError, match="All caches must correspond to defined scopes."):
        Lifecycle(
            scopes,
            caches={"bad": MemoryCache},
        )


def test_get_cache_invalid_scope(lifecycle: Lifecycle):
    """
    Test that getting a cache for an invalid scope raises an error.

    Given: A Lifecycle instance
    When: An attempt is made to get a cache for an invalid scope
    Then: A ValueError should be raised
    """
    # Act & Assert
    with pytest.raises(ValueError, match="Unknown scope: bad"):
        lifecycle.get_cache("bad")
