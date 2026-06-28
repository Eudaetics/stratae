"""Validation of Scope construction."""

from stratae.cache import MemoryCache
from stratae.lifecycle import Scope


def test_scope_init():
    """
    Verify that Scope can be constructed with positional args and no cache.

    Given: a name and isolation level,
    When: Scope is constructed positionally without a cache,
    Then: it should store the name and isolation, and default to a MemoryCache.
    """
    # Arrange
    name = "request"
    isolation = "none"

    # Act
    scope = Scope(name, isolation)

    # Assert
    assert scope.name == name
    assert scope.isolation == isolation
    assert isinstance(scope.cache, MemoryCache)


def test_scope_init_custom_cache():
    """
    Verify that Scope stores a custom cache when one is provided.

    Given: a name, isolation level, and a custom cache instance,
    When: Scope is constructed positionally with that cache,
    Then: it should store the same cache instance rather than creating a default one.
    """
    # Arrange
    name = "request"
    isolation = "none"
    cache = MemoryCache()

    # Act
    scope = Scope(name, isolation, cache)

    # Assert
    assert scope.name == name
    assert scope.isolation == isolation
    assert scope.cache is cache


def test_scope_keyword_init():
    """
    Verify that Scope fields can be assigned via keyword arguments.

    Given: a name, isolation level, and cache instance,
    When: Scope is constructed using keyword arguments,
    Then: it should store each field correctly.
    """
    # Arrange
    name = "request"
    isolation = "context"
    cache = MemoryCache()

    # Act
    scope = Scope(name=name, isolation=isolation, cache=cache)

    # Assert
    assert scope.name == name
    assert scope.isolation == isolation
    assert scope.cache is cache
