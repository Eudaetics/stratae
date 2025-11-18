"""Test suite for the MemoryCache class."""

from unittest.mock import AsyncMock, Mock

import pytest

from stratae.cache import Cache, MemoryCache


def test_protocol():
    """
    Test that MemoryCache adheres to the Cache protocol.

    Given: A new instance of MemoryCache.
    When: The instance is created.
    Then: It should be an instance of the Cache protocol.
    """
    # Arrange & Act
    cache = MemoryCache()

    # Assert
    assert isinstance(cache, Cache)


def test_initialization():
    """
    Test the initialization of the MemoryCache.

    Given: A new instance of MemoryCache.
    When: The instance is created.
    Then: It should have an empty cache dictionary.
    """
    # Arrange & Act
    cache = MemoryCache()

    # Assert
    assert isinstance(cache, MemoryCache)
    assert cache._cache == {}  # pyright: ignore[reportPrivateUsage]


def test_clear_cache():
    """
    Test the clear method of MemoryCache.

    Given: A MemoryCache instance with some items.
    When: The clear method is called.
    Then: The cache should be empty afterwards.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")
    cache.set("key2", "value2")

    # Act
    cache.clear()

    # Assert
    assert cache._cache == {}  # pyright: ignore[reportPrivateUsage]


def test_has_item() -> None:
    """
    Test the has method of MemoryCache.

    Given: An item in the cache.
    When: The has method is called with that item.
    Then: It should return True.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")

    # Act
    result = cache.has("key1")

    # Assert
    assert result


def test_has_item_false() -> None:
    """
    Test the has method of MemoryCache when the item is not in the cache.

    Given: An item not in the cache.
    When: The has method is called with that item.
    Then: It should return False.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")

    # Act
    result = cache.has("key2")

    # Assert
    assert not result


def test_get() -> None:
    """
    Test the get method of MemoryCache.

    Given: An item in the cache.
    When: The get method is called with that item.
    Then: It should return the cached value.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")

    # Act
    result = cache.get("key1")

    # Assert
    assert result == "value1"


def test_get_object() -> None:
    """
    Test the get method of MemoryCache with an object.

    Given: An object in the cache.
    When: The get method is called with that object.
    Then: It should return the cached object.
    """
    # Arrange
    cache = MemoryCache()

    class SimpleObject:
        def __init__(self, name: str):
            self.name = name

    obj = SimpleObject(name="test_object")
    cache.set("key1", obj)

    # Act
    result = cache.get("key1")

    # Assert
    assert result is obj


def test_get_default() -> None:
    """
    Test the get method of MemoryCache with a default value.

    Given: An item not in the cache.
    When: The get method is called with that item and a default value.
    Then: It should return the default value.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")

    # Act
    result = cache.get("key2", default="default_value")

    # Assert
    assert result == "default_value"


def test_get_key_error() -> None:
    """
    Test the get method of MemoryCache when the item is not cached.

    Given: An item not in the cache.
    When: The get method is called with that item.
    Then: It should raise a KeyError.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")

    # Act & Assert
    with pytest.raises(KeyError, match="'key2'"):
        cache.get("key2")


def test_get_none() -> None:
    """
    Test the get method of MemoryCache when the item is None.

    Given: An item with a None value in the cache.
    When: The get method is called with that item.
    Then: It should return None.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", None)

    # Act
    result = cache.get("key1")

    # Assert
    assert result is None


def test_get_or_set_found() -> None:
    """
    Test the get_or_set method of MemoryCache.

    Given: An item in the cache and a factory function.
    When: The get_or_set method is called with them.
    Then: It should return the cached value without calling the factory.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "cached_value")

    factory = Mock(return_value="value_from_factory")

    # Act
    result = cache.get_or_set("key1", factory)

    # Assert
    assert result == "cached_value"
    assert factory.call_count == 0


def test_get_or_set_not_found() -> None:
    """
    Test the get_or_set method of MemoryCache.

    Given: An item not in the cache and a factory function.
    When: The get_or_set method is called with them.
    Then: It should return the value produced by the factory and store it in the cache.
    """
    # Arrange
    cache = MemoryCache()

    factory = Mock(return_value="value_from_factory")

    # Act
    result = cache.get_or_set("key1", factory)

    # Assert
    assert result == "value_from_factory"
    assert cache.get("key1") == "value_from_factory"
    assert factory.call_count == 1


def test_get_or_set_with_none() -> None:
    """
    Test the get_or_set method of MemoryCache when the factory returns None.

    Given: An item not in the cache and a factory function that returns None.
    When: The get_or_set method is called with them.
    Then: It should return None and store None in the cache.
    """
    # Arrange
    cache = MemoryCache()

    factory = Mock(return_value=None)

    # Act
    result = cache.get_or_set("key1", factory)

    # Assert
    assert result is None
    assert cache.get("key1") is None
    assert factory.call_count == 1


def test_get_or_set_exception_in_factory() -> None:
    """
    Test the get_or_set method of MemoryCache when the factory raises an exception.

    Given: An item not in the cache and a factory function that raises an exception.
    When: The get_or_set method is called with them.
    Then: It should propagate the exception and not store anything in the cache.
    """
    # Arrange
    cache = MemoryCache()

    mock = Mock(side_effect=ValueError("Factory error"))

    # Act & Assert
    with pytest.raises(ValueError, match="Factory error"):
        cache.get_or_set("key1", mock)

    assert not cache.has("key1")


async def test_aget_or_set_found() -> None:
    """
    Test the aget_or_set method of MemoryCache.

    Given: An item in the cache and an async factory function.
    When: The aget_or_set method is called with them.
    Then: It should return the cached value without calling the factory.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "cached_value")

    factory = AsyncMock(return_value="value_from_factory")

    # Act
    result = await cache.aget_or_set("key1", factory)

    # Assert
    assert result == "cached_value"
    assert factory.call_count == 0


async def test_aget_or_set_not_found() -> None:
    """
    Test the aget_or_set method of MemoryCache.

    Given: An item not in the cache and an async factory function.
    When: The aget_or_set method is called with them.
    Then: It should return the value produced by the factory and store it in the cache.
    """
    # Arrange
    cache = MemoryCache()

    factory = AsyncMock(return_value="value_from_factory")

    # Act
    result = await cache.aget_or_set("key1", factory)

    # Assert
    assert result == "value_from_factory"
    assert cache.get("key1") == "value_from_factory"
    assert factory.call_count == 1


async def test_aget_or_set_with_none() -> None:
    """
    Test the aget_or_set method of MemoryCache when the factory returns None.

    Given: An item not in the cache and an async factory function that returns None.
    When: The aget_or_set method is called with them.
    Then: It should return None and store None in the cache.
    """
    # Arrange
    cache = MemoryCache()

    factory = AsyncMock(return_value=None)

    # Act
    result = await cache.aget_or_set("key1", factory)

    # Assert
    assert result is None
    assert cache.get("key1") is None
    assert factory.call_count == 1


async def test_aget_or_set_exception_in_factory() -> None:
    """
    Test the aget_or_set method of MemoryCache when the factory raises an exception.

    Given: An item not in the cache and an async factory function that raises an exception.
    When: The aget_or_set method is called with them.
    Then: It should propagate the exception and not store anything in the cache.
    """
    # Arrange
    cache = MemoryCache()

    mock = AsyncMock(side_effect=ValueError("Factory error"))

    # Act & Assert
    with pytest.raises(ValueError, match="Factory error"):
        await cache.aget_or_set("key1", mock)

    assert not cache.has("key1")


def test_set() -> None:
    """
    Test the set method of MemoryCache.

    Given: An item and its value.
    When: The set method is called with them.
    Then: The item should be added to the cache.
    """
    # Arrange
    cache = MemoryCache()

    # Act
    cache.set("key1", "value1")

    # Assert
    assert cache.has("key1")
    assert cache.get("key1") == "value1"


def test_set_overwrite() -> None:
    """
    Test the set method of MemoryCache when overwriting an existing item.

    Given: A key is already in the cache.
    When: The set method is called with that key and a new value.
    Then: The cached value should be updated.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")

    # Act
    cache.set("key1", "new_value")

    # Assert
    assert cache.get("key1") == "new_value"


def test_unset() -> None:
    """
    Test the unset method of MemoryCache.

    Given: An item in the cache.
    When: The unset method is called with that item.
    Then: The item should be removed from the cache.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")

    # Act
    cache.unset("key1")

    # Assert
    assert not cache.has("key1")


def test_unset_key_error() -> None:
    """
    Test the unset method of MemoryCache when the item is not cached.

    Given: An item not in the cache.
    When: The unset method is called with that item.
    Then: It should raise a KeyError.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")

    # Act & Assert
    with pytest.raises(KeyError, match="'key2'"):
        cache.unset("key2")


def test_is_empty() -> None:
    """
    Test the is_empty method of MemoryCache.

    Given: A MemoryCache instance.
    When: The is_empty method is called.
    Then: It should return True if the cache is empty
    """
    # Arrange
    cache = MemoryCache()

    # Act & Assert
    assert cache.is_empty()


def test_is_not_empty() -> None:
    """
    Test the is_empty method of MemoryCache when the cache has items.

    Given: A MemoryCache instance with items.
    When: The is_empty method is called.
    Then: It should return False.
    """
    # Arrange
    cache = MemoryCache()
    cache.set("key1", "value1")

    # Act & Assert
    assert not cache.is_empty()
