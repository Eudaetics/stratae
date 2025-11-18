"""In-memory cache implementation."""

from typing import Any, Awaitable, Callable, Hashable


class MemoryCache:
    """A simple in-memory cache."""

    _missing = object()

    def __init__(self):
        """Initialize the memory cache."""
        self._cache: dict[Hashable, Any] = {}

    def has(self, key: Hashable) -> bool:
        """Check if an item exists in the cache."""
        return key in self._cache

    def get(self, key: Hashable, default: Any = _missing) -> Any:
        """Retrieve an item from the cache."""
        if key in self._cache:
            return self._cache.get(key)
        if default is not self._missing:
            return default
        raise KeyError(key)

    def get_or_set[T](self, key: Hashable, factory: Callable[[], T]) -> T:
        """Get an item from the cache or set it using a factory function."""
        if key in self._cache:
            return self._cache[key]
        value = factory()
        self.set(key, value)
        return value

    async def aget_or_set[T](self, key: Hashable, factory: Callable[[], Awaitable[T]]) -> T:
        """Asynchronously get an item from the cache or set it using a factory function."""
        if key in self._cache:
            return self._cache[key]
        value = await factory()
        self.set(key, value)
        return value

    def set(self, key: Hashable, value: Any) -> None:
        """Store an item in the cache."""
        self._cache[key] = value

    def unset(self, key: Hashable) -> None:
        """Unset a cached item."""
        if key in self._cache:
            del self._cache[key]
        else:
            raise KeyError(key)

    def clear(self) -> None:
        """Clear the cache."""
        self._cache.clear()

    async def aclear(self) -> None:
        """Asynchronously clear the cache."""
        self.clear()

    def is_empty(self) -> bool:
        """Check if the cache is empty."""
        return not self._cache
