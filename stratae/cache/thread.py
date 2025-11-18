"""Thread-Safe In-memory cache implementation."""

import threading
from typing import Any, Awaitable, Callable, Hashable

from stratae.cache import MemoryCache


class ThreadSafeMemoryCache:
    """A thread-safe in-memory cache."""

    _missing = object()

    def __init__(self):
        """Initialize the memory cache."""
        self._cache = MemoryCache()
        self._lock = threading.RLock()

    def has(self, key: Hashable) -> bool:
        """Check if an item exists in the cache."""
        with self._lock:
            return self._cache.has(key)

    def get(self, key: Hashable, default: Any = _missing) -> Any:
        """Retrieve an item from the cache."""
        with self._lock:
            if default is self._missing:
                return self._cache.get(key)
            return self._cache.get(key, default)

    def get_or_set[T](self, key: Hashable, factory: Callable[[], T]) -> T:
        """Get an item from the cache or set it using a factory function."""
        with self._lock:
            return self._cache.get_or_set(key, factory)

    async def aget_or_set[T](self, key: Hashable, factory: Callable[[], Awaitable[T]]) -> T:
        """Asynchronously get an item from the cache or set it using a factory function."""
        raise NotImplementedError("ThreadSafeMemoryCache does not support async operations.")

    def set(self, key: Hashable, value: Any) -> None:
        """Store an item in the cache."""
        with self._lock:
            self._cache.set(key, value)

    def unset(self, key: Hashable) -> None:
        """Unset a cached item."""
        with self._lock:
            self._cache.unset(key)

    def clear(self) -> None:
        """Clear the cache."""
        with self._lock:
            self._cache.clear()

    async def aclear(self) -> None:
        """Asynchronously clear the cache."""
        raise NotImplementedError("ThreadSafeMemoryCache does not support async operations.")

    def is_empty(self) -> bool:
        """Check if the cache is empty."""
        with self._lock:
            return self._cache.is_empty()
