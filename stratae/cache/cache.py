"""Protocol class for cache implementations."""

from typing import Any, Awaitable, Callable, Hashable, Protocol, runtime_checkable


@runtime_checkable
class Cache(Protocol):
    """Abstract base class for cache implementations."""

    def has(self, key: Hashable) -> bool:
        """Check if an item exists in the cache."""
        ...

    def get(self, key: Hashable, default: Any = ...) -> Any:
        """Retrieve an item from the cache."""
        ...

    def get_or_set[T](self, key: Hashable, factory: Callable[[], T]) -> T:
        """Get an item from the cache or set it using a factory function."""
        ...

    async def aget_or_set[T](self, key: Hashable, factory: Callable[[], Awaitable[T]]) -> T:
        """Asynchronously get an item from the cache or set it using a factory function."""
        ...

    def set(self, key: Hashable, value: Any) -> None:
        """Store an item in the cache."""
        ...

    def unset(self, key: Hashable) -> None:
        """Unset a cached item."""
        ...

    def clear(self) -> None:
        """Clear the cache."""
        ...

    async def aclear(self) -> None:
        """Asynchronously clear the cache."""
        ...

    def is_empty(self) -> bool:
        """Check if the cache is empty."""
        ...
