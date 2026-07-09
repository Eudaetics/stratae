"""Depends function for dependency injection."""

from inspect import iscoroutinefunction
from typing import Any, Awaitable, Callable, Hashable, Self, overload


class DependsWrapper:
    """Class used to wrap the dependency injection."""

    __slots__ = {"dependency", "_is_async"}

    _registry: dict[Hashable, Self] = {}
    dependency: Callable[..., Any]
    _is_async: bool

    def __new__(cls, dependency: Callable[..., Any]) -> Self:
        """Singleton factory for dependency wrappers."""
        existing = cls._registry.get(dependency)
        if existing is not None:
            return existing
        instance = super().__new__(cls)
        instance.dependency = dependency
        instance._is_async = iscoroutinefunction(dependency)
        cls._registry[dependency] = instance
        return instance

    def provide(self):
        """Provide the dependency."""
        return self.dependency()

    @property
    def is_async(self) -> bool:
        """Return True if the dependency is asynchronous, False otherwise."""
        return self._is_async


@overload
def Depends[**P, R](dependency: Callable[P, Awaitable[R]]) -> DependsWrapper: ...


@overload
def Depends[**P, R](dependency: Callable[P, R]) -> DependsWrapper: ...


def Depends[**P, R](dependency: Callable[P, R | Awaitable[R]]) -> DependsWrapper:
    """Marker function used to denote a dependency injection."""
    return DependsWrapper(dependency=dependency)
