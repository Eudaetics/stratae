"""Depends function for dependency injection."""

from inspect import iscoroutinefunction
from typing import Any, Awaitable, Callable, cast, overload


class DependsWrapper:
    """Class used to wrap the dependency injection."""

    def __init__(self, dependency: Callable[..., Any]) -> None:
        """Initialize the Depends instance with an injectable dependency."""
        self.dependency = dependency
        self._is_async = iscoroutinefunction(dependency)

    def provide(self):
        """Provide the dependency."""
        return self.dependency()

    async def aprovide(self):
        """Asynchronously provide the dependency."""
        return await self.dependency()

    @property
    def is_async(self) -> bool:
        """Return True if the dependency is asynchronous, False otherwise."""
        return self._is_async


@overload
def Depends[**P, R](dependency: Callable[P, Awaitable[R]]) -> R: ...


@overload
def Depends[**P, R](dependency: Callable[P, R]) -> R: ...


def Depends[**P, R](dependency: Callable[P, R | Awaitable[R]]) -> R:
    """Marker function used to denote a dependency injection."""
    return cast(R, DependsWrapper(dependency=dependency))
