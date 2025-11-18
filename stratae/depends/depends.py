"""Depends function for dependency injection."""

from inspect import iscoroutinefunction, unwrap
from typing import Any, Awaitable, Callable, cast, overload

AUTO: Any = None


class DependsWrapper:
    """Class used to wrap the dependency injection."""

    def __init__(self, dependency: Callable[..., Any], allow_override: bool = True) -> None:
        """Initialize the Depends instance with an injectable dependency."""
        self.dependency = dependency
        self._is_async = iscoroutinefunction(dependency)
        self.allow_override = allow_override

    def provide(self):
        """Provide the dependency."""
        self._fix_outermost()
        self.provide = self._fixed_provide
        return self.dependency()

    async def aprovide(self):
        """Asynchronously provide the dependency."""
        self._fix_outermost()
        self.aprovide = self._fixed_aprovide
        return await self.dependency()

    def _fixed_provide(self):
        return self.dependency()

    async def _fixed_aprovide(self):
        return await self.dependency()

    def _fix_outermost(self) -> None:
        """Fix the dependency to use its outermost version, if applicable."""
        original = unwrap(self.dependency)
        if hasattr(original, "__outermost__"):
            self.dependency = original.__outermost__
        self._resolved_outermost = True

    @property
    def is_async(self) -> bool:
        """Return True if the dependency is asynchronous, False otherwise."""
        return self._is_async


@overload
def Depends[**P, R](dependency: Callable[P, Awaitable[R]], *, allow_override: bool = True) -> R: ...


@overload
def Depends[**P, R](dependency: Callable[P, R], *, allow_override: bool = True) -> R: ...


def Depends[**P, R](dependency: Callable[P, R | Awaitable[R]], *, allow_override: bool = True) -> R:
    """Marker function used to denote a dependency injection."""
    return cast(R, DependsWrapper(dependency=dependency, allow_override=allow_override))
