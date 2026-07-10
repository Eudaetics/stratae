"""Depends function for dependency injection."""

from contextvars import ContextVar
from inspect import iscoroutinefunction
from threading import Lock
from typing import Any, Awaitable, Callable, Hashable, Self, overload

from stratae.depends.exceptions import DependencyNotFoundError

_UNSET = object()


class DependsWrapper:
    """Class used to wrap the dependency injection."""

    __slots__ = {"dependency", "provide", "is_async", "override", "override_count", "lock"}

    _registry: dict[Hashable, Self] = {}
    dependency: Callable[[], Any]
    provide: Callable[[], Any]
    is_async: bool
    override: ContextVar[Any]
    override_count: int
    lock: Lock

    def __new__(cls, dependency: Callable[..., Any]) -> Self:
        """Singleton factory for dependency wrappers."""
        existing = cls._registry.get(dependency)
        if existing is not None:
            return existing
        instance = super().__new__(cls)
        instance.dependency = dependency
        instance.provide = dependency
        instance.is_async = iscoroutinefunction(dependency)
        instance.override = ContextVar[Any](f"{dependency}_dep", default=_UNSET)
        instance.override_count = 0
        instance.lock = Lock()
        cls._registry[dependency] = instance
        return instance

    def provide_override(self):
        """Return override if set, otherwise evaluate the dependency."""
        ctx = self.override.get()
        if ctx is _UNSET:
            return self.dependency()
        return ctx

    def update(self, dependency: Callable[..., Any]):
        """Update the dependency while also correcting the provide."""
        with self.lock:
            self.dependency = dependency
            if self.override_count == 0:
                self.provide = self.dependency

    @classmethod
    def find(cls, func: Callable[..., Any]):
        """Find the associated DependsWrapper for the injected dependency."""
        try:
            return cls._registry[func]
        except KeyError:
            raise DependencyNotFoundError(f"No Dependency found for {func}") from None


@overload
def Depends[**P, R](dependency: Callable[P, Awaitable[R]]) -> DependsWrapper: ...


@overload
def Depends[**P, R](dependency: Callable[P, R]) -> DependsWrapper: ...


def Depends[**P, R](dependency: Callable[P, R | Awaitable[R]]) -> DependsWrapper:
    """Marker function used to denote a dependency injection."""
    return DependsWrapper(dependency=dependency)
