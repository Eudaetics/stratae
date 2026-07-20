from contextvars import ContextVar
from inspect import iscoroutinefunction
from threading import Lock
from typing import Any, Callable, Hashable, Self
from weakref import WeakValueDictionary

from stratae.depends.exceptions import DependencyNotFoundError

_UNSET = object()


class Provider:
    """Class used to wrap the dependency injection."""

    __slots__ = {
        "dependency",
        "provide",
        "is_async",
        "override",
        "override_count",
        "lock",
        "resolved",
        "__weakref__",
    }

    _registry: WeakValueDictionary[Hashable, Self] = WeakValueDictionary()
    dependency: Callable[[], Any]
    provide: Callable[[], Any]
    is_async: bool
    override: ContextVar[Any]
    override_count: int
    lock: Lock
    resolved: bool

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
        instance.resolved = False
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
        """Find the associated Provider for the injected dependency."""
        try:
            return cls._registry[func]
        except KeyError:
            raise DependencyNotFoundError(f"No Dependency found for {func}") from None
