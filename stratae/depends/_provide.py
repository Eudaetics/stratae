from collections.abc import Callable, Hashable
from contextvars import ContextVar
from threading import Lock
from typing import Any, ClassVar, Self
from weakref import WeakValueDictionary

from stratae._callables import is_async_callable
from stratae.depends.exceptions import DependencyNotFoundError, UnhashableDependencyError

_UNSET = object()


class Provider:
    """A registered dependency provider and its override state."""

    __slots__ = {
        "__weakref__",
        "dependency",
        "is_async",
        "lock",
        "override",
        "override_count",
        "provide",
        "resolved",
    }

    _registry: ClassVar[WeakValueDictionary[Hashable, Self]] = WeakValueDictionary()
    dependency: Callable[[], Any]
    provide: Callable[[], Any]
    is_async: bool
    override: ContextVar[Any]
    override_count: int
    lock: Lock
    resolved: bool

    def __new__(cls, dependency: Callable[..., Any]) -> Self:
        """
        Return the single Provider for `dependency`.

        :raises InjectionSignatureError: If `dependency` is unhashable.
        """
        try:
            existing = cls._registry.get(dependency)
        except TypeError:
            raise UnhashableDependencyError(dependency) from None
        if existing is not None:
            return existing
        instance = super().__new__(cls)
        instance.dependency = dependency
        instance.provide = dependency
        instance.is_async = is_async_callable(dependency)
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
        """Replace the dependency and its provide function."""
        with self.lock:
            self.dependency = dependency
            if self.override_count == 0:
                self.provide = self.dependency

    @classmethod
    def find(cls, func: Callable[..., Any]):
        """Find the Provider registered for a dependency."""
        try:
            return cls._registry[func]
        except KeyError:
            raise DependencyNotFoundError(func) from None
