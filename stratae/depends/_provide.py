from collections.abc import Awaitable, Callable, Hashable
from contextvars import ContextVar
from threading import RLock
from typing import Any, ClassVar, Self
from weakref import WeakValueDictionary

from stratae._callables import is_async_callable
from stratae.depends.exceptions import UnhashableDependencyError

_UNSET = object()


class Provider:
    """
    A registered dependency provider and its override state.

    `provide` is the dependency itself until an override is entered. It's the override path
    while any context can still see an override value, and the dependency again once none
    can.
    """

    __slots__ = {
        "__weakref__",
        "dependency",
        "is_async",
        "live",
        "lock",
        "override",
        "provide",
        "resolved",
    }

    _registry: ClassVar[WeakValueDictionary[Hashable, Self]] = WeakValueDictionary()
    dependency: Callable[[], Any]
    provide: Callable[[], Any]
    is_async: bool
    override: ContextVar[Any]
    live: int
    lock: RLock
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
        instance.live = 0
        instance.lock = RLock()
        instance.resolved = False
        cls._registry[dependency] = instance
        return instance

    def provide_held(self) -> Any:
        """Return the override's value if set, otherwise evaluate the dependency."""
        held = self.override.get()
        if held is _UNSET:
            return self.dependency()
        return held.value

    def provide_held_async(self) -> Awaitable[Any]:
        """Return an awaitable of the override's value if set, otherwise the dependency's."""
        held = self.override.get()
        if held is _UNSET:
            return self.dependency()
        return held

    def hold(self) -> None:
        """Count a new override value; the first moves `provide` onto the override path."""
        with self.lock:
            self.live += 1
            if self.live == 1:
                self.provide = self.provide_held_async if self.is_async else self.provide_held

    def release(self) -> None:
        """Count an override value no context can see; the last moves `provide` back."""
        with self.lock:
            self.live -= 1
            if self.live == 0:
                self.provide = self.dependency

    def update(self, dependency: Callable[..., Any]) -> None:
        """Replace the dependency and its provide function."""
        with self.lock:
            self.dependency = dependency
            if self.live == 0:
                self.provide = self.dependency
