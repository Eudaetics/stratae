from abc import abstractmethod
from collections.abc import Awaitable, Callable, Generator, Hashable
from contextvars import ContextVar
from threading import Lock, RLock
from typing import Any, ClassVar, TypeGuard
from weakref import WeakValueDictionary

from stratae.callables import is_async_callable
from stratae.depends.exceptions import UnhashableDependencyError


class Held[Value]:
    """
    An override value as its provider's ContextVar holds it, and awaitable for an async one.

    It lives as long as some context can see it, including contexts copied by tasks started
    inside the override. Its finalizer tells the provider when to leave the override path.
    """

    __slots__ = ("__weakref__", "value")

    def __init__(self, value: Value) -> None:
        self.value = value

    def __await__(self) -> Generator[None, None, Value]:
        if False:  # noqa: S5797
            yield
        return self.value


class Provider[Value, Provided]:
    """
    A registered dependency provider and its override state.

    `provide` is the dependency itself until an override is entered. It's the override path
    while any context can still see an override value, and the dependency again once none
    can. Typed by the dependency's value, and by what providing gives: the value itself, or an
    awaitable of it for an async dependency.
    """

    __slots__ = (
        "__weakref__",
        "dependency",
        "live",
        "lock",
        "override",
        "provide",
        "resolved",
    )

    is_async: ClassVar[bool]
    """Whether providing gives an awaitable."""
    dependency: Callable[..., Provided]
    """The dependency, or once resolved, the dependency with its own dependencies injected."""
    provide: Callable[[], Provided]
    """What injection calls: the dependency, or the override path while one is live."""
    override: ContextVar[Held[Value] | None]
    """The override value each context sees, or `None` if it sees none."""
    live: int
    lock: RLock
    resolved: bool

    def __init__(self, dependency: Callable[..., Provided]) -> None:
        """Provide `dependency`, with no override."""
        self.dependency = dependency
        self.provide = dependency
        self.override = ContextVar(f"{dependency}_dep", default=None)
        self.live = 0
        self.lock = RLock()
        self.resolved = False

    @abstractmethod
    def provide_held(self) -> Provided:
        """Return the override's value if set, otherwise the dependency's."""
        ...

    def hold(self) -> None:
        """Count a new override value; the first moves `provide` onto the override path."""
        with self.lock:
            self.live += 1
            if self.live == 1:
                self.provide = self.provide_held

    def release(self) -> None:
        """Count an override value no context can see; the last moves `provide` back."""
        with self.lock:
            self.live -= 1
            if self.live == 0:
                self.provide = self.dependency

    def update(self, dependency: Callable[..., Provided]) -> None:
        """Replace the dependency and its provide function."""
        with self.lock:
            self.dependency = dependency
            if self.live == 0:
                self.provide = self.dependency


class SyncProvider[Value](Provider[Value, Value]):
    """A provider of a sync dependency: providing gives the value."""

    __slots__ = ()

    is_async: ClassVar[bool] = False

    def provide_held(self) -> Value:
        """Return the override's value if set, otherwise evaluate the dependency."""
        held = self.override.get()
        if held is None:
            return self.dependency()
        return held.value


class AsyncProvider[Value](Provider[Value, Awaitable[Value]]):
    """A provider of an async dependency: providing gives an awaitable of the value."""

    __slots__ = ()

    is_async: ClassVar[bool] = True

    def provide_held(self) -> Awaitable[Value]:
        """Return an awaitable of the override's value if set, otherwise the dependency's."""
        held = self.override.get()
        if held is None:
            return self.dependency()
        return held


type AnyProvider = SyncProvider[Any] | AsyncProvider[Any]
"""A provider of some dependency, as the registry holds it."""


def is_provider(obj: object) -> TypeGuard[AnyProvider]:
    """Whether `obj` is a provider, of whatever dependency."""
    return isinstance(obj, Provider)


_registry: WeakValueDictionary[Hashable, AnyProvider] = WeakValueDictionary()
"""Each dependency's provider, for as long as something holds it."""

_registering = Lock()


def provider(dependency: Callable[..., object]) -> AnyProvider:
    """
    Return the single provider for `dependency`, making it if there's none yet.

    Threads that ask for the same dependency at once all get the same provider.

    :raises InjectionSignatureError: If `dependency` is unhashable.
    """
    try:
        existing = _registry.get(dependency)
    except TypeError:
        raise UnhashableDependencyError(dependency) from None
    if existing is not None:
        return existing
    made: AnyProvider = (
        AsyncProvider(dependency) if is_async_callable(dependency) else SyncProvider(dependency)
    )
    with _registering:
        return _registry.setdefault(dependency, made)
