"""
New scope classes - a parallel implementation, not yet wired into the rest of the package.

`BaseScope` holds the state a scope owns directly: the `ContextVar`/`SharedVar` for its
activation, the empty-slot template copied on activation, and the slot-allocation
bookkeeping `cache()` will use. `Scope` and `AsyncScope` are the concrete, usable
subclasses, differing only in which exit stack type they use.
"""

from contextvars import ContextVar
from typing import get_args

from stratae.lifecycle._scope import (
    UNSET,
    AsyncExitStack,
    ExitStack,
    ScopeVar,
    SharedVar,
    SlotDict,
    SlotStorage,
)
from stratae.lifecycle.exceptions import LifecycleConfigurationError
from stratae.lifecycle.scope import IsolationType, StorageType


class BaseScope:
    """
    Shared state and validation behind Scope and AsyncScope.

    See {py:class}`Scope` and {py:class}`AsyncScope` for the concrete, usable classes.

    Storage defaults to dense. Below ~50 registered functions, dense wins outright
    regardless of touched count - allocating a dict already costs more than copying the
    whole list. Above that, it's the touched/registered ratio that decides: dense and
    sparse roughly break even around 1-4% touched (2% at 1,000 registered / 20 touched),
    sparse pulling ahead below it (~4x faster at 1,000 registered / 0 touched) and dense
    pulling ahead above it (~1.5x faster at 1,000 registered / 90 touched).
    """

    __slots__ = (
        "name",
        "_isolation",
        "_storage",
        "_requires",
        "_var",
        "_template",
        "_counter",
        "_free_slots",
    )

    _exit_stack_cls: type[ExitStack] | type[AsyncExitStack]
    _isolation: IsolationType
    _storage: StorageType
    _requires: "BaseScope | None"
    _var: ScopeVar
    _template: SlotStorage
    _counter: int
    _free_slots: list[int]

    def __init__(
        self,
        name: str,
        isolation: IsolationType = "context",
        storage: StorageType = "dense",
        requires: "BaseScope | None" = None,
    ) -> None:
        """
        Initialize the scope's identity, isolation, and storage, and build its state.

        :param name: Identifier for the scope (e.g. `"request"`, `"application"`). Must
            be a valid Python identifier.
        :param isolation: Cache isolation strategy for this scope, one of the
            {py:data}`IsolationType` values. `"shared"` uses a single cache visible to all
            concurrent tasks/threads while the scope is active, regardless of execution
            context - suitable for application-wide state such as database pools.
            `"context"` (the default) isolates the cache per execution context, backed by
            a `contextvars.ContextVar`, so concurrent contexts (e.g. concurrent requests)
            each see their own cache - suitable for request- or session-scoped state.
        :param storage: Slot storage strategy for this scope's cached values, one of the
            {py:data}`StorageType` values. `"dense"` (the default) indexes slots directly
            by position - the cheapest per-access cost, but every activation pays to
            copy/reset the full slot list, so it fits scopes with few registered
            functions or where most of them get used per activation. `"sparse"` allocates
            slots lazily and resets in O(touched) rather than O(registered) - the fit for
            scopes registering many functions where a given activation only touches a
            handful, e.g. a large API's per-resource caches.
        :param requires: The scope that must be active before this one can activate, or
            `None` for a scope with no such requirement. A `"shared"` scope may only
            require another `"shared"` scope, since a `"context"` scope's activity is
            per-execution-context and there's no single answer to "is it active" that
            would hold for every context concurrently sharing the requiring scope.
        :raises LifecycleConfigurationError: If `name` is not a valid Python identifier,
            `isolation`/`storage` is not one of their allowed values, or this scope is
            `"shared"` while `requires` is a `"context"`-isolated scope.

        """
        if isolation not in frozenset(get_args(IsolationType)):
            raise LifecycleConfigurationError(f"Invalid scope isolation given for {name}.")
        if storage not in frozenset(get_args(StorageType)):
            raise LifecycleConfigurationError(f"Invalid scope storage given for {name}.")
        if requires is not None and isolation == "shared" and requires.isolation == "context":
            raise LifecycleConfigurationError(
                f"Shared scope {name!r} cannot require context-isolated scope {requires.name!r}."
            )

        self.name = name
        self._isolation = isolation
        self._storage = storage
        self._requires = requires
        self._template = [UNSET] if storage == "dense" else SlotDict()
        self._var = ContextVar(name) if isolation == "context" else SharedVar(name)
        self._counter = 1
        self._free_slots = []

    @property
    def isolation(self) -> IsolationType:
        """Cache isolation strategy for this scope - fixed at construction, never reassignable."""
        return self._isolation

    @property
    def storage(self) -> StorageType:
        """Slot storage strategy for this scope - fixed at construction, never reassignable."""
        return self._storage

    @property
    def requires(self) -> "BaseScope | None":
        """The scope that must be active before this one can activate, if any."""
        return self._requires


class Scope(BaseScope):
    """A sync-flavored scope - activated with `with`, caches sync functions."""

    __slots__ = ()

    _exit_stack_cls = ExitStack


class AsyncScope(BaseScope):
    """An async-flavored scope - activated with `async with`, caches sync and async functions."""

    __slots__ = ()

    _exit_stack_cls = AsyncExitStack
