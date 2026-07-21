"""
Scope activation storage - shared vars, exit stacks, and the UNSET sentinel.

Each activated scope holds a `SlotStorage` (a list for "dense" scopes, a
`SlotDict` for "sparse" ones) reachable through a `ScopeVar` - a
`contextvars.ContextVar` for `isolation="context"` scopes, or a `SharedVar`
for `isolation="shared"` ones. `build_lifecycle_state` builds one of each,
plus the per-scope bookkeeping a `Lifecycle`/`AsyncLifecycle` needs, from a
sequence of `Scope` declarations.
"""

import threading
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from contextvars import ContextVar
from typing import Any, Callable, NamedTuple, Protocol, Sequence

from stratae.lifecycle._async_lock import AsyncRLock
from stratae.lifecycle.scope import Scope

UNSET: Any = object()
_MISSING: Any = object()


class SlotDict(dict[int, Any]):
    """Dict-backed slot storage - missing slots read as UNSET without inserting them."""

    __slots__ = ()

    def __missing__(self, key: int) -> Any:
        """Return UNSET for a slot that was never written, without inserting it."""
        return UNSET

    def copy(self) -> "SlotDict":
        """Return a shallow copy, preserving the `__missing__` behavior."""
        return SlotDict(self)


SlotStorage = list[Any] | SlotDict


class SharedToken:
    """Activation token for a shared scope, mirroring contextvars.Token's .var backref."""

    __slots__ = ("var",)

    var: "SharedVar"

    def __init__(self, var: "SharedVar") -> None:
        self.var = var


class SharedVar:
    """
    ContextVar-shaped holder for a shared scope's activation, visible to every context.

    The live SlotStorage sits in the storage slot - codegen'd wrappers bind the var and
    read the attribute directly - and UNSET there marks the scope inactive. set() always
    hands back the same token: shared activations don't nest, so deactivation clears the
    storage rather than restoring a prior value.

    A shared scope's slots can be read and written by concurrent callers while the scope
    is active - that's the point of "shared" - so a slot's first computation needs a lock
    to stay cached-once: lock for sync-flavored cached functions in this scope, async_lock
    for async ones. A given slot is only ever touched by one flavor, so one pair of locks
    per scope is enough. Both are reentrant, since a cached function's own computation can
    call another cached function in the same scope (e.g. a dependency chain), which would
    otherwise deadlock the same thread/task against its own lock.
    """

    __slots__ = ("name", "storage", "_token", "lock", "async_lock")

    def __init__(self, name: str) -> None:
        self.name = name
        self.storage: SlotStorage = UNSET
        self._token = SharedToken(self)
        self.lock = threading.RLock()
        self.async_lock = AsyncRLock()

    def get(self, default: Any = _MISSING) -> SlotStorage:
        """Return the live storage, or default when inactive, else raise LookupError."""
        value = self.storage
        if value is not UNSET:
            return value
        if default is _MISSING:
            raise LookupError(self.name)
        return default

    def set(self, value: SlotStorage) -> SharedToken:
        """Activate the scope with the given storage, returning its reusable token."""
        self.storage = value
        return self._token

    def clear(self) -> None:
        """Deactivate the scope, leaving UNSET as the storage."""
        self.storage = UNSET

    def reset(self, _token: SharedToken) -> None:
        """Deactivate like clear() - the token is unused since shared activations don't nest."""
        self.storage = UNSET


ScopeVar = ContextVar[SlotStorage] | SharedVar


class ScopeVarProto[TokenT](Protocol):
    """
    The set/reset pairing a lifecycle context needs, generic over the token type.

    Correlates a var with its token type so the ContextVar/Token and SharedVar/
    SharedToken pairs both type-check a reset(token) call without narrowing at the
    call site - narrowing would cost an isinstance per activation exit.
    """

    def set(self, value: SlotStorage, /) -> TokenT:
        """Activate the scope with `value`, returning a token `reset` accepts."""
        ...

    def reset(self, token: TokenT, /) -> None:
        """Deactivate the scope, using the token returned by the matching `set`."""
        ...


class LifecycleState(NamedTuple):
    """
    Per-scope state a Lifecycle/AsyncLifecycle manager builds once at construction.

    Args:
        templates: Each scope's empty-slot template, copied on every
            activation.
        scope_vars: Each scope's activation holder - a `ContextVar` for
            context-isolated scopes, a `SharedVar` for shared ones.
        contexts: One reusable context-manager instance per shared scope,
            keyed by scope name; context-isolated scopes are absent here
            since they need a fresh instance per activation.
        counters: Per-scope slot counter for sparse-backed scopes, used to
            mint new slot indices.
        free_slots: Per-scope stack of released slot indices available for
            reuse before minting a new one.

    """

    templates: dict[str, SlotStorage]
    scope_vars: dict[str, ScopeVar]
    contexts: dict[str, Any]
    counters: dict[str, int]
    free_slots: dict[str, list[int]]


def _build_templates(scopes: Sequence[Scope]) -> dict[str, SlotStorage]:
    """Build each scope's empty-slot template - a list for "dense" storage, else a dict."""
    return {scope.name: [UNSET] if scope.storage == "dense" else SlotDict() for scope in scopes}


def _build_scope_vars(scopes: Sequence[Scope]) -> dict[str, ScopeVar]:
    """Build each scope's activation holder - a ContextVar if context-isolated, else a SharedVar."""
    return {
        scope.name: (
            ContextVar(scope.name) if scope.isolation == "context" else SharedVar(scope.name)
        )
        for scope in scopes
    }


def _build_contexts(
    scopes: Sequence[Scope],
    scope_vars: dict[str, ScopeVar],
    templates: dict[str, SlotStorage],
    context_cls: Callable[[ScopeVarProto[Any], SlotStorage], Any],
) -> dict[str, Any]:
    """
    Build one reusable context manager per shared scope.

    Shared activations don't nest, so one instance per scope can carry the activation
    state that context-isolated scopes need a fresh instance for on every start().
    """
    return {
        scope.name: context_cls(scope_vars[scope.name], templates[scope.name])
        for scope in scopes
        if scope.isolation == "shared"
    }


def _build_counters(scopes: Sequence[Scope]) -> dict[str, int]:
    """Build the per-scope slot counter used by sparse-backed scopes, starting after slot 0."""
    return {scope.name: 1 for scope in scopes if scope.storage == "sparse"}


def build_lifecycle_state(
    scopes: Sequence[Scope],
    context_cls: Callable[[ScopeVarProto[Any], SlotStorage], Any],
) -> LifecycleState:
    """
    Build the per-scope state shared by Lifecycle and AsyncLifecycle construction.

    Both managers derive identical state from their scopes, differing only in which
    context manager class wraps a shared scope's activation - passed in so this stays
    agnostic to sync vs. async.

    Args:
        scopes: The scopes to build state for, already validated.
        context_cls: `LifecycleContext` or `AsyncLifecycleContext`,
            determining which context manager class wraps a shared scope's
            activation.

    Returns:
        The `LifecycleState` a `Lifecycle`/`AsyncLifecycle` stores on itself.

    """
    templates = _build_templates(scopes)
    scope_vars = _build_scope_vars(scopes)
    contexts = _build_contexts(scopes, scope_vars, templates, context_cls)
    counters = _build_counters(scopes)
    free_slots: dict[str, list[int]] = {scope.name: [] for scope in scopes}
    return LifecycleState(templates, scope_vars, contexts, counters, free_slots)


def _raise_collected(exc: Exception) -> None:
    """Collect an exception's __context__ chain and raise it as an ExceptionGroup."""
    exceptions: list[Exception] = [exc]
    ctx = exc.__context__
    while ctx:
        if isinstance(ctx, Exception):
            exceptions.append(ctx)
        ctx = getattr(ctx, "__context__", None)
    if len(exceptions) > 1:
        raise ExceptionGroup("Multiple exceptions raised during scope cleanup", exceptions)
    raise exc


def _close_one(ctx: AbstractContextManager[Any], exc: Exception | None) -> Exception | None:
    exc_type = type(exc) if exc else None
    tb = exc.__traceback__ if exc else None
    try:
        suppressed = ctx.__exit__(exc_type, exc, tb)
    except Exception as new_exc:
        return new_exc
    return None if suppressed else exc


async def _aclose_one(
    ctx: AbstractAsyncContextManager[Any], exc: Exception | None
) -> Exception | None:
    exc_type = type(exc) if exc else None
    tb = exc.__traceback__ if exc else None
    try:
        suppressed = await ctx.__aexit__(exc_type, exc, tb)
    except Exception as new_exc:
        return new_exc
    return None if suppressed else exc


class ExitStack:
    """
    Sync exit stack for context managers entered by resources within a scope activation.

    Lives in a scope's reserved slot 0, created lazily on first use; `close`
    unwinds every entered context manager in reverse order when the owning
    scope deactivates.
    """

    __slots__ = ("_contexts",)

    def __init__(self) -> None:
        """Initialize with no registered context managers."""
        self._contexts: list[AbstractContextManager[Any]] = []

    def enter_context[R](self, ctx: AbstractContextManager[R]) -> R:
        """Enter a context manager and register it for cleanup on close()."""
        result = ctx.__enter__()
        self._contexts.append(ctx)
        return result

    def close(self) -> None:
        """Close every registered context manager, in reverse order, running all of them."""
        exc: Exception | None = None
        while self._contexts:
            exc = _close_one(self._contexts.pop(), exc)
        if exc is not None:
            _raise_collected(exc)


class AsyncExitStack:
    """
    Async exit stack for context managers entered by resources within a scope activation.

    Lives in a scope's reserved slot 0, created lazily on first use; `aclose`
    unwinds every entered context manager in reverse order when the owning
    scope deactivates, awaiting only the async ones.
    """

    __slots__ = ("_contexts",)

    def __init__(self) -> None:
        """Initialize with no registered context managers."""
        self._contexts: list[tuple[bool, Any]] = []

    def enter_context[T](self, ctx: AbstractContextManager[T]) -> T:
        """Enter a sync context manager and register it for cleanup on aclose()."""
        result = ctx.__enter__()
        self._contexts.append((False, ctx))
        return result

    async def enter_async_context[T](self, ctx: AbstractAsyncContextManager[T]) -> T:
        """Enter an async context manager and register it for cleanup on aclose()."""
        result = await ctx.__aenter__()
        self._contexts.append((True, ctx))
        return result

    async def aclose(self) -> None:
        """Close every registered context manager, in reverse order, running all of them."""
        exc: Exception | None = None
        while self._contexts:
            is_async, ctx = self._contexts.pop()
            if is_async:
                exc = await _aclose_one(ctx, exc)
            else:
                exc = _close_one(ctx, exc)
        if exc is not None:
            _raise_collected(exc)
