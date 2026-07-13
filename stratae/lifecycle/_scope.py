"""Exit stacks and the UNSET sentinel backing lifecycle scope activations."""

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from contextvars import ContextVar
from typing import Any, Callable, NamedTuple, Sequence

from stratae.lifecycle.scope import Scope

UNSET: Any = object()


class SlotDict(dict[int, Any]):
    """Dict-backed slot storage - missing slots read as UNSET without inserting them."""

    __slots__ = ()

    def __missing__(self, key: int) -> Any:
        return UNSET

    def copy(self) -> "SlotDict":
        return SlotDict(self)


SlotStorage = list[Any] | SlotDict


class LifecycleState(NamedTuple):
    """Per-scope state a Lifecycle/AsyncLifecycle manager builds once at construction."""

    templates: dict[str, SlotStorage]
    cvars: dict[str, ContextVar[SlotStorage]]
    shared: dict[str, SlotStorage]
    active: dict[str, SlotStorage]
    contexts: dict[str, Any]
    counters: dict[str, int]


def _build_templates(scopes: Sequence[Scope]) -> dict[str, SlotStorage]:
    """Build each scope's empty-slot template - a list for "dense" storage, else a dict."""
    return {scope.name: [UNSET] if scope.storage == "dense" else SlotDict() for scope in scopes}


def _build_cvars(scopes: Sequence[Scope]) -> dict[str, ContextVar[SlotStorage]]:
    """Build one ContextVar per context-isolated scope."""
    return {scope.name: ContextVar(scope.name) for scope in scopes if scope.isolation == "context"}


def _build_shared(
    scopes: Sequence[Scope], templates: dict[str, SlotStorage]
) -> dict[str, SlotStorage]:
    """Build each shared-isolation scope's permanent slot storage from its template."""
    return {
        scope.name: templates[scope.name].copy() for scope in scopes if scope.isolation == "shared"
    }


def _build_contexts(
    shared: dict[str, SlotStorage],
    templates: dict[str, SlotStorage],
    active: dict[str, SlotStorage],
    shared_context_cls: Callable[[str, SlotStorage, dict[str, SlotStorage], SlotStorage], Any],
) -> dict[str, Any]:
    """
    Build one reusable context manager per shared scope, all sharing the same active dict.

    The manager's push()/pop()/get_slots() must observe that identical `active` object.
    """
    return {
        name: shared_context_cls(name, entry, active, templates[name])
        for name, entry in shared.items()
    }


def _build_counters(scopes: Sequence[Scope]) -> dict[str, int]:
    """Build the per-scope slot counter used by sparse-backed scopes, starting after slot 0."""
    return {scope.name: 1 for scope in scopes if scope.storage == "sparse"}


def build_lifecycle_state(
    scopes: Sequence[Scope],
    shared_context_cls: Callable[[str, SlotStorage, dict[str, SlotStorage], SlotStorage], Any],
) -> LifecycleState:
    """
    Build the per-scope state shared by Lifecycle and AsyncLifecycle construction.

    Both managers derive identical state from their scopes, differing only in which
    context manager class wraps a shared scope's activation - passed in so this stays
    agnostic to sync vs. async.
    """
    templates = _build_templates(scopes)
    cvars = _build_cvars(scopes)
    shared = _build_shared(scopes, templates)
    active: dict[str, SlotStorage] = {}
    contexts = _build_contexts(shared, templates, active, shared_context_cls)
    counters = _build_counters(scopes)
    return LifecycleState(templates, cvars, shared, active, contexts, counters)


def reset_slots(slots: SlotStorage, template: SlotStorage) -> None:
    """
    Reset slot storage in place for the scope's next activation.

    In place rather than replacing, since the manager's _shared entry and whatever a
    shared context captured as its own _entry are independent references to the same
    object - only an in-place reset keeps both observing the same state without a
    lookup. Lists refill from their template; dicts just empty, since missing keys
    already read as UNSET.
    """
    if isinstance(slots, list):
        slots[:] = template
    else:
        slots.clear()


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
    __slots__ = ("_contexts",)

    def __init__(self) -> None:
        self._contexts: list[AbstractContextManager[Any]] = []

    def enter_context[R](self, ctx: AbstractContextManager[R]) -> R:
        result = ctx.__enter__()
        self._contexts.append(ctx)
        return result

    def close(self) -> None:
        exc: Exception | None = None
        while self._contexts:
            exc = _close_one(self._contexts.pop(), exc)
        if exc is not None:
            _raise_collected(exc)


class AsyncExitStack:
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
