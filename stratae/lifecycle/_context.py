"""Context managers for lifecycle scope activations."""

from contextvars import ContextVar
from typing import Any

from stratae.lifecycle._scope import UNSET


class SharedLifecycleContext:
    """Reusable context manager for a shared-isolation scope on a sync Lifecycle."""

    __slots__ = ("_scope", "_entry", "_active", "_template")

    def __init__(
        self,
        scope: str,
        entry: list[Any],
        active: dict[str, list[Any]],
        template: list[Any],
    ) -> None:
        """Initialize with the scope's permanent slot list and reset template pre-resolved."""
        self._scope = scope
        self._entry = entry
        self._active = active
        self._template = template

    def __enter__(self) -> None:
        """Activate the scope - push minus the lookup already done at construction."""
        self._active[self._scope] = self._entry

    def __exit__(self, *_) -> None:
        """Deactivate the scope, closing its exit stack if one was created (slot 0)."""
        self._active.pop(self._scope, None)
        slots = self._entry
        stack = slots[0]
        slots[:] = self._template
        if stack is not UNSET:
            stack.close()


class IsolatedLifecycleContext:
    """Per-activation context manager for a context-isolation scope on a sync Lifecycle."""

    __slots__ = ("_cv", "_template", "_slots", "token")

    def __init__(self, cv: ContextVar[list[Any]], template: list[Any]) -> None:
        """Initialize with the scope's ContextVar and all-UNSET slot template pre-resolved."""
        self._cv = cv
        self._template = template

    def __enter__(self) -> None:
        """Activate the scope in the current execution context."""
        slots = self._template.copy()
        self.token = self._cv.set(slots)
        self._slots = slots

    def __exit__(self, *_) -> None:
        """Deactivate the scope, closing its exit stack if one was created (slot 0)."""
        self._cv.reset(self.token)
        stack = self._slots[0]
        if stack is not UNSET:
            stack.close()


class AsyncSharedLifecycleContext:
    """Reusable async context manager for a shared-isolation scope on an AsyncLifecycle."""

    __slots__ = ("_scope", "_entry", "_active", "_template")

    def __init__(
        self,
        scope: str,
        entry: list[Any],
        active: dict[str, list[Any]],
        template: list[Any],
    ) -> None:
        """Initialize with the scope's permanent slot list and reset template pre-resolved."""
        self._scope = scope
        self._entry = entry
        self._active = active
        self._template = template

    async def __aenter__(self) -> None:
        """Activate the scope - push minus the lookup already done at construction."""
        self._active[self._scope] = self._entry

    async def __aexit__(self, *_) -> None:
        """Deactivate the scope, closing its exit stack if one was created (slot 0)."""
        self._active.pop(self._scope, None)
        slots = self._entry
        stack = slots[0]
        slots[:] = self._template
        if stack is not UNSET:
            await stack.aclose()


class AsyncIsolatedLifecycleContext:
    """Per-activation async context manager for a context-isolation scope on an AsyncLifecycle."""

    __slots__ = ("_cv", "_template", "_slots", "token")

    def __init__(self, cv: ContextVar[list[Any]], template: list[Any]) -> None:
        """Initialize with the scope's ContextVar and all-UNSET slot template pre-resolved."""
        self._cv = cv
        self._template = template

    async def __aenter__(self):
        """Activate the scope - AsyncLifecycle.push minus the lookups done at start()."""
        slots = self._template.copy()
        self.token = self._cv.set(slots)
        self._slots = slots

    async def __aexit__(self, *_) -> None:
        """Deactivate the scope, closing its exit stack if one was created (slot 0)."""
        self._cv.reset(self.token)
        stack = self._slots[0]
        if stack is not UNSET:
            await stack.aclose()
