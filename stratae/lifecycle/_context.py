"""
Context managers for lifecycle scope activations, returned by `start`.

`LifecycleContext` and `AsyncLifecycleContext` are the objects
`BaseLifecycle.start` hands back for use as ``with``/``async with`` blocks;
entering activates the scope's slot storage, exiting deactivates it and
closes any exit stack the activation created.
"""

from types import TracebackType
from typing import Any

from stratae.lifecycle._scope import UNSET, ScopeVarProto, SlotStorage


class LifecycleContext:
    """
    Context manager for a scope activation on a sync Lifecycle.

    Returned by `Lifecycle.start`, not constructed directly. Works for both
    isolations through the ScopeVarProto interface, which pairs set with
    reset so exit needs no narrowing: shared scopes reuse one instance per scope (their
    activations don't nest), context-isolated scopes get a fresh instance per start().
    Deliberately not generic over the token type - a Generic base taxes per-activation
    instantiation - so the var/token pairing is typed Any here.
    """

    __slots__ = ("_var", "_template", "_slots", "token")

    token: Any

    def __init__(self, var: ScopeVarProto[Any], template: SlotStorage) -> None:
        """
        Initialize with the scope's var and all-UNSET slot template pre-resolved.

        Args:
            var: The scope's activation holder - a `ContextVar` for a
                context-isolated scope, a `SharedVar` for a shared one.
            template: The scope's empty-slot template, copied fresh on
                each `__enter__`.

        """
        self._var = var
        self._template = template

    def __enter__(self) -> None:
        """Activate the scope with a fresh copy of the template."""
        slots = self._template.copy()
        self.token = self._var.set(slots)
        self._slots = slots

    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc: Exception | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate the scope, closing its exit stack (if created) with the block's exception."""
        self._var.reset(self.token)
        stack = self._slots[0]
        if stack is not UNSET:
            return stack.close(exc)
        return None


class AsyncLifecycleContext:
    """
    Async context manager for a scope activation on an AsyncLifecycle.

    Returned by `AsyncLifecycle.start`, not constructed directly. Works for both
    isolations through the ScopeVarProto interface, which pairs set with
    reset so exit needs no narrowing: shared scopes reuse one instance per scope (their
    activations don't nest), context-isolated scopes get a fresh instance per start().
    Deliberately not generic over the token type - a Generic base taxes per-activation
    instantiation - so the var/token pairing is typed Any here.
    """

    __slots__ = ("_var", "_template", "_slots", "token")

    token: Any

    def __init__(self, var: ScopeVarProto[Any], template: SlotStorage) -> None:
        """
        Initialize with the scope's var and all-UNSET slot template pre-resolved.

        Args:
            var: The scope's activation holder - a `ContextVar` for a
                context-isolated scope, a `SharedVar` for a shared one.
            template: The scope's empty-slot template, copied fresh on
                each `__aenter__`.

        """
        self._var = var
        self._template = template

    async def __aenter__(self) -> None:
        """Activate the scope with a fresh copy of the template."""
        slots = self._template.copy()
        self.token = self._var.set(slots)
        self._slots = slots

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc: Exception | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate the scope, closing its exit stack (if created) with the block's exception."""
        self._var.reset(self.token)
        stack = self._slots[0]
        if stack is not UNSET:
            return await stack.aclose(exc)
        return None
