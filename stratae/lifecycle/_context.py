"""Context managers for lifecycle scope activations."""

from typing import Any

from stratae.lifecycle._scope import UNSET, ScopeVarProto, SlotStorage


class LifecycleContext:
    """
    Context manager for a scope activation on a sync Lifecycle.

    Works for both isolations through the ScopeVarProto interface, which pairs set with
    reset so exit needs no narrowing: shared scopes reuse one instance per scope (their
    activations don't nest), context-isolated scopes get a fresh instance per start().
    Deliberately not generic over the token type - a Generic base taxes per-activation
    instantiation - so the var/token pairing is typed Any here.
    """

    __slots__ = ("_var", "_template", "_slots", "token")

    token: Any

    def __init__(self, var: ScopeVarProto[Any], template: SlotStorage) -> None:
        """Initialize with the scope's var and all-UNSET slot template pre-resolved."""
        self._var = var
        self._template = template

    def __enter__(self) -> None:
        """Activate the scope with a fresh copy of the template."""
        slots = self._template.copy()
        self.token = self._var.set(slots)
        self._slots = slots

    def __exit__(self, *_) -> None:
        """Deactivate the scope, closing its exit stack if one was created (slot 0)."""
        self._var.reset(self.token)
        stack = self._slots[0]
        if stack is not UNSET:
            stack.close()


class AsyncLifecycleContext:
    """
    Async context manager for a scope activation on an AsyncLifecycle.

    Works for both isolations through the ScopeVarProto interface, which pairs set with
    reset so exit needs no narrowing: shared scopes reuse one instance per scope (their
    activations don't nest), context-isolated scopes get a fresh instance per start().
    Deliberately not generic over the token type - a Generic base taxes per-activation
    instantiation - so the var/token pairing is typed Any here.
    """

    __slots__ = ("_var", "_template", "_slots", "token")

    token: Any

    def __init__(self, var: ScopeVarProto[Any], template: SlotStorage) -> None:
        """Initialize with the scope's var and all-UNSET slot template pre-resolved."""
        self._var = var
        self._template = template

    async def __aenter__(self) -> None:
        """Activate the scope with a fresh copy of the template."""
        slots = self._template.copy()
        self.token = self._var.set(slots)
        self._slots = slots

    async def __aexit__(self, *_) -> None:
        """Deactivate the scope, closing its exit stack if one was created (slot 0)."""
        self._var.reset(self.token)
        stack = self._slots[0]
        if stack is not UNSET:
            await stack.aclose()
