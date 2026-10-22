import threading
from contextvars import ContextVar
from typing import Any

from stratae.lifecycle.exceptions import ScopeActivationError

UNSET: Any = object()
_MISSING: Any = object()


class SlotDict(dict[int, Any]):
    """
    Dict slot storage where missing slots read as UNSET.

    Live storage always holds slots 0-3, so an empty SlotDict is an ended activation.
    """

    __slots__ = ()

    def __missing__(self, key: int) -> Any:
        """
        Return UNSET for an unwritten slot.

        :raises KeyError: If the activation has ended.
        """
        if self:
            return UNSET
        raise KeyError(key)

    def copy(self) -> "SlotDict":
        """Return a shallow copy, preserving the `__missing__` behavior."""
        return SlotDict(self)


SlotStorage = list[Any] | SlotDict


class SharedVar:
    """A ContextVar stand-in holding one storage for every context, rejecting re-activation."""

    __slots__ = ("name", "storage", "lock")

    def __init__(self, name: str) -> None:
        self.name = name
        self.storage: SlotStorage = UNSET
        self.lock = threading.Lock()

    def get(self, default: Any = _MISSING) -> SlotStorage:
        """Return the live storage, or default when inactive, else raise LookupError."""
        value = self.storage
        if value is not UNSET:
            return value
        if default is _MISSING:
            raise LookupError(self.name)
        return default

    def set(self, value: SlotStorage) -> None:
        """Activate the scope, raising if it's already active."""
        with self.lock:
            if self.storage is not UNSET:
                raise ScopeActivationError(
                    f"Cannot activate shared scope {self.name!r}: already active."
                )
            self.storage = value

    def reset(self, token: None) -> None:
        """Deactivate the scope; `token` is unused, kept to match `ContextVar.reset`."""
        self.storage = UNSET


ScopeVar = ContextVar[SlotStorage] | SharedVar
