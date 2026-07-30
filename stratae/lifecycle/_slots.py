import threading
from contextvars import ContextVar
from typing import Any

from stratae.lifecycle._async_lock import AsyncRLock
from stratae.lifecycle.exceptions import ScopeActivationError

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


class SharedToken:
    """Activation token for a shared scope, mirroring contextvars.Token's .var backref."""

    __slots__ = ("var",)

    var: "SharedVar"

    def __init__(self, var: "SharedVar") -> None:
        self.var = var


SlotStorage = list[Any] | SlotDict


class SharedVar:
    """A SharedVar that rejects re-entrant activation and stale-token deactivation."""

    __slots__ = (
        "name",
        "storage",
        "_token",
        "lock",
        "async_lock",
        "_current_token",
    )

    def __init__(self, name: str) -> None:
        self.name = name
        self.storage: SlotStorage = UNSET
        self._token = SharedToken(self)
        self.lock = threading.RLock()
        self.async_lock = AsyncRLock()
        self._current_token: SharedToken | None = None

    def get(self, default: Any = _MISSING) -> SlotStorage:
        """Return the live storage, or default when inactive, else raise LookupError."""
        value = self.storage
        if value is not UNSET:
            return value
        if default is _MISSING:
            raise LookupError(self.name)
        return default

    def set(self, value: SlotStorage) -> SharedToken:
        """Activate the scope, raising if it's already active."""
        if self.storage is not UNSET:
            raise ScopeActivationError(
                f"Cannot activate shared scope {self.name!r}: already active."
            )
        token = SharedToken(self)
        self.storage = value
        self._current_token = token
        return token

    def reset(self, token: SharedToken) -> None:
        """Deactivate the scope, raising if token isn't the current activation."""
        if token is not self._current_token:
            raise ScopeActivationError(
                f"Cannot deactivate shared scope {self.name!r}: "
                "token is not the current activation."
            )
        self.storage = UNSET
        self._current_token = None


ScopeVar = ContextVar[SlotStorage] | SharedVar
