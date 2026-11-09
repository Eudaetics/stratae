import threading
from contextvars import ContextVar
from functools import partial
from typing import Any

from stratae.lifecycle.exceptions import (
    ActivationEndedError,
    NoActivationError,
    ScopeAlreadyActiveError,
)

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


SlotStorage = list[Any] | SlotDict


class SharedVar:
    """A ContextVar stand-in holding one storage for every context, rejecting re-activation."""

    __slots__ = ("lock", "name", "storage")

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
                raise ScopeAlreadyActiveError(self.name)
            self.storage = value

    def reset(self, _token: None) -> None:
        """Deactivate the scope; the token is unused, kept to match `ContextVar.reset`."""
        self.storage = UNSET


ScopeVar = ContextVar[SlotStorage] | SharedVar


RESERVED_SLOTS = 4
"""
How many slots at the start of every activation's storage hold its own bookkeeping.

- Slot 0: the entered resources, closed when the activation ends. See
  `stratae.lifecycle._stack`.
- Slot 1: how many activations of scopes requiring this one are live.
- Slot 2: the token that resets the scope's activation holder.
- Slot 3: the storage of the activation this one requires, or `None`.

Code reading or writing them uses the bare numbers, each commented with its slot. A named
module constant costs a global lookup per use, and every use is on an activation, a
deactivation or a resource entry.
"""


class SlotStore:
    """
    A scope's slot storage: its activation holder, and the slots its cache wrappers use.

    Internal to the lifecycle package. A scope builds one and hands it to its cache
    decorators, so wrappers never reach into the scope itself. The scope keeps its own
    references to `var`, `new` and `lock` for its activation hot paths.
    """

    __slots__ = (
        "_counter",
        "_free",
        "_slot_lock",
        "dense",
        "lock",
        "name",
        "new",
        "shared",
        "template",
        "threads",
        "var",
    )

    var: ScopeVar
    """The activation holder: a `ContextVar`, or a `SharedVar` for a shared scope."""
    lock: "threading.Lock | None"
    """A shared scope's activation lock; `None` for a context scope."""

    def __init__(self, name: str, *, shared: bool, dense: bool, threads: bool) -> None:
        """
        Create the storage for one scope.

        :param name: The scope's name, for errors.
        :param shared: Whether one activation is shared by every context.
        :param dense: Whether slots are positional, rather than allocated lazily.
        :param threads: Whether threads sharing an activation compute each entry once.
        """
        self.name = name
        self.shared = shared
        self.dense = dense
        self.threads = threads
        self.template: SlotStorage
        if dense:
            template = [UNSET, 0, None, None]  # slots 0-3, see RESERVED_SLOTS
            self.template = template
            self.new = template.copy
        else:
            self.template = SlotDict({1: 0, 2: None, 3: None})  # slots 1-3
            # Built from a plain dict: faster than copying the SlotDict.
            self.new = partial(SlotDict, {1: 0, 2: None, 3: None})
        if shared:
            var = SharedVar(name)
            self.var, self.lock = var, var.lock
        else:
            self.var, self.lock = ContextVar(name), None
        self._counter = RESERVED_SLOTS
        self._free: list[int] = []
        self._slot_lock = threading.RLock()

    def get(self) -> SlotStorage:
        """
        Get the live storage, extended with any slots allocated since activation.

        :raises ScopeInactiveError: If the scope isn't active in the calling context.
        """
        try:
            slots = self.var.get()
        except LookupError:
            raise NoActivationError(self.name) from None
        if not slots:
            raise ActivationEndedError(self.name)
        self._extend(slots)
        return slots

    def _extend(self, slots: SlotStorage) -> None:
        """Extend live dense storage with any slots allocated since it was copied."""
        template = self.template
        if isinstance(template, list) and isinstance(slots, list) and len(slots) < len(template):
            slots.extend(template[len(slots) :])

    def allocate(self) -> tuple[int, bool]:
        """
        Allocate a slot for a cached function.

        :returns: The slot's index, and whether it was reused from the free pool.
        """
        with self._slot_lock:
            if free := self._free:
                return free.pop(), True
            if isinstance(self.template, SlotDict):
                slot = self._counter
                self._counter = slot + 1
                return slot, False
            self.template.append(UNSET)
            if active := self.var.get(None):
                self._extend(active)
            return len(self.template) - 1, False

    def release(self, slot: int) -> None:
        """
        Return a slot to the free pool once its owning cache wrapper is gone.

        :param slot: The slot from {py:meth}`SlotStore.allocate`.
        """
        with self._slot_lock:
            active = self.var.get(None)
            if isinstance(active, list):
                if slot < len(active):
                    active[slot] = UNSET
            elif active is not None:
                active.pop(slot, None)
            self._free.append(slot)
