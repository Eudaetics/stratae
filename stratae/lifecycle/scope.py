"""
Scope activation and per-scope function-result caching.

{py:class}`Scope` and {py:class}`AsyncScope` hold the cached results for one scope.
Activate one with `with scope:` (`async with` for {py:class}`AsyncScope`), or with
{py:meth}`Scope.activate` and {py:meth}`Scope.deactivate` when the two happen in different
callbacks. `@scope.cache()` caches a function's result for the active activation. A
{py:func}`resource <stratae.lifecycle.resource.resource>`-tagged function is entered when
cached, and its yielded value is cached in its place.

`isolation="shared"` uses one cache across all threads and tasks, for application-wide
state. `isolation="context"` (the default) gives each execution context its own cache, for
request-scoped state. `storage="sparse"` allocates slots lazily, for scopes that register
many functions but use few per activation.

A shared scope computes each cached entry once, even under concurrent misses; later callers
wait for it, or recompute if it raised. A call that would wait on its own unfinished
computation raises {py:exc}`CacheCycleError <stratae.lifecycle.exceptions.CacheCycleError>`.
Async cached functions in a context scope get the same treatment across tasks sharing its
activation. A task that outlives the activation gets
{py:exc}`ScopeInactiveError <stratae.lifecycle.exceptions.ScopeInactiveError>`.

A scope can require a parent scope with `requires`; activating it raises unless the parent
is active.

````{example} Three scope tiers working together: application, request, and block
```{code-block} python
import asyncio
from itertools import count
from stratae.lifecycle import AsyncScope

application = AsyncScope("application", isolation="shared")
request = AsyncScope("request", storage="sparse", requires=application)
block = AsyncScope("block")

class ConnectionPool:
    def __init__(self):
        print("Opening connection pool")

@application.cache()
async def get_pool() -> ConnectionPool:
    return ConnectionPool()

_next_request_id = count(1)

@request.cache()
async def get_request_id() -> int:
    return next(_next_request_id)

_next_block_id = count(1)

@block.cache()
async def get_block_id() -> int:
    return next(_next_block_id)

async def process_order(order_id: int) -> None:
    async with block:
        await get_pool()
        req_id, block_id = await get_request_id(), await get_block_id()
        print(f"request {req_id} block {block_id}: processing {order_id}")

async def log_audit(order_id: int) -> None:
    async with block:
        await get_pool()
        req_id, block_id = await get_request_id(), await get_block_id()
        print(f"request {req_id} block {block_id}: logging {order_id}")

async def handle_order(order_id: int) -> None:
    async with request:
        req_id = await get_request_id()
        print(f"request {req_id}: handling {order_id}")
        await asyncio.gather(process_order(order_id), log_audit(order_id))

async def main() -> None:
    async with application:
        await handle_order(101)
        await handle_order(102)

asyncio.run(main())
```
```{output}
request 1: handling 101
Opening connection pool
request 1 block 1: processing 101
request 1 block 2: logging 101
request 2: handling 102
request 2 block 3: processing 102
request 2 block 4: logging 102
```
````
"""

import threading
from contextvars import ContextVar
from types import TracebackType
from typing import Any, Callable, Hashable, Literal, NoReturn, cast, get_args

from stratae.lifecycle._decorators import AsyncCacheDecorator, CacheDecorator
from stratae.lifecycle._slots import UNSET, ScopeVar, SharedVar, SlotDict, SlotStorage
from stratae.lifecycle._stack import AsyncExitStack, ExitStack
from stratae.lifecycle.exceptions import (
    LifecycleConfigurationError,
    ScopeActivationError,
    ScopeInactiveError,
)

_RESERVED_SLOTS = 4
"""Slots 0-3 hold the exit stack, live-dependent count, reset token, and parent's storage."""

IsolationType = Literal["shared", "context"]
StorageType = Literal["dense", "sparse"]


def _validate_types(name: str, isolation: IsolationType, storage: StorageType) -> None:
    if isolation not in frozenset(get_args(IsolationType)):
        raise LifecycleConfigurationError(f"Invalid scope isolation given for {name}.")
    if storage not in frozenset(get_args(StorageType)):
        raise LifecycleConfigurationError(f"Invalid scope storage given for {name}.")


def _validate_requires(name: str, isolation: IsolationType, requires: "BaseScope | None") -> None:
    if requires and isolation == "shared" and requires.isolation == "context":
        raise LifecycleConfigurationError(
            f"Shared scope {name!r} cannot require context-isolated scope {requires.name!r}."
        )


class BaseScope:
    """
    Shared state and validation behind Scope and AsyncScope.

    See {py:class}`Scope` and {py:class}`AsyncScope` for the concrete, usable classes.
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
        "_slot_lock",
        "_lock",
        "_parent_lock",
    )

    _exit_stack_cls: type[ExitStack] | type[AsyncExitStack]
    _isolation: IsolationType
    _storage: StorageType
    _requires: "BaseScope | None"
    _var: ScopeVar
    _template: SlotStorage
    _counter: int
    _free_slots: list[int]
    _slot_lock: threading.RLock
    _lock: threading.Lock | None
    _parent_lock: threading.Lock | None

    def __init__(
        self,
        name: str,
        isolation: IsolationType = "context",
        storage: StorageType = "dense",
        requires: "BaseScope | None" = None,
    ) -> None:
        """
        Create a scope.

        :param name: Identifier for the scope, e.g. `"request"`.
        :param isolation: `"shared"` for one cache across all threads and tasks, or
            `"context"` (default) for one per execution context.
        :param storage: `"dense"` (default) for positional slots, or `"sparse"` for lazily
            allocated ones.
        :param requires: A scope that must be active before this one activates. A shared
            scope can't require a context scope.
        :raises LifecycleConfigurationError: If `isolation` or `storage` is invalid, or a
            shared scope requires a context scope.
        """
        _validate_types(name, isolation, storage)
        _validate_requires(name, isolation, requires)

        self.name = name
        self._isolation = isolation
        self._storage = storage
        self._requires = requires
        self._template = (
            [UNSET, 0, None, None] if storage == "dense" else SlotDict({1: 0, 2: None, 3: None})
        )
        self._var = ContextVar(name) if isolation == "context" else SharedVar(name)
        self._counter = _RESERVED_SLOTS
        self._free_slots = []
        self._slot_lock = threading.RLock()
        self._lock = self._var.lock if isinstance(self._var, SharedVar) else None
        self._parent_lock = requires._lock if requires is not None else None

    @property
    def isolation(self) -> IsolationType:
        """Cache isolation strategy for this scope."""
        return self._isolation

    @property
    def storage(self) -> StorageType:
        """Slot storage strategy for this scope."""
        return self._storage

    @property
    def requires(self) -> "BaseScope | None":
        """The scope that must be active before this one can activate, if any."""
        return self._requires

    def is_active(self) -> bool:
        """Whether this scope has a live activation in the calling context."""
        return bool(self._var.get(None))

    def is_shared(self) -> bool:
        """Whether this scope's activation holder is a lock-guarded SharedVar."""
        return isinstance(self._var, SharedVar)

    def activation_var(self) -> ScopeVar:
        """Return the scope's raw `ContextVar` or `SharedVar`."""
        return self._var

    def get_slots(self) -> SlotStorage:
        """
        Get this scope's live slot storage, extended with any slots allocated since activation.

        :returns: The scope's live slot storage.
        :raises ScopeInactiveError: If the scope isn't active in the calling context.
        """
        try:
            slots = self._var.get()
        except LookupError:
            raise ScopeInactiveError(f"Scope {self.name!r} is not active.") from None
        if not slots:
            raise ScopeInactiveError(f"Scope {self.name!r} is not active: its activation ended.")
        self._extend(slots)
        return slots

    def _extend(self, slots: SlotStorage) -> None:
        """Extend live dense storage with any slots allocated since it was copied."""
        template = self._template
        if isinstance(template, list) and isinstance(slots, list) and len(slots) < len(template):
            slots.extend(template[len(slots) :])

    def _activate(self) -> SlotStorage:
        """
        Activate this scope, returning the new activation's storage.

        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        parent_lock = self._parent_lock
        if requires := self._requires:
            if parent_lock is not None:
                parent_lock.acquire()
            try:
                parent_slots = requires._var.get(None)
                if not parent_slots:
                    raise ScopeActivationError(
                        f"Cannot activate {self.name!r}: required scope "
                        f"{requires.name!r} is not active."
                    )
                parent_slots[1] += 1
            finally:
                if parent_lock is not None:
                    parent_lock.release()
        else:
            parent_slots = None
        slots = self._template.copy()
        slots[0] = self._exit_stack_cls()
        slots[3] = parent_slots
        try:
            slots[2] = self._var.set(slots)
        except BaseException:
            if parent_slots is not None:
                # Only a shared scope's set raises, and a shared scope can only require
                # another shared scope, so there's always a parent lock here.
                with cast(threading.Lock, parent_lock):
                    parent_slots[1] -= 1
            raise
        return slots

    def _end(self, expected: SlotStorage | None) -> SlotStorage:
        """
        Check the current activation can end, then reset the scope's activation holder.

        :param expected: The activation's storage, or `None` for the current one.
        :returns: The ended activation's storage, still to be closed and released.
        :raises ScopeActivationError: If the activation can't be ended here.
        """
        var = self._var
        lock = self._lock
        if lock is not None:
            lock.acquire()
        try:
            slots = var.get(None)
            if expected is not None and slots is not expected:
                _raise_not_current(self.name, expected)
            if not slots:
                raise ScopeActivationError(f"Cannot deactivate {self.name!r}: not active.")
            if slots[1] > 0:
                raise ScopeActivationError(
                    f"Cannot deactivate {self.name!r}: a scope requiring it is still active."
                )
            try:
                var.reset(slots[2])
            except ValueError:
                raise ScopeActivationError(
                    f"Cannot deactivate {self.name!r} outside the context that activated it."
                ) from None
            return slots
        finally:
            if lock is not None:
                lock.release()

    def exit_stack_type(self) -> type[ExitStack] | type[AsyncExitStack]:
        """Return this scope's exit stack type."""
        return self._exit_stack_cls

    def allocate_slot(self) -> tuple[int, bool]:
        """
        Allocate a slot for a cached function. Internal to the cache decorators.

        :returns: The slot's index, and whether it was reused from the free pool.
        """
        with self._slot_lock:
            if free := self._free_slots:
                return free.pop(), True
            if isinstance(self._template, SlotDict):
                slot = self._counter
                self._counter = slot + 1
                return slot, False
            self._template.append(UNSET)
            if active := self._var.get(None):
                self._extend(active)
            return len(self._template) - 1, False

    def release_slot(self, slot: int) -> None:
        """
        Return a slot to the free pool once its owning cache wrapper is gone.

        :param slot: The slot index/key returned by {py:meth}`BaseScope.allocate_slot`.
        """
        with self._slot_lock:
            active = self._var.get(None)
            if isinstance(active, list):
                if slot < len(active):
                    active[slot] = UNSET
            elif active is not None:
                active.pop(slot, None)
            self._free_slots.append(slot)


def _release(slots: SlotStorage, parent_lock: threading.Lock | None) -> None:
    """Empty an ended activation's storage and stop counting it against its parent."""
    parent_slots = slots[3]
    slots.clear()
    if parent_slots is not None:
        if parent_lock is None:
            parent_slots[1] -= 1
        else:
            # Faster than `with` on this hot path.
            parent_lock.acquire()
            try:
                parent_slots[1] -= 1
            finally:
                parent_lock.release()


def _raise_not_current(name: str, expected: SlotStorage) -> NoReturn:
    """Raise for a token whose activation isn't the scope's current one."""
    if not expected:
        raise ScopeActivationError(f"Cannot deactivate {name!r}: already deactivated.")
    raise ScopeActivationError(f"Cannot deactivate {name!r} outside the context that activated it.")


class Activation:
    """
    Token for one activation of a sync scope.

    Returned by {py:meth}`Scope.activate`. Pass it to {py:meth}`Scope.deactivate`, or use it
    with `with`.
    """

    __slots__ = ("scope", "slots")

    slots: Any
    """The activation's live slot storage, declared `Any` rather than `SlotStorage`."""

    def __init__(self, scope: "Scope", slots: SlotStorage) -> None:
        """Construct the token for one activation of `scope`."""
        self.scope = scope
        self.slots = slots

    def __enter__(self) -> "Activation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate the scope this token belongs to."""
        return self.scope.deactivate(self, exc)


class AsyncActivation:
    """
    Token for one activation of an async scope.

    Returned by {py:meth}`AsyncScope.activate`. Pass it to {py:meth}`AsyncScope.deactivate`,
    or use it with `async with`.
    """

    __slots__ = ("scope", "slots")

    slots: Any
    """The activation's live slot storage, declared `Any` rather than `SlotStorage`."""

    def __init__(self, scope: "AsyncScope", slots: SlotStorage) -> None:
        """Construct the token for one activation of `scope`."""
        self.scope = scope
        self.slots = slots

    async def __aenter__(self) -> "AsyncActivation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate the scope this token belongs to."""
        return await self.scope.deactivate(self, exc)


class Scope(BaseScope):
    """A sync-flavored scope - activated with `with`, caches sync functions."""

    __slots__ = ()

    _exit_stack_cls = ExitStack

    def __enter__(self) -> None:
        """
        Activate this scope for the duration of the `with` block.

        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        self._activate()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate this scope's current activation, closing its entered resources."""
        # Inlined deactivate: `with scope:` is the hot path.
        slots = self._end(None)
        try:
            stack = slots[0]
            if stack.contexts:
                return stack.close(exc)
            stack.closed = True
            return False
        finally:
            _release(slots, self._parent_lock)

    def activate(self) -> Activation:
        """
        Activate this scope, returning a token to pass to `deactivate()`.

        Prefer `with scope:` for a plain block; it's faster.

        :returns: An {py:class}`Activation` token.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        return Activation(self, self._activate())

    def deactivate(
        self, activation: Activation | None = None, exc: BaseException | None = None
    ) -> bool:
        """
        Deactivate an activation of this scope, closing the resources entered within it.

        :param activation: The token from {py:meth}`Scope.activate`, or `None` for the current
            activation.
        :param exc: The exception ending the activation, passed to each resource's cleanup.
        :returns: Whether `exc` was suppressed.
        :raises ScopeActivationError: If the scope isn't active, `activation` isn't current, or
            a scope requiring this one is still active.
        """
        slots = self._end(None if activation is None else activation.slots)
        try:
            stack = slots[0]
            if stack.contexts:
                return stack.close(exc)
            stack.closed = True
            return False
        finally:
            _release(slots, self._parent_lock)

    def cache(
        self,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> CacheDecorator:
        """
        Create a decorator that caches a function's result within this scope.

        Like `functools.lru_cache`, results are keyed by arguments. A
        {py:func}`resource <stratae.lifecycle.resource.resource>`-tagged function is entered,
        its yielded value cached, and exited when the activation ends.

        ```{tip}
        Pass `ignore_params=True` when the result doesn't vary by argument within an
        activation. It uses a single, faster slot instead of a keyed dict.
        ```

        :param cache_key: Derives a hashable key from the arguments. Defaults to the
            arguments themselves.
        :param ignore_params: Cache one value per activation regardless of arguments.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises ValueError: If both `cache_key` and `ignore_params` are given.
        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return CacheDecorator(self, cache_key, ignore_params)


class AsyncScope(BaseScope):
    """An async-flavored scope - activated with `async with`, caches sync and async functions."""

    __slots__ = ()

    _exit_stack_cls = AsyncExitStack

    async def __aenter__(self) -> None:
        """
        Activate this scope for the duration of the `async with` block.

        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        self._activate()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate this scope's current activation, closing its entered resources."""
        # Inlined deactivate: `async with scope:` is the hot path; this saves a coroutine.
        slots = self._end(None)
        try:
            stack = slots[0]
            if stack.contexts:
                return await stack.aclose(exc)
            stack.closed = True
            return False
        finally:
            _release(slots, self._parent_lock)

    def activate(self) -> AsyncActivation:
        """
        Activate this scope, returning a token to pass to `deactivate()`.

        Prefer `async with scope:` for a plain block; it's faster.

        :returns: An {py:class}`AsyncActivation` token.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        return AsyncActivation(self, self._activate())

    async def deactivate(
        self, activation: AsyncActivation | None = None, exc: BaseException | None = None
    ) -> bool:
        """
        Deactivate an activation of this scope, closing the resources entered within it.

        :param activation: The token from {py:meth}`AsyncScope.activate`, or `None` for the
            current activation.
        :param exc: The exception ending the activation, passed to each resource's cleanup.
        :returns: Whether `exc` was suppressed.
        :raises ScopeActivationError: If the scope isn't active, `activation` isn't current, or
            a scope requiring this one is still active.
        """
        slots = self._end(None if activation is None else activation.slots)
        try:
            stack = slots[0]
            if stack.contexts:
                return await stack.aclose(exc)
            stack.closed = True
            return False
        finally:
            _release(slots, self._parent_lock)

    def cache(
        self,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> AsyncCacheDecorator:
        """
        Create a decorator that caches a function's result within this scope.

        Accepts sync and async functions. A
        {py:func}`resource <stratae.lifecycle.resource.resource>` or
        {py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged function
        is entered, its yielded value cached, and exited when the activation ends.

        :param cache_key: Derives a hashable key from the arguments. Defaults to the
            arguments themselves.
        :param ignore_params: Cache one value per activation regardless of arguments.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises ValueError: If both `cache_key` and `ignore_params` are given.
        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return AsyncCacheDecorator(self, cache_key, ignore_params)
