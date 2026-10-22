"""
Scope activation and per-scope function-result caching.

{py:class}`Scope` (sync) and {py:class}`AsyncScope` (async) hold the isolation, storage,
and cached results for one scope. A scope is itself a context manager: `with scope:`
(`async with` for {py:class}`AsyncScope`) activates it for the block's duration. For
split-callback lifecycles, where activation and deactivation happen in different functions,
{py:meth}`Scope.activate` returns a token to pass to {py:meth}`Scope.deactivate` instead.
`@scope.cache()` (via {py:meth}`Scope.cache`/{py:meth}`AsyncScope.cache`) decorates a
function so its result is cached for the lifetime of that scope's active activation. A
{py:func}`resource <stratae.lifecycle.resource.resource>`/
{py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged function is
entered automatically when cached. Its yielded value is cached in place of the context
manager itself.

Each scope chooses its own isolation and storage independently. `isolation="shared"`
uses one cache visible to every thread and task while the scope is active. Use it for
application-wide state like a connection pool. `isolation="context"` (the default)
isolates the cache per execution context through a `contextvars.ContextVar`. Use it for
request- or session-scoped state. `storage="dense"` (the default) indexes slots directly
by position. `storage="sparse"` allocates slots lazily. Use sparse storage for a scope
that registers many functions but touches only a handful per activation.

A shared scope computes each cached entry once, even when several threads or tasks miss it
at the same time. Later callers wait for that first computation and reuse its result, or
compute it themselves if it raised. A computation may call other cached functions, including
its own function with different arguments. A call that would wait on its own unfinished
computation, such as a no-parameter function calling itself, raises
{py:exc}`CacheCycleError <stratae.lifecycle.exceptions.CacheCycleError>` instead of
deadlocking. Only a call from the thread or task doing the computation is detected: a
computation that waits on another task or thread which needs that same entry, such as a
child task it awaits, deadlocks.

A context scope's activation is shared by the tasks started inside it, since they inherit
its context. When those tasks miss the same entry of an async cached function or
`async_resource` at once, it's computed once, with the same waiting, retrying, and
{py:exc}`CacheCycleError <stratae.lifecycle.exceptions.CacheCycleError>` rules as a shared
scope. A sync cached function can't be interrupted by another task part-way through, so it
isn't guarded this way; only threads running a copied context could overlap it. A task or
copied context that outlives the activation can't keep using it: its cached calls raise
{py:exc}`ScopeInactiveError <stratae.lifecycle.exceptions.ScopeInactiveError>`, and
{py:meth}`BaseScope.is_active` reports `False` there.

A scope can declare another scope as a parent with `requires`. Activating it raises
immediately unless that scope is already active.

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
        # Activating a scope doesn't eagerly cache anything. get_pool() runs
        # on its first real call, inside process_order below.
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

See {py:class}`BaseScope`, {py:class}`Scope`, and {py:class}`AsyncScope` for the rest of
the module's API.
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
        Initialize the scope's identity, isolation, and storage, and build its state.

        :param name: Identifier for the scope (e.g. `"request"`, `"application"`). Must
            be a valid Python identifier.
        :param isolation: Cache isolation strategy for this scope, one of the
            {py:data}`IsolationType` values. `"shared"` uses a single cache visible to
            every concurrent task/thread while the scope is active. `"context"` (the
            default) isolates the cache per execution context, backed by a
            `contextvars.ContextVar`.
        :param storage: Slot storage strategy for this scope's cached values, one of the
            {py:data}`StorageType` values. `"dense"` (the default) indexes slots directly
            by position. `"sparse"` allocates slots lazily.
        :param requires: The scope that must be active before this one can activate, or
            `None` for no such requirement. A `"shared"` scope cannot require a
            `"context"`-isolated scope: a context scope's activity is per-execution-context,
            so there's no single active/inactive answer for a shared scope's concurrent
            callers to all rely on.
        :raises LifecycleConfigurationError: If `isolation`/`storage` is not one of their
            allowed values, or this scope is `"shared"` while `requires` is a
            `"context"`-isolated scope.

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

    def is_active(self) -> bool:
        """Whether this scope has a live activation in the calling context."""
        # Live storage is never empty; an ended activation's storage is emptied.
        return bool(self._var.get(None))

    def is_shared(self) -> bool:
        """Whether this scope's activation holder is a SharedVar, needing lock-guarded access."""
        return isinstance(self._var, SharedVar)

    def activation_var(self) -> ScopeVar:
        """
        Return the scope's raw activation holder - a ContextVar or SharedVar.

        :returns: The scope's `ContextVar`/`SharedVar`.
        """
        return self._var

    def get_slots(self) -> SlotStorage:
        """
        Get this scope's live slot storage for the current activation.

        Dense storage is first brought up to date with any slots allocated since the
        activation started: an allocation only extends the activation live in the
        allocating context, so another context's activation is missing those slots until
        a cached wrapper's read of one fails and falls back here.

        :returns: The scope's live slot storage.
        :raises ScopeInactiveError: If the scope has no active activation in the calling
            context.
        """
        try:
            slots = self._var.get()
        except LookupError:
            raise ScopeInactiveError(f"Scope {self.name!r} is not active.") from None
        if not slots:
            # Emptied when its activation ended - reached from a task that copied the
            # context while the activation was live.
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

        The activation's own state - its reset token and its parent's storage - is kept in
        reserved slots of that storage, so ending it needs nothing but the storage itself.

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

        :param expected: The storage of the activation a token is ending, which must be the
            current one, or `None` to end whichever is current.
        :returns: The ended activation's storage, still to be closed and released.
        :raises ScopeActivationError: If the scope isn't active, the expected activation already
            ended or isn't the current one, or a scope requiring it is still active, or it's
            deactivated outside the context that activated it.
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
        """
        Return this scope's exit stack type.

        :returns: `ExitStack` for a `Scope`, `AsyncExitStack` for an `AsyncScope`.
        """
        return self._exit_stack_cls

    def allocate_slot(self) -> tuple[int, bool]:
        """
        Allocate a dedicated slot for a cached function - a value directly, or a dict entry.

        Internal to the cache decorators; not meant to be called directly. Slots 0 through 3
        are reserved (the exit stack, the live-dependent count, the reset token, and the
        parent's storage), so the first allocated slot is 4.

        :returns: The allocated slot's index/key, to be passed to
            {py:meth}`BaseScope.release_slot` once the owning wrapper is garbage collected,
            and whether it was reused from the free pool.
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

        Only the calling context's activation is cleared; a reused slot's next wrapper
        ignores what other activations still hold.

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
            # acquire/release rather than `with`: about half the cost on a hot path.
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
    The token `Scope.activate()` returns - pass to `deactivate()`, or use as `with`.

    Returned by {py:meth}`Scope.activate`, not constructed directly. Only needed for
    split-callback lifecycles; `with scope:` activates without one.
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
        """
        Deactivate the scope this token belongs to.

        :raises ScopeActivationError: If it's already deactivated, a scope requiring this one
            is still active, or it's deactivated outside the context that activated it.
        """
        return self.scope.deactivate(self, exc)


class AsyncActivation:
    """
    The token `AsyncScope.activate()` returns - pass to `deactivate()`, or `async with`.

    Returned by {py:meth}`AsyncScope.activate`, not constructed directly. Only needed for
    split-callback lifecycles; `async with scope:` activates without one.
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
        """
        Deactivate the scope this token belongs to.

        :raises ScopeActivationError: If it's already deactivated, a scope requiring this one
            is still active, or it's deactivated outside the context that activated it.
        """
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
        """
        Deactivate this scope's current activation, closing its entered resources.

        :raises ScopeActivationError: If the scope isn't active, a scope requiring it is still
            active, or it's deactivated outside the context that activated it.
        """
        # The body of deactivate, written out: `with scope:` is the hot path.
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

        For a plain block, use `with scope:` instead - it needs no token, and is faster.

        :returns: An `Activation` to hold onto and pass to {py:meth}`Scope.deactivate`, for
            split-callback lifecycles where activation and deactivation happen in different
            functions. It can also be used as `with`.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        return Activation(self, self._activate())

    def deactivate(
        self, activation: Activation | None = None, exc: BaseException | None = None
    ) -> bool:
        """
        Deactivate an activation of this scope, closing the resources entered within it.

        :param activation: The `Activation` returned by the matching {py:meth}`Scope.activate`
            call, which must be the current one, or `None` to end the current activation.
        :param exc: The exception ending the activation, if any, handed to each resource's
            cleanup as `with` would.
        :returns: Whether `exc` was suppressed by a resource's cleanup.
        :raises ScopeActivationError: If the scope isn't active, `activation` already ended or
            isn't the current one, or a scope requiring this one is still active.
        """
        slots = self._end(None if activation is None else activation.slots)
        try:
            stack = slots[0]
            if stack.contexts:
                return stack.close(exc)
            # Nothing entered means nothing to close or suppress - what close would return.
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

        A {py:func}`resource <stratae.lifecycle.resource.resource>`-tagged function is
        entered automatically. Its yielded value is cached in place of the context manager
        itself, and exited when this scope's activation ends.

        ```{note}
        Cache keying behaves like `functools.lru_cache`: unless `ignore_params` is set, a
        function that takes parameters gets one cached value per distinct set of arguments (or
        per `cache_key` result), not one cached value for the whole scope activation. Calling it
        again with different arguments computes and caches a separate value rather than reusing
        the first one. A function that takes no parameters has only one possible argument set,
        so it always uses the same fast slot path as `ignore_params=True`.
        ```

        ```{tip}
        If you know a function's result won't actually vary within a scope activation, even
        though it still accepts parameters, pass `ignore_params=True`. That caches the value
        directly in the function's slot instead of a keyed dict, trading per-argument caching
        for a single, faster slot lookup.
        ```

        :param cache_key: Callable deriving a hashable cache key from the decorated
            function's arguments. When omitted, the key is the arguments themselves.
            Mutually exclusive with `ignore_params`.
        :param ignore_params: Cache a single value per scope activation regardless of
            arguments, instead of keying by argument values. Mutually exclusive with
            `cache_key`.
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
        """
        Deactivate this scope's current activation, closing its entered resources.

        :raises ScopeActivationError: If the scope isn't active, a scope requiring it is still
            active, or it's deactivated outside the context that activated it.
        """
        # The body of deactivate, written out: `async with scope:` is the hot path, and
        # awaiting deactivate would cost a coroutine.
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

        For a plain block, use `async with scope:` instead - it needs no token, and is faster.

        :returns: An `AsyncActivation` to hold onto and pass to
            {py:meth}`AsyncScope.deactivate`, for split-callback lifecycles where activation
            and deactivation happen in different functions. It can also be used as `async with`.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        return AsyncActivation(self, self._activate())

    async def deactivate(
        self, activation: AsyncActivation | None = None, exc: BaseException | None = None
    ) -> bool:
        """
        Deactivate an activation of this scope, closing the resources entered within it.

        :param activation: The `AsyncActivation` returned by the matching
            {py:meth}`AsyncScope.activate` call, which must be the current one, or `None` to
            end the current activation.
        :param exc: The exception ending the activation, if any, handed to each resource's
            cleanup as `async with` would.
        :returns: Whether `exc` was suppressed by a resource's cleanup.
        :raises ScopeActivationError: If the scope isn't active, `activation` already ended or
            isn't the current one, or a scope requiring this one is still active.
        """
        slots = self._end(None if activation is None else activation.slots)
        try:
            stack = slots[0]
            if stack.contexts:
                return await stack.aclose(exc)
            # Nothing entered means nothing to close or suppress - what aclose would return,
            # without the coroutine.
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

        Accepts sync functions, async functions, and
        {py:func}`resource <stratae.lifecycle.resource.resource>`/
        {py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged
        context managers of either flavor. A tagged function is entered automatically. Its
        yielded value is cached in place of the context manager itself, and exited when
        this scope's activation ends.

        :param cache_key: Callable deriving a hashable cache key from the decorated
            function's arguments. When omitted, the key is the arguments themselves.
            Mutually exclusive with `ignore_params`.
        :param ignore_params: Cache a single value per scope activation regardless of
            arguments, instead of keying by argument values. Mutually exclusive with
            `cache_key`.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises ValueError: If both `cache_key` and `ignore_params` are given.
        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return AsyncCacheDecorator(self, cache_key, ignore_params)
