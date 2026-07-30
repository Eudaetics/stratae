"""
Scope activation and per-scope function-result caching.

{py:class}`Scope` (sync) and {py:class}`AsyncScope` (async) hold the isolation, storage,
and cached results for one scope. {py:meth}`Scope.activate` returns a context manager
(`async with` for {py:class}`AsyncScope`) that activates the scope for its duration.
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

A scope can declare another scope as a parent with `requires`. {py:meth}`Scope.activate`
raises immediately unless that scope is already active.

````{example} Three scope tiers working together: application, request, and block
```{code-block} python
import asyncio
from itertools import count
from stratae.lifecycle._scope2 import AsyncScope

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
    async with block.activate():
        await get_pool()
        req_id, block_id = await get_request_id(), await get_block_id()
        print(f"request {req_id} block {block_id}: processing {order_id}")

async def log_audit(order_id: int) -> None:
    async with block.activate():
        await get_pool()
        req_id, block_id = await get_request_id(), await get_block_id()
        print(f"request {req_id} block {block_id}: logging {order_id}")

async def handle_order(order_id: int) -> None:
    async with request.activate():
        req_id = await get_request_id()
        print(f"request {req_id}: handling {order_id}")
        await asyncio.gather(process_order(order_id), log_audit(order_id))

async def main() -> None:
    async with application.activate():
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

from contextvars import ContextVar
from types import TracebackType
from typing import Any, Callable, Hashable, get_args

from stratae.lifecycle._decorators2 import AsyncCacheDecorator, CacheDecorator
from stratae.lifecycle._slots import UNSET, ScopeVar, SharedVar, SlotDict, SlotStorage
from stratae.lifecycle._stack import AsyncExitStack, ExitStack
from stratae.lifecycle.exceptions import (
    LifecycleConfigurationError,
    ScopeActivationError,
    ScopeInactiveError,
)
from stratae.lifecycle.scope import IsolationType, StorageType


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
    )

    _exit_stack_cls: type[ExitStack] | type[AsyncExitStack]
    _activation_cls: Callable[..., Any]
    _isolation: IsolationType
    _storage: StorageType
    _requires: "BaseScope | None"
    _var: ScopeVar
    _template: SlotStorage
    _counter: int
    _free_slots: list[int]

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
        self._template = [UNSET, 0] if storage == "dense" else SlotDict({1: 0})
        self._var = ContextVar(name) if isolation == "context" else SharedVar(name)
        self._counter = 2
        self._free_slots = []

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
        return self._var.get(UNSET) is not UNSET

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

        :returns: The scope's live slot storage.
        :raises ScopeInactiveError: If the scope has no active activation in the calling
            context.
        """
        try:
            return self._var.get()
        except LookupError:
            raise ScopeInactiveError(f"Scope {self.name!r} is not active.") from None

    def exit_stack_type(self) -> type[ExitStack] | type[AsyncExitStack]:
        """
        Return this scope's exit stack type, for codegen that lazily initializes exit stacks.

        :returns: `ExitStack` for a `Scope`, `AsyncExitStack` for an `AsyncScope`.
        """
        return self._exit_stack_cls

    def allocate_slot(self) -> int:
        """
        Allocate a dedicated slot for a cached function - a value directly, or a dict entry.

        Internal to the cache decorators; not meant to be called directly. Slots 0 and 1
        are reserved (the exit stack and the live-dependent count), so the first allocated
        slot is 2.

        :returns: The allocated slot's index/key, to be passed to
            {py:meth}`BaseScope.release_slot` once the owning wrapper is garbage collected.
        """
        if free := self._free_slots:
            return free.pop()
        if isinstance(self._template, SlotDict):
            slot = self._counter
            self._counter = slot + 1
            return slot
        self._template.append(UNSET)
        active = self._var.get(UNSET)
        if isinstance(active, list):
            active.append(UNSET)
        return len(self._template) - 1

    def release_slot(self, slot: int) -> None:
        """
        Return a slot to the free pool once its owning cache wrapper is gone.

        :param slot: The slot index/key returned by {py:meth}`BaseScope.allocate_slot`.
        """
        active = self._var.get(UNSET)
        if isinstance(active, list):
            active[slot] = UNSET
        elif active is not UNSET:
            active.pop(slot, None)
        self._free_slots.append(slot)


class Activation:
    """
    The token `Scope.activate()` returns - use as `with`, or pass to `deactivate()`.

    Returned by {py:meth}`Scope.activate`, not constructed directly. Used for both dense
    and sparse storage - slot 1 (the live-dependent count) is always present in either
    case, so no separate sparse-storage variant is needed.
    """

    __slots__ = ("var", "token", "slots", "parent_slots")

    slots: Any
    """The activation's live slot storage, declared `Any` rather than `SlotStorage`."""

    def __init__(
        self,
        var: ScopeVar,
        token: Any,
        slots: SlotStorage,
        parent_slots: SlotStorage | None,
    ) -> None:
        self.var = var
        self.token = token
        self.slots = slots
        self.parent_slots = parent_slots

    def __enter__(self) -> "Activation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc: Exception | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """
        Deactivate the scope this token belongs to.

        :raises ScopeActivationError: If a scope requiring this one is still active.
        """
        slots = self.slots
        if slots[1] > 0:
            raise ScopeActivationError(
                f"Cannot deactivate {self.var.name!r}: a scope requiring it is still active."
            )
        self.var.reset(self.token)
        parent_slots = self.parent_slots
        try:
            stack = slots[0]
            if stack is not UNSET:
                return stack.close(exc)
            return None
        finally:
            if parent_slots is not None:
                parent_slots[1] -= 1


class AsyncActivation:
    """
    The token `AsyncScope.activate()` returns - `async with`, or to `deactivate()`.

    Returned by {py:meth}`AsyncScope.activate`, not constructed directly. Used for both
    dense and sparse storage - slot 1 (the live-dependent count) is always present in
    either case, so no separate sparse-storage variant is needed.
    """

    __slots__ = ("var", "token", "slots", "parent_slots")

    slots: Any
    """The activation's live slot storage, declared `Any` rather than `SlotStorage`."""

    def __init__(
        self,
        var: ScopeVar,
        token: Any,
        slots: SlotStorage,
        parent_slots: SlotStorage | None,
    ) -> None:
        self.var = var
        self.token = token
        self.slots = slots
        self.parent_slots = parent_slots

    async def __aenter__(self) -> "AsyncActivation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc: Exception | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """
        Deactivate the scope this token belongs to.

        :raises ScopeActivationError: If a scope requiring this one is still active.
        """
        slots = self.slots
        if slots[1] > 0:
            raise ScopeActivationError(
                f"Cannot deactivate {self.var.name!r}: a scope requiring it is still active."
            )
        self.var.reset(self.token)
        parent_slots = self.parent_slots
        try:
            stack = slots[0]
            if stack is not UNSET:
                return await stack.aclose(exc)
            return None
        finally:
            if parent_slots is not None:
                parent_slots[1] -= 1


class Scope(BaseScope):
    """A sync-flavored scope - activated with `with`, caches sync functions."""

    __slots__ = ()

    _exit_stack_cls = ExitStack
    _activation_cls = Activation

    def activate(self) -> Activation:
        """
        Activate this scope, returning a token usable as `with` or passed to `deactivate()`.

        :returns: An `Activation` - enter it directly (`with scope.activate():`) or hold
            onto it and call {py:meth}`Scope.deactivate` manually, for split-callback
            lifecycles where activation and deactivation happen in different functions.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        if requires := self._requires:
            try:
                parent_slots = requires._var.get()
                parent_slots[1] += 1
            except LookupError:
                raise ScopeActivationError(
                    f"Cannot activate {self.name!r}: required scope "
                    f"{requires.name!r} is not active."
                ) from LookupError
        else:
            parent_slots = None
        slots = self._template.copy()
        var = self._var
        token = var.set(slots)
        return self._activation_cls(var, token, slots, parent_slots)

    def deactivate(self, activation: Activation) -> None:
        """
        Deactivate the scope activation identified by the given token.

        :param activation: The `Activation` returned by the matching
            {py:meth}`Scope.activate` call.
        :raises ScopeActivationError: If the scope is not currently active, or a scope
            requiring this one is still active.
        """
        activation.__exit__(None, None, None)

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
    _activation_cls = AsyncActivation

    def activate(self) -> AsyncActivation:
        """
        Activate this scope, returning a token usable as `async with` or passed to `deactivate()`.

        :returns: An `AsyncActivation` - enter it directly (`async with scope.activate():`)
            or hold onto it and call {py:meth}`AsyncScope.deactivate` manually, for
            split-callback lifecycles where activation and deactivation happen in
            different functions.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        if requires := self._requires:
            try:
                parent_slots = requires._var.get()
                parent_slots[1] += 1
            except LookupError:
                raise ScopeActivationError(
                    f"Cannot activate {self.name!r}: required scope "
                    f"{requires.name!r} is not active."
                ) from LookupError
        else:
            parent_slots = None
        slots = self._template.copy()
        var = self._var
        token = var.set(slots)
        return self._activation_cls(var, token, slots, parent_slots)

    async def deactivate(self, activation: AsyncActivation) -> None:
        """
        Deactivate the scope activation identified by the given token.

        :param activation: The `AsyncActivation` returned by the matching
            {py:meth}`AsyncScope.activate` call.
        :raises ScopeActivationError: If the scope is not currently active, or a scope
            requiring this one is still active.
        """
        await activation.__aexit__(None, None, None)

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
