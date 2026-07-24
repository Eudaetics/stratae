"""
Scope activation and per-scope function-result caching.

{py:class}`BaseLifecycle` implements the scope and slot mechanics shared by
{py:class}`Lifecycle` (sync) and {py:class}`AsyncLifecycle` (async), each constructed from
a sequence of {py:class}`Scope <stratae.lifecycle.scope.Scope>` declarations.
{py:func}`BaseLifecycle.start` returns a context manager (``async with`` for
{py:class}`AsyncLifecycle`) that activates a scope for its duration. ``@lifecycle.cache(scope)``
(via {py:func}`Lifecycle.cache` or {py:func}`AsyncLifecycle.cache`) decorates a function so
its result is cached for the lifetime of that scope's active activation. A
{py:func}`resource <stratae.lifecycle.resource.resource>`/
{py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged function is
entered automatically when cached. Its yielded value is cached in place of the context
manager itself.

````{example} Three scope tiers working together: application, request, and block
```{code-block} python
import asyncio
from itertools import count
from stratae.lifecycle import AsyncLifecycle, Scope

lifecycle = AsyncLifecycle(
    [
        Scope("application", isolation="shared"),
        Scope("request", storage="sparse"),
        Scope("block", storage="sparse"),
    ]
)

class ConnectionPool:
    def __init__(self):
        print("Opening connection pool")

@lifecycle.cache("application")
async def get_pool() -> ConnectionPool:
    return ConnectionPool()

_next_request_id = count(1)

@lifecycle.cache("request")
async def get_request_id() -> int:
    return next(_next_request_id)

_next_block_id = count(1)

@lifecycle.cache("block")
async def get_block_id() -> int:
    return next(_next_block_id)

async def process_order(order_id: int) -> None:
    async with lifecycle.start("block"):
        await get_pool()
        req_id, block_id = await get_request_id(), await get_block_id()
        print(f"request {req_id} block {block_id}: processing {order_id}")

async def log_audit(order_id: int) -> None:
    async with lifecycle.start("block"):
        await get_pool()
        req_id, block_id = await get_request_id(), await get_block_id()
        print(f"request {req_id} block {block_id}: logging {order_id}")

async def handle_order(order_id: int) -> None:
    async with lifecycle.start("request"):
        req_id = await get_request_id()
        print(f"request {req_id}: handling {order_id}")
        await asyncio.gather(process_order(order_id), log_audit(order_id))

async def main() -> None:
    async with lifecycle.start("application"):
        # Starting a scope doesn't cache anything - get_pool() only runs
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

```{note}
Cache keying behaves like `functools.lru_cache`: unless `ignore_params` is set, a
function that takes parameters gets one cached value per distinct set of
arguments (or per `cache_key` result), not one cached value for the whole scope
activation. Calling it again with different arguments computes and caches a
separate value rather than reusing the first one. A function that takes no
parameters has only one possible argument set, so it always uses the same fast
slot path as `ignore_params=True`.
```

```{tip}
If you know a function's result won't actually vary within a scope activation,
even though it still accepts parameters, pass `ignore_params=True`. That caches
the value directly in the function's slot instead of a keyed dict, trading
per-argument caching for a single, faster slot lookup.
```
See {py:class}`BaseLifecycle`, {py:class}`Lifecycle`, and {py:class}`AsyncLifecycle` for
the rest of the module's API.
"""

from contextvars import Token
from typing import Callable, Hashable, Sequence

from stratae.lifecycle._context import AsyncLifecycleContext, LifecycleContext
from stratae.lifecycle._decorators import AsyncCacheDecorator, CacheDecorator
from stratae.lifecycle._scope import (
    UNSET,
    AsyncExitStack,
    ExitStack,
    SharedToken,
    SlotDict,
    SlotStorage,
    build_lifecycle_state,
)
from stratae.lifecycle._validation import validate_config
from stratae.lifecycle.exceptions import (
    ScopeActivationError,
    ScopeInactiveError,
    ScopeNotFoundError,
)
from stratae.lifecycle.scope import Scope


class BaseLifecycle[
    ContextT: (LifecycleContext, AsyncLifecycleContext),
    StackT: (ExitStack, AsyncExitStack),
]:
    """
    Shared scope and slot mechanics behind Lifecycle and AsyncLifecycle.

    See {py:class}`Lifecycle` and {py:class}`AsyncLifecycle` for the concrete, usable
    classes.
    """

    __slots__ = (
        "_scopes",
        "_templates",
        "_vars",
        "_contexts",
        "_counters",
        "_free_slots",
    )

    _context_cls: type[ContextT]
    _exit_stack_cls: type[StackT]

    def __init__(self, scopes: Sequence[Scope]) -> None:
        """
        Initialize the lifecycle manager with its scopes.

        :param scopes: The scopes this manager activates and caches within. Each scope's
            name must be unique.
        :raises LifecycleConfigurationError: If `scopes` is empty, or two scopes share
            the same name.

        """
        validate_config(scopes)
        self._scopes: dict[str, Scope] = {scope.name: scope for scope in scopes}
        state = build_lifecycle_state(scopes, self._context_cls)
        self._templates = state.templates
        self._vars = state.scope_vars
        self._contexts = state.contexts
        self._counters = state.counters
        self._free_slots = state.free_slots

    def push(self, scope: str) -> Token[SlotStorage] | SharedToken:
        """
        Push a new lifecycle scope activation, returning the token pop() takes.

        A lower-level alternative to {py:func}`BaseLifecycle.start`, for callers managing
        activation lifetime outside a ``with`` block.

        :param scope: Name of the scope to activate.
        :returns: A token identifying this activation; pass it to `pop()` to deactivate
            the scope again.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.
        """
        try:
            return self._vars[scope].set(self._templates[scope].copy())
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

    def start(self, scope: str) -> ContextT:
        """
        Get a scope context by name for use as a context manager.

        Shared scopes return the same reusable context instance on every call - their
        activations don't nest, so sharing one per scope skips an allocation per
        activation. Context-isolated scopes get a fresh context with the scope's
        ContextVar and slot template pre-resolved, so its enter/exit run with zero dict
        lookups. The dict lookups double as scope validation.

        :param scope: Name of the scope to activate.
        :returns: A context manager (``async with`` for {py:class}`AsyncLifecycle`) that
            activates the scope on entry and deactivates it on exit, closing any exit
            stack that activation created.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.
        """
        ctx = self._contexts.get(scope)
        if ctx is not None:
            return ctx
        try:
            return self._context_cls(self._vars[scope], self._templates[scope])
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

    def is_empty(self) -> bool:
        """
        Report whether this lifecycle currently has no active scope activations.

        Introspection only.

        :returns: `True` if no scope declared on this lifecycle currently has an active
            activation in the calling context.

        """
        return all(var.get(UNSET) is UNSET for var in self._vars.values())

    def active_scopes(self) -> Sequence[str]:
        """
        Get the names of currently active scopes, in declaration order.

        :returns: The names of scopes with an active activation in the calling context.
        """
        return [name for name in self._scopes if self._is_active(name)]

    def _is_active(self, scope: str) -> bool:
        """Whether the scope has a live activation in the calling context."""
        return self._vars[scope].get(UNSET) is not UNSET

    def _scope_error(self, scope: str) -> ScopeNotFoundError | ScopeInactiveError:
        """Build the exception for a failed scope lookup - only ever called off the hot path."""
        if scope not in self._scopes:
            return ScopeNotFoundError(f"Unknown scope: {scope}")
        return ScopeInactiveError(f"Scope '{scope}' is not active.")

    def get_exit_stack(self, scope: str) -> StackT:
        """
        Get the exit stack for the specified lifecycle scope, creating it on first use.

        :param scope: Name of the scope whose exit stack to fetch.
        :returns: The scope's `ExitStack`/`AsyncExitStack`, closed automatically when the
            scope's current activation deactivates.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.
        :raises ScopeInactiveError: If `scope` has no active activation in the calling
            context.
        """
        slots = self.get_slots(scope)
        stack = slots[0]
        if stack is UNSET:
            stack = slots[0] = self._exit_stack_cls()
        return stack

    def allocate_slot(self, scope: str) -> int:
        """
        Allocate a dedicated slot for a cached function - a value directly, or a dict entry.

        Internal to the cache decorators; not meant to be called directly.

        :param scope: Name of the scope to allocate a slot in.
        :returns: The allocated slot's index/key, to be passed to
            {py:func}`BaseLifecycle.release_slot` once the owning wrapper is garbage
            collected.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.

        """
        try:
            template = self._templates[scope]
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

        if free := self._free_slots.get(scope):
            return free.pop()

        if isinstance(template, SlotDict):
            slot = self._counters[scope]
            self._counters[scope] = slot + 1
            return slot

        template.append(UNSET)
        active = self._vars[scope].get(UNSET)
        if isinstance(active, list):
            active.append(UNSET)
        return len(template) - 1

    def release_slot(self, scope: str, slot: int) -> None:
        """
        Return a scope's slot to the free pool once its owning wrapper is gone.

        :param scope: Name of the scope the slot was allocated in.
        :param slot: The slot index/key returned by {py:func}`BaseLifecycle.allocate_slot`.

        """
        active = self._vars[scope].get(UNSET)
        if isinstance(active, list):
            active[slot] = UNSET
        elif active is not UNSET:
            active.pop(slot, None)
        self._free_slots[scope].append(slot)

    def get_slots(self, scope: str) -> SlotStorage:
        """
        Get the scope's slot storage - slot 0 is reserved for the exit stack.

        :param scope: Name of the scope whose slot storage to fetch.
        :returns: The scope's live slot storage for the current activation.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.
        :raises ScopeInactiveError: If `scope` has no active activation in the calling
            context.
        """
        try:
            return self._vars[scope].get()
        except LookupError:
            raise self._scope_error(scope) from None

    def exit_stack_type(self) -> type[StackT]:
        """
        Return the exit stack type, for codegen that lazily initializes exit stacks.

        :returns: `ExitStack` for a {py:class}`Lifecycle`, `AsyncExitStack` for an
            {py:class}`AsyncLifecycle`.
        """
        return self._exit_stack_cls


class Lifecycle(BaseLifecycle[LifecycleContext, ExitStack]):
    """
    Manager for caching function results within sync scope activations.

    Register scopes at construction, activate one with {py:func}`BaseLifecycle.start` (or
    the lower-level {py:func}`BaseLifecycle.push`/{py:func}`Lifecycle.pop`), and decorate
    functions with ``@lifecycle.cache(scope)`` to cache their results for that activation's
    lifetime.

    See the module docstring for an example.
    """

    __slots__ = ()

    _context_cls = LifecycleContext
    _exit_stack_cls = ExitStack

    def pop(self, token: Token[SlotStorage] | SharedToken) -> None:
        """
        Pop the lifecycle scope activation identified by the token returned from push.

        :param token: The token returned by the matching {py:func}`BaseLifecycle.push` call.
        :raises ScopeActivationError: If the scope identified by `token` is not currently
            active.
        """
        var = token.var
        try:
            slots = var.get()
        except LookupError:
            raise ScopeActivationError(f"Cannot pop {var.name}: scope is not active.") from None
        if isinstance(token, SharedToken):
            token.var.clear()
        else:
            token.var.reset(token)
        stack = slots[0]
        if stack is not UNSET:
            stack.close()

    def cache(
        self,
        scope: str,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> CacheDecorator:
        """
        Create a decorator that caches a function's result within the named scope.

        A {py:func}`resource <stratae.lifecycle.resource.resource>`-tagged function is
        entered automatically. Its yielded value is cached in place of the context manager
        itself, and exited when the owning scope's activation ends.

        :param scope: Name of the scope the decorated function's result is cached in.
        :param cache_key: Callable deriving a hashable cache key from the decorated
            function's arguments. When omitted, the key is the arguments themselves.
            Mutually exclusive with `ignore_params`.
        :param ignore_params: Cache a single value per scope activation regardless of
            arguments, instead of keying by argument values. Mutually exclusive with
            `cache_key`.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises ValueError: If both `cache_key` and `ignore_params` are given.

        See the module docstring for cache-keying semantics, when to use
        `ignore_params`, and an example.
        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return CacheDecorator(scope, self, cache_key, ignore_params)


class AsyncLifecycle(BaseLifecycle[AsyncLifecycleContext, AsyncExitStack]):
    """
    Manager for caching function results within async scope activations.

    Register scopes at construction, activate one with
    ``async with lifecycle.start(scope):`` (or the lower-level
    {py:func}`BaseLifecycle.push`/{py:func}`AsyncLifecycle.pop`), and decorate functions
    with ``@lifecycle.cache(scope)`` to cache their results for that activation's lifetime.
    Accepts sync functions, async functions, and
    {py:func}`resource <stratae.lifecycle.resource.resource>`/
    {py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged context
    managers of either flavor.

    See the module docstring for an example (sync there, but the same caching and
    `ignore_params` semantics apply with `async`/`await`).
    """

    __slots__ = ()

    _context_cls = AsyncLifecycleContext
    _exit_stack_cls = AsyncExitStack

    async def pop(self, token: Token[SlotStorage] | SharedToken) -> None:
        """
        Asynchronously pop the scope activation identified by the token returned from push.

        :param token: The token returned by the matching {py:func}`BaseLifecycle.push` call.
        :raises ScopeActivationError: If the scope identified by `token` is not currently
            active.
        """
        var = token.var
        try:
            slots = var.get()
        except LookupError:
            raise ScopeActivationError(f"Cannot pop {var.name}: scope is not active.") from None
        if isinstance(token, SharedToken):
            token.var.clear()
        else:
            token.var.reset(token)
        stack = slots[0]
        if stack is not UNSET:
            await stack.aclose()

    def cache(
        self,
        scope: str,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> AsyncCacheDecorator:
        """
        Create a decorator that caches a function's result within the named scope.

        Accepts sync functions, async functions, and
        {py:func}`resource <stratae.lifecycle.resource.resource>`/
        {py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged
        context managers of either flavor. A tagged function is entered automatically. Its
        yielded value is cached in place of the context manager itself, and exited when the
        owning scope's activation ends.

        :param scope: Name of the scope the decorated function's result is cached in.
        :param cache_key: Callable deriving a hashable cache key from the decorated
            function's arguments. When omitted, the key is the arguments themselves.
            Mutually exclusive with `ignore_params`.
        :param ignore_params: Cache a single value per scope activation regardless of
            arguments, instead of keying by argument values. Mutually exclusive with
            `cache_key`.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises ValueError: If both `cache_key` and `ignore_params` are given.

        See the module docstring for cache-keying semantics, when to use
        `ignore_params`, and an example. See
        {py:func}`async_resource <stratae.lifecycle.resource.async_resource>` for its
        own auto-entry example.
        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return AsyncCacheDecorator(scope, self, cache_key, ignore_params)
