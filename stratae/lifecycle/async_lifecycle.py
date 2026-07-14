"""
Asynchronous lifecycle scoping to cache the results of function calls based on defined scopes.

This module provides decorators and context managers to handle the async lifecycle of resources
and cache the results of function calls based on specified scopes. It supports both synchronous
and asynchronous functions, including generator functions with automatic cleanup for
resource management.

Key Features:
- Scopes are declared as Scope objects, each choosing its cache isolation:
    - lifecycle = AsyncLifecycle([Scope('application', 'shared'), Scope('request', 'context')])
    - "shared": one cache visible to all tasks while the scope is active, regardless of
      context lineage - the fit for application-wide resources (database pools, config)
      entered once at startup, even when startup runs in a different task than requests.
    - "context": a cache per execution context, backed by a ContextVar, so concurrent
      requests each see their own cache.
- Context managers for managing resource lifetimes.
- `@lifecycle.cache('<scope>')`: Decorator to define the cache scope of a function
    - `@lifecycle.cache('application')`
    - `@lifecycle.cache('request')`
- Automatic caching of function results based on the defined scope.
- Support for synchronous and asynchronous functions, including generators.
- Automatic cleanup of resources when the scope ends.

Usage:
Example:
    lifecycle = AsyncLifecycle([Scope('application', 'shared'), Scope('request', 'context')])

    @lifecycle.cache('application')
    async def get_database_connection() -> Connection:
        # This connection will be cached for the application scope
        return await create_connection()

    @lifecycle.cache('request')
    async def get_request_session() -> AsyncGenerator[Session, None]:
        session = await create_session()
        try:
            yield session  # This session will be cached for the request scope
        finally:
            await session.close()

    async with lifecycle.start('application'):
        async with lifecycle.start('request'):
            connection = await get_database_connection()
            session = await get_request_session()
"""

from contextvars import Token
from typing import Callable, Hashable, Sequence

from stratae.lifecycle._context import AsyncLifecycleContext
from stratae.lifecycle._decorators import AsyncCacheDecorator
from stratae.lifecycle._scope import (
    UNSET,
    AsyncExitStack,
    SharedToken,
    SlotDict,
    SlotStorage,
)
from stratae.lifecycle.exceptions import (
    ScopeActivationError,
    ScopeInactiveError,
    ScopeNotFoundError,
)
from stratae.lifecycle.lifecycle import BaseLifecycle
from stratae.lifecycle.scope import Scope


class AsyncLifecycle(BaseLifecycle):
    """Manager for handling lifecycle contexts."""

    def __init__(self, scopes: Sequence[Scope]) -> None:
        """Initialize async lifecycle state using async context."""
        super().__init__(scopes, AsyncLifecycleContext)

    def push(self, scope: str) -> Token[SlotStorage] | SharedToken:
        """Push a new lifecycle scope activation, returning the token pop() takes."""
        try:
            return self._vars[scope].set(self._templates[scope].copy())
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

    async def pop(self, token: Token[SlotStorage] | SharedToken) -> None:
        """
        Asynchronously pop the lifecycle scope activation identified by the token.

        Under LIFO push/pop discipline, get() always returns the token's own activation;
        a reused or cross-context ContextVar token surfaces contextvars' own error from
        reset(), while a shared scope's token simply clears its var. Only awaits when the
        activation actually created an exit stack.
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
        """Create a decorator to set the lifecycle scope for caching function results."""
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return AsyncCacheDecorator(scope, self, cache_key, ignore_params)

    def start(self, scope: str) -> AsyncLifecycleContext:
        """
        Get a scope context by name for use as an async context manager.

        Shared scopes return the same reusable AsyncLifecycleContext instance on every
        call - their activations don't nest, so sharing one per scope skips an allocation
        per activation. Context-isolated scopes get a fresh AsyncLifecycleContext with
        the scope's ContextVar and slot template pre-resolved, so its enter/exit run with
        zero dict lookups and no method call back into this class. The dict lookups
        double as scope validation.
        """
        ctx = self._contexts.get(scope)
        if ctx is not None:
            return ctx
        try:
            return AsyncLifecycleContext(self._vars[scope], self._templates[scope])
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

    def is_empty(self) -> bool:
        """Check if there are no active scopes, introspection only."""
        return all(var.get(UNSET) is UNSET for var in self._vars.values())

    def active_scopes(self) -> Sequence[str]:
        """Get a list of active scopes, in declaration order."""
        return [name for name in self._scopes if self._is_active(name)]

    def _is_active(self, scope: str) -> bool:
        """Whether the scope has a live activation in the calling context."""
        return self._vars[scope].get(UNSET) is not UNSET

    def _scope_error(self, scope: str) -> ScopeNotFoundError | ScopeInactiveError:
        """Build the exception for a failed scope lookup - only ever called off the hot path."""
        if scope not in self._scopes:
            return ScopeNotFoundError(f"Unknown scope: {scope}")
        return ScopeInactiveError(f"Scope '{scope}' is not active.")

    def get_exit_stack(self, scope: str) -> AsyncExitStack:
        """Get the exit stack for the specified lifecycle scope, creating it on first use."""
        slots = self.get_slots(scope)
        stack = slots[0]
        if stack is UNSET:
            stack = slots[0] = AsyncExitStack()
        return stack

    def allocate_slot(self, scope: str) -> int:
        """
        Allocate a dedicated slot for a cached function - a value directly, or a dict.

        Every scope draws from its free-slot stack first, if it isn't empty (see
        release_slot), before minting a new index. Dense-backed scopes that do mint a new
        index grow their template by one slot and index by position, and also grow the
        currently visible activation's copy in place if one exists - the shared copy, or
        the calling context's; copies live in other execution contexts are unreachable -
        so a slot allocated mid-activation is visible without waiting for the next
        push(). Sparse-backed
        scopes hand out the next int key from a per-scope counter instead -
        SlotDict.__missing__ means the key needn't exist anywhere until first written.
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
        """Return a scope's slot to the free pool once its owning wrapper is gone."""
        active = self._vars[scope].get(UNSET)
        if isinstance(active, list):
            active[slot] = UNSET
        elif active is not UNSET:
            active.pop(slot, None)
        self._free_slots[scope].append(slot)

    def get_slots(self, scope: str) -> SlotStorage:
        """
        Get the scope's slot list - slot 0 is reserved for the exit stack (see __init__).

        An unknown scope's KeyError is a LookupError too, so one except distinguishes
        the two failures through _scope_error.
        """
        try:
            return self._vars[scope].get()
        except LookupError:
            raise self._scope_error(scope) from None

    @staticmethod
    def exit_stack_type():
        """Return the exit stack type for codegen lazily initiating exit stacks."""
        return AsyncExitStack
