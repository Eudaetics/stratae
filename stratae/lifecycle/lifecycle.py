"""
Lifecycle scoping to cache the results of function calls based on defined scopes.

This module provides decorators and context managers to handle the lifecycle of resources
and cache the results of function calls based on specified scopes. It supports only synchronous
functions, including generator functions with automatic cleanup for resource management.

Key Features:
- Scopes are declared as Scope objects, each choosing its cache isolation:
    - lifecycle = Lifecycle([Scope('application', 'shared'), Scope('request', 'context')])
    - "shared": one cache visible to all threads/contexts while the scope is active.
    - "context": a cache per execution context, backed by a ContextVar.
- Context managers for managing resource lifetimes.
- `@lifecycle.cache('<scope>')`: Decorator to define the cache scope of a function
    - `@lifecycle.cache('application')`
    - `@lifecycle.cache('request')`
- Automatic caching of function results based on the defined scope.
- Support for synchronous functions, including generators.
- Automatic cleanup of resources when the scope ends.

Usage:
Example:
    lifecycle = Lifecycle([Scope('application', 'shared'), Scope('request', 'context')])

    @lifecycle.cache('application')
    def get_database_connection() -> Connection:
        # This connection will be cached for the application scope
        return create_connection()

    @lifecycle.cache('request')
    def get_request_session() -> Generator[Session, None, None]:
        session = create_session()
        try:
            yield session  # This session will be cached for the request scope
        finally:
            session.close()

    with lifecycle.start('application'):
        with lifecycle.start('request'):
            connection = get_database_connection()
            session = get_request_session()
"""

from contextvars import Token
from typing import Callable, Hashable, Sequence

from stratae.lifecycle._context import AsyncLifecycleContext, LifecycleContext
from stratae.lifecycle._decorators import CacheDecorator
from stratae.lifecycle._scope import (
    UNSET,
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


class BaseLifecycle:
    """Shared behavior for sync and async lifecycles."""

    __slots__ = (
        "_scopes",
        "_templates",
        "_vars",
        "_contexts",
        "_counters",
        "_free_slots",
    )

    def __init__(
        self,
        scopes: Sequence[Scope],
        context_cls: type[LifecycleContext] | type[AsyncLifecycleContext],
    ):
        """Initialize the LifecycleManager."""
        validate_config(scopes)
        self._scopes: dict[str, Scope] = {scope.name: scope for scope in scopes}
        state = build_lifecycle_state(scopes, context_cls)
        self._templates = state.templates
        self._vars = state.scope_vars
        self._contexts = state.contexts
        self._counters = state.counters
        self._free_slots = state.free_slots


class Lifecycle(BaseLifecycle):
    """Manager for handling lifecycle contexts."""

    def __init__(self, scopes: Sequence[Scope]) -> None:
        """Initialize lifecycle state using the sync lifecycle context."""
        super().__init__(scopes, LifecycleContext)

    def push(self, scope: str) -> Token[SlotStorage] | SharedToken:
        """Push a new lifecycle scope activation, returning the token pop() takes."""
        try:
            return self._vars[scope].set(self._templates[scope].copy())
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

    def pop(self, token: Token[SlotStorage] | SharedToken) -> None:
        """
        Pop the lifecycle scope activation identified by the token push() returned.

        Under LIFO push/pop discipline, get() always returns the token's own activation;
        a reused or cross-context ContextVar token surfaces contextvars' own error from
        reset(), while a shared scope's token simply clears its var.
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
    ):
        """Create a decorator to set the lifecycle scope for caching function results."""
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return CacheDecorator(scope, self, cache_key, ignore_params)

    def start(self, scope: str) -> LifecycleContext:
        """
        Get a scope context by name for use as a context manager.

        Shared scopes return the same reusable LifecycleContext instance on every call -
        their activations don't nest, so sharing one per scope skips an allocation per
        activation. Context-isolated scopes get a fresh LifecycleContext with the scope's
        ContextVar and slot template pre-resolved, so its enter/exit run with zero dict
        lookups. The dict lookups double as scope validation.
        """
        ctx = self._contexts.get(scope)
        if ctx is not None:
            return ctx
        try:
            return LifecycleContext(self._vars[scope], self._templates[scope])
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

    def get_exit_stack(self, scope: str) -> ExitStack:
        """Get the exit stack for the specified lifecycle scope, creating it on first use."""
        slots = self.get_slots(scope)
        stack = slots[0]
        if stack is UNSET:
            stack = slots[0] = ExitStack()
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
        return ExitStack
