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

from contextvars import ContextVar, Token
from typing import Any, Callable, Hashable, Sequence

from stratae.lifecycle._context import IsolatedLifecycleContext, SharedLifecycleContext
from stratae.lifecycle._decorators import CacheDecorator
from stratae.lifecycle._scope import UNSET, ExitStack
from stratae.lifecycle._validation import validate_config
from stratae.lifecycle.exceptions import (
    ScopeActivationError,
    ScopeInactiveError,
    ScopeNotFoundError,
)
from stratae.lifecycle.scope import Scope


class Lifecycle:
    """Manager for handling lifecycle contexts."""

    __slots__ = ("_scopes", "_templates", "_cvars", "_shared", "_active", "_contexts")

    def __init__(self, scopes: Sequence[Scope]) -> None:
        """Initialize the LifecycleManager."""
        validate_config(scopes)
        self._scopes: dict[str, Scope] = {scope.name: scope for scope in scopes}
        self._templates: dict[str, list[Any]] = {scope.name: [UNSET] for scope in scopes}
        self._cvars: dict[str, ContextVar[list[Any]]] = {
            scope.name: ContextVar(scope.name) for scope in scopes if scope.isolation == "context"
        }
        self._shared: dict[str, list[Any]] = {
            scope.name: self._templates[scope.name].copy()
            for scope in scopes
            if scope.isolation == "shared"
        }
        self._active: dict[str, list[Any]] = {}
        self._contexts: dict[str, SharedLifecycleContext] = {
            name: SharedLifecycleContext(name, entry, self._active, self._templates[name])
            for name, entry in self._shared.items()
        }

    def push(self, scope: str) -> Token[list[Any]] | str:
        """Push a new lifecycle scope activation, returning the handle pop() takes."""
        if scope in self._cvars:
            return self._cvars[scope].set(self._templates[scope].copy())
        try:
            self._active[scope] = self._shared[scope]
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None
        return scope

    def pop(self, handle: Token[list[Any]] | str) -> None:
        """Pop the lifecycle scope activation identified by handle."""
        if isinstance(handle, str):
            self._pop_shared(handle)
        else:
            self._pop_isolated(handle)

    def _pop_shared(self, scope: str) -> None:
        """Deactivate a shared scope by name."""
        try:
            slots = self._active.pop(scope)
        except KeyError:
            raise self._pop_error(scope) from None
        stack = slots[0]
        slots[:] = self._templates[scope]
        if stack is not UNSET:
            stack.close()

    def _pop_isolated(self, token: Token[list[Any]]) -> None:
        """Deactivate a context-isolated activation by resetting its ContextVar."""
        try:
            slots = token.var.get()
        except LookupError:
            raise ScopeActivationError(
                f"Cannot pop {token.var.name}: scope is not active."
            ) from None
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

    def start(self, scope: str) -> SharedLifecycleContext | IsolatedLifecycleContext:
        """
        Get a scope context by name for use as a context manager.

        Shared scopes return the same reusable SharedLifecycleContext instance on every
        call - they carry no per-activation state, so sharing one per scope skips an
        allocation per activation. Context-isolated scopes get a fresh
        IsolatedLifecycleContext with the scope's ContextVar and slot template
        pre-resolved, so its enter/exit run with zero dict lookups. The dict lookups
        double as scope validation.
        """
        ctx = self._contexts.get(scope)
        if ctx is not None:
            return ctx
        try:
            return IsolatedLifecycleContext(self._cvars[scope], self._templates[scope])
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

    def is_empty(self) -> bool:
        """Check if there are no active scopes, introspection only."""
        return not self._active and all(cv.get(UNSET) is UNSET for cv in self._cvars.values())

    def active_scopes(self) -> Sequence[str]:
        """Get a list of active scopes, in declaration order."""
        return [name for name in self._scopes if self._is_active(name)]

    def _is_active(self, scope: str) -> bool:
        """Whether the scope has a live activation in the calling context."""
        cv = self._cvars.get(scope)
        if cv is not None:
            return cv.get(UNSET) is not UNSET
        return scope in self._active

    def _scope_error(self, scope: str) -> ScopeNotFoundError | ScopeInactiveError:
        """Build the exception for a failed scope lookup - only ever called off the hot path."""
        if scope not in self._scopes:
            return ScopeNotFoundError(f"Unknown scope: {scope}")
        return ScopeInactiveError(f"Scope '{scope}' is not active.")

    def _pop_error(self, scope: str) -> ScopeNotFoundError | ScopeActivationError:
        """Build the exception for a failed by-name pop - only ever called off the hot path."""
        if scope not in self._scopes:
            return ScopeNotFoundError(f"Unknown scope: {scope}")
        if scope in self._cvars:
            return ScopeActivationError(
                f"Cannot pop {scope} by name: context-isolated scopes pop with the token"
                " returned by push()."
            )
        return ScopeActivationError(f"Cannot pop {scope}: scope is not active.")

    def get_exit_stack(self, scope: str) -> ExitStack:
        """Get the exit stack for the specified lifecycle scope, creating it on first use."""
        slots = self.get_slots(scope)
        stack = slots[0]
        if stack is UNSET:
            stack = slots[0] = ExitStack()
        return stack

    def allocate_slot(self, scope: str) -> int:
        """Allocate a dedicated slot for a cached function - a value directly, or a dict."""
        try:
            template = self._templates[scope]
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

        template.append(UNSET)
        shared = self._shared.get(scope)
        if shared is not None:
            shared.append(UNSET)
        return len(template) - 1

    def get_slots(self, scope: str) -> list[Any]:
        """Get the scope's slot list - slot 0 is reserved for the exit stack (see __init__)."""
        cv = self._cvars.get(scope)
        if cv is not None:
            try:
                return cv.get()
            except LookupError:
                raise ScopeInactiveError(f"Scope '{scope}' is not active.") from None
        try:
            return self._active[scope]
        except KeyError:
            raise self._scope_error(scope) from None
