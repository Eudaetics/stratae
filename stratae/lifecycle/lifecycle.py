"""
Lifecycle scoping to cache the results of function calls based on defined scopes.

This module provides decorators and context managers to handle the lifecycle of resources
and cache the results of function calls based on specified scopes. It supports only synchronous
functions, including generator functions with automatic cleanup for resource management.

Key Features:
- Configurable lifecycle scopes using enums.
    - lifecycle = Lifecycle(['application', 'request', 'block'])
- Context managers for managing resource lifetimes.
- `@lifecycle.cache('<scope>')`: Decorator to define the cache scope of a function
    - `@lifecycle.cache('application')`
    - `@lifecycle.cache('request')`
    - `@lifecycle.cache('block')`
- Automatic caching of function results based on the defined scope.
- Support for synchronous functions, including generators.
- Automatic cleanup of resources when the scope ends.

Usage:
Example:
    lifecycle = Lifecycle(['application', 'request', 'block'])

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

from __future__ import annotations

from collections import deque
from contextlib import ExitStack
from typing import TYPE_CHECKING, Callable, Hashable, Sequence

from stratae.cache import MemoryCache
from stratae.lifecycle._context import LifecycleContext
from stratae.lifecycle._decorators import CacheDecorator
from stratae.lifecycle._scope import Scope
from stratae.lifecycle.exceptions import (
    LifecycleConfigurationError,
    ScopeActivationError,
    ScopeInactiveError,
    ScopeNotFoundError,
)

if TYPE_CHECKING:
    from stratae.cache import Cache


class Lifecycle:
    """Manager for handling lifecycle contexts."""

    def __init__(self, scopes: Sequence[str], caches: dict[str, Cache] | None = None) -> None:
        """Initialize the LifecycleManager."""
        if not scopes:
            raise LifecycleConfigurationError("At least one scope must be defined.")
        if any(not scope.isidentifier() for scope in scopes):
            raise LifecycleConfigurationError("All scopes must be valid Python identifiers.")
        if len(set(scopes)) != len(scopes):
            raise LifecycleConfigurationError("All scopes must be unique.")
        if caches and any(key not in scopes for key in caches.keys()):
            raise LifecycleConfigurationError("All caches must correspond to defined scopes.")

        self._caches = caches or {}
        self._scopes: dict[str, int] = {scope: index for index, scope in enumerate(scopes)}
        self._stack: dict[str, Scope] = {
            scope: Scope(cache=self._caches.get(scope, MemoryCache()), exit_stack=ExitStack())
            for scope in scopes
        }
        self._active: deque[str] = deque()

    def push(self, scope: str) -> None:
        """Push a new lifecycle scope onto the stack."""
        if scope not in self._scopes:
            raise ScopeNotFoundError(f"Unknown scope: {scope}")

        current = self._active[-1] if self._active else None
        if current and self._scopes[current] >= self._scopes[scope]:
            raise ScopeActivationError(
                f"Cannot push {scope} scope when {current} is already active."
            )
        self._active.append(scope)

    def pop(self) -> None:
        """Pop the current lifecycle scope from the stack."""
        if not self._active:
            return

        current = self._active.pop()
        self._stack[current].clear()

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
        """Get the current scope context by name for use as a decorator or context manager."""
        if scope not in self._scopes:
            raise ScopeNotFoundError(f"No lifecycle scope named '{scope}'.")

        return LifecycleContext(scope, self)

    def is_empty(self) -> bool:
        """Check if there are no active scopes."""
        return not self._active

    def active_scopes(self) -> Sequence[str]:
        """Get a list of active scopes."""
        return list(self._active)

    def get_cache(self, scope: str) -> Cache:
        """Get the cache for the specified lifecycle scope."""
        if scope not in self._scopes:
            raise ScopeNotFoundError(f"Unknown scope: {scope}")

        if scope not in self._active:
            raise ScopeInactiveError(f"Scope '{scope}' is not active.")

        return self._stack[scope].cache

    def get_exit_stack(self, scope: str) -> ExitStack:
        """Get the exit stack for the specified lifecycle scope."""
        if scope not in self._scopes:
            raise ScopeNotFoundError(f"Unknown scope: {scope}")

        if scope not in self._active:
            raise ScopeInactiveError(f"Scope '{scope}' is not active.")

        return self._stack[scope].exit_stack
