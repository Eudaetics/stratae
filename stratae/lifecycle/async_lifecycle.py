"""
Asynchronous lifecycle scoping to cache the results of function calls based on defined scopes.

This module provides decorators and context managers to handle the async lifecycle of resources
and cache the results of function calls based on specified scopes. It supports both synchronous
and asynchronous functions, including generator functions with automatic cleanup for
resource management.

Key Features:
- Configurable lifecycle scopes using enums.
    - lifecycle = AsyncLifecycle(['application', 'request', 'block'])
- Context managers for managing resource lifetimes.
- `@lifecycle.cache('<scope>')`: Decorator to define the cache scope of a function
    - `@lifecycle.cache('application')`
    - `@lifecycle.cache('request')`
    - `@lifecycle.cache('block')`
- Automatic caching of function results based on the defined scope.
- Support for synchronous and asynchronous functions, including generators.
- Automatic cleanup of resources when the scope ends.

Usage:
Example:
    lifecycle = Lifecycle(['application', 'request', 'block'])

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

from __future__ import annotations

from contextlib import AsyncExitStack
from contextvars import ContextVar, Token
from typing import Callable, Hashable, Sequence

from stratae.cache import Cache, MemoryCache
from stratae.lifecycle._context import AsyncLifecycleContext
from stratae.lifecycle._decorators import AsyncCacheDecorator
from stratae.lifecycle._scope import AsyncScope
from stratae.lifecycle.exceptions import (
    LifecycleConfigurationError,
    ScopeActivationError,
    ScopeInactiveError,
    ScopeNotFoundError,
)


class AsyncLifecycle:
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

        self._scopes: dict[str, int] = {scope: index for index, scope in enumerate(scopes)}
        self._caches = caches or {}
        self._stack: ContextVar[dict[str, AsyncScope]] = ContextVar("lifecycle_stack")
        self._stack.set({})

    def push(self, scope: str) -> Token[dict[str, AsyncScope]]:
        """Push a new lifecycle scope onto the stack."""
        if scope not in self._scopes:
            raise ScopeNotFoundError(f"Unknown scope: {scope}")

        current_stack = dict(self._stack.get())
        current = next(reversed(current_stack), None)
        if current and self._scopes[current] >= self._scopes[scope]:
            raise ScopeActivationError(
                f"Cannot push {scope} scope when {current} is already active."
            )

        current_stack.update(
            {
                scope: AsyncScope(
                    cache=self._caches.get(scope, MemoryCache()), exit_stack=AsyncExitStack()
                )
            }
        )
        return self._stack.set(current_stack)

    async def pop(self, token: Token[dict[str, AsyncScope]]) -> None:
        """Asynchronously pop the current lifecycle scope from the stack."""
        current_stack = dict(self._stack.get())
        if not current_stack:
            return

        popped_scope = next(reversed(current_stack))
        popped = current_stack.pop(popped_scope)
        self._stack.reset(token)
        await popped.clear()

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
        """Start a new lifecycle scope context manager."""
        if scope not in self._scopes:
            raise ScopeNotFoundError(f"No lifecycle scope named '{scope}'.")
        return AsyncLifecycleContext(scope, self)

    def is_empty(self) -> bool:
        """Check if there are no active scopes."""
        return not self._stack.get()

    def active_scopes(self) -> Sequence[str]:
        """Get a list of active scopes."""
        return list(self._stack.get().keys())

    def get_cache(self, scope: str) -> Cache:
        """Get the cache for the specified lifecycle scope."""
        if scope not in self._scopes:
            raise ScopeNotFoundError(f"Unknown scope: {scope}")

        current_scopes = dict(self._stack.get())
        if scope not in current_scopes:
            raise ScopeInactiveError(f"Scope '{scope}' is not active.")
        return current_scopes[scope].cache

    def get_exit_stack(self, scope: str) -> AsyncExitStack:
        """Get the exit stack for the specified lifecycle scope."""
        if scope not in self._scopes:
            raise ScopeNotFoundError(f"Unknown scope: {scope}")

        current_scopes = dict(self._stack.get())
        if scope not in current_scopes:
            raise ScopeInactiveError(f"Scope '{scope}' is not active.")
        return current_scopes[scope].exit_stack
