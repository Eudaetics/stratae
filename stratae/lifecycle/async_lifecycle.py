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
from typing import Callable, Hashable, Sequence, TypedDict

from stratae.cache import Cache, MemoryCache
from stratae.lifecycle._context import AsyncLifecycleContext
from stratae.lifecycle._decorators import AsyncCacheDecorator
from stratae.lifecycle._scope import AsyncActiveScope
from stratae.lifecycle._validation import validate_config
from stratae.lifecycle.exceptions import (
    ScopeActivationError,
    ScopeInactiveError,
    ScopeNotFoundError,
)


class _Active(TypedDict):
    """The currently active scope: its token, the scope itself, and the record it sits on top of."""

    token: Token[AsyncActiveScope | None]
    scope: AsyncActiveScope
    previous: "_Active | None"


class AsyncLifecycle:
    """Manager for handling lifecycle contexts."""

    def __init__(self, scopes: Sequence[str], caches: dict[str, type[Cache]] | None = None) -> None:
        """Initialize the LifecycleManager."""
        validate_config(scopes, caches)
        self._scopes: dict[str, int] = {scope: index for index, scope in enumerate(scopes)}
        self._caches = caches or {}
        self._stack: dict[str, ContextVar[AsyncActiveScope | None]] = {
            scope: ContextVar(scope, default=None) for scope in scopes
        }
        self._current: ContextVar[_Active | None] = ContextVar("lifecycle_current", default=None)

    def push(self, scope: str) -> Token[AsyncActiveScope | None]:
        """Push a new lifecycle scope onto the stack."""
        try:
            cur = self._stack[scope]
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

        current = self._current.get()
        if current is not None and self._scopes[current["token"].var.name] >= self._scopes[scope]:
            raise ScopeActivationError(
                f"Cannot push {scope} scope when {current['token'].var.name} is already active."
            )

        scope_obj = AsyncActiveScope(self._caches.get(scope, MemoryCache))
        token = cur.set(scope_obj)
        self._current.set({"token": token, "scope": scope_obj, "previous": current})
        return token

    async def pop(self, token: Token[AsyncActiveScope | None]) -> None:
        """Asynchronously pop the current lifecycle scope from the stack."""
        current = self._current.get()
        if current is None:
            raise ScopeActivationError(f"Cannot pop {token.var.name} while no scopes are active.")
        elif current["token"] is not token:
            active = current["token"].var.name
            raise ScopeActivationError(
                f"Cannot pop {token.var.name} scope while {active} is still active."
            )

        token.var.set(None)
        self._current.set(current["previous"])
        await current["scope"].clear()

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
        return self._current.get() is None

    def active_scopes(self) -> Sequence[str]:
        """Get a list of active scopes, ordered from outermost to innermost."""
        names: list[str] = []
        current = self._current.get()
        while current is not None:
            names.append(current["token"].var.name)
            current = current["previous"]
        return list(reversed(names))

    def get_cache(self, scope: str) -> Cache:
        """Get the cache for the specified lifecycle scope."""
        try:
            active_scope = self._stack[scope]
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

        active = active_scope.get()
        if active is None:
            raise ScopeInactiveError(f"Scope '{scope}' is not active.")
        return active.cache

    def get_exit_stack(self, scope: str) -> AsyncExitStack:
        """Get the exit stack for the specified lifecycle scope."""
        try:
            var = self._stack[scope]
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

        active = var.get()
        if active is None:
            raise ScopeInactiveError(f"Scope '{scope}' is not active.")
        return active.exit_stack
