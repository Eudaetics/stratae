"""Context managers and decorators for lifecycle scopes."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stratae.lifecycle.async_lifecycle import AsyncLifecycle
    from stratae.lifecycle.lifecycle import Lifecycle


class LifecycleContext:
    """
    Defines lifecycle context for functions and context managers.

    Allows decorating functions to specify their lifecycle scope for caching. Supports
    both synchronous and asynchronous functions, including generator functions. Generators
    are automatically converted to return their yielded value, with cleanup handled by
    the lifecycle manager. Cleanup is automatic when the scope ends.

    Attributes:
        scope (Scope): The lifecycle scope to apply to the decorated function.

    Usage:
        @scoped.application
        def get_resource() -> Resource:
            try:
                resource = create_resource()
                yield resource  # This will be cached for the application scope
            finally:
                cleanup_resource(resource)

    """

    def __init__(self, scope: str, lifecycle: Lifecycle) -> None:
        """Initialize the ScopeDecorator with a specific lifecycle scope."""
        self._scope = scope
        self._lifecycle = lifecycle

    def __enter__(self, *_):
        """Enter the context manager."""
        self._lifecycle.push(self._scope)

    def __exit__(self, *_) -> None:
        """Exit the context manager."""
        self._lifecycle.pop()


class AsyncLifecycleContext:
    """Asynchronous context manager for lifecycle scopes."""

    def __init__(self, scope: str, lifecycle: AsyncLifecycle) -> None:
        """Initialize the AsyncScopeContext with a specific lifecycle scope."""
        self._scope = scope
        self._lifecycle = lifecycle

    async def __aenter__(self):
        """Asynchronously enter the context manager."""
        self.token = self._lifecycle.push(self._scope)

    async def __aexit__(self, *_) -> None:
        """Asynchronously exit the context manager."""
        await self._lifecycle.pop(self.token)
