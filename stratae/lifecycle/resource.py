"""Context managers and decorators for lifecycle scopes."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from inspect import iscoroutinefunction, unwrap
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    TypeGuard,
    cast,
    overload,
)

from stratae.lifecycle._wrappers import (
    create_async_wrapper,
    create_asynccm_wrapper,
    create_sync_in_async_wrapper,
    create_sync_wrapper,
    create_synccm_in_async_wrapper,
    create_synccm_wrapper,
)
from stratae.lifecycle.manage import AUTO_ENTER_ASYNC, AUTO_ENTER_SYNC

if TYPE_CHECKING:
    from stratae.lifecycle.async_lifecycle import AsyncLifecycle
    from stratae.lifecycle.lifecycle import Lifecycle


def _is_awaitable[**P, T](
    f: Callable[P, Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T],
) -> TypeGuard[Callable[P, Awaitable[T]]]:
    """Type guard to narrow func type when it is awaitable."""
    return iscoroutinefunction(f)


def _is_auto_sync_cm[**P, T](
    f: Callable[P, Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T],
) -> TypeGuard[Callable[P, AbstractContextManager[T]]]:
    """Type guard to narrow func type when auto_enter is 'sync'."""
    return getattr(unwrap(f), "__auto_enter__", None) == AUTO_ENTER_SYNC


def _is_auto_async_cm[**P, T, U](
    f: Callable[P, U | Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[U]],
) -> TypeGuard[Callable[P, AbstractAsyncContextManager[U]]]:
    """Type guard to narrow func type when auto_enter is 'async'."""
    return getattr(unwrap(f), "__auto_enter__", None) == AUTO_ENTER_ASYNC


class ScopeContext:
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

    @overload
    def __call__[**P, T](self, func: Callable[P, AbstractContextManager[T]]) -> Callable[P, T]: ...

    @overload
    def __call__[**P, T](self, func: Callable[P, T]) -> Callable[P, T]: ...

    def __call__[**P, T](
        self,
        func: Callable[P, T | AbstractContextManager[T]],
    ) -> Callable[P, T]:
        """Decorate a function to set its lifecycle scope for caching."""

        def add_scope_to_func(
            f: Callable[P, T | AbstractContextManager[T]],
        ) -> Callable[P, T]:
            if _is_auto_sync_cm(f):
                wrapper = create_synccm_wrapper(f, self._lifecycle, self._scope)
                return wrapper
            wrapper = create_sync_wrapper(f, self._lifecycle, self._scope)
            return cast(Callable[P, T], wrapper)

        return add_scope_to_func(func)

    def __enter__(self, *_):
        """Enter the context manager."""
        self._lifecycle.push(self._scope)

    def __exit__(self, *_) -> None:
        """Exit the context manager."""
        self._lifecycle.pop()


class AsyncScopeContext:
    """Asynchronous context manager for lifecycle scopes."""

    def __init__(self, scope: str, lifecycle: AsyncLifecycle) -> None:
        """Initialize the AsyncScopeContext with a specific lifecycle scope."""
        self._scope = scope
        self._lifecycle = lifecycle

    @overload
    def __call__[**P, T](self, func: Callable[P, AbstractContextManager[T]]) -> Callable[P, T]: ...

    @overload
    def __call__[**P, T](
        self, func: Callable[P, AbstractAsyncContextManager[T]]
    ) -> Callable[P, Awaitable[T]]: ...

    @overload
    def __call__[**P, T](self, func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]: ...

    @overload
    def __call__[**P, T](self, func: Callable[P, T]) -> Callable[P, T]: ...

    def __call__[**P, T](
        self,
        func: Callable[
            P, Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T
        ],
    ) -> Callable[P, Awaitable[T] | T]:
        """Decorate a function to set its lifecycle scope for caching."""

        def add_scope_to_func(
            f: Callable[
                P, Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T
            ],
        ) -> Callable[P, Awaitable[T] | T]:
            if _is_auto_async_cm(f):
                return create_asynccm_wrapper(f, self._lifecycle, self._scope)
            elif _is_auto_sync_cm(f):
                return create_synccm_in_async_wrapper(f, self._lifecycle, self._scope)
            elif _is_awaitable(f):
                wrapper = create_async_wrapper(f, self._lifecycle, self._scope)
                return wrapper
            else:
                return create_sync_in_async_wrapper(
                    cast(Callable[P, T], f), self._lifecycle, self._scope
                )

        return add_scope_to_func(func)

    async def __aenter__(self) -> AsyncScopeContext:
        """Asynchronously enter the context manager."""
        self.token = self._lifecycle.push(self._scope)
        return self

    async def __aexit__(self, *_) -> None:
        """Asynchronously exit the context manager."""
        await self._lifecycle.pop(self.token)
