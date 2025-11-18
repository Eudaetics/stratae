from __future__ import annotations

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from inspect import iscoroutinefunction, unwrap
from typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    Hashable,
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


class CacheDecorator:
    """Decorator class to set the lifecycle scope for caching function results."""

    def __init__(
        self,
        scope: str,
        lifecycle: Lifecycle,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> None:
        """Initialize the ScopeDecorator with a specific lifecycle scope."""
        self._scope = scope
        self._lifecycle = lifecycle
        self._cache_key = cache_key
        self._ignore_params = ignore_params

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
                return create_synccm_wrapper(
                    f, self._lifecycle, self._scope, self._cache_key, self._ignore_params
                )
            return cast(
                Callable[P, T],
                create_sync_wrapper(
                    f, self._lifecycle, self._scope, self._cache_key, self._ignore_params
                ),
            )

        return add_scope_to_func(func)


class AsyncCacheDecorator:
    """Asynchronous decorator class to set the lifecycle scope for caching function results."""

    def __init__(
        self,
        scope: str,
        lifecycle: AsyncLifecycle,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> None:
        self._scope = scope
        self._lifecycle = lifecycle
        self._cache_key = cache_key
        self._ignore_params = ignore_params

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
                return create_asynccm_wrapper(
                    f, self._lifecycle, self._scope, self._cache_key, self._ignore_params
                )
            elif _is_auto_sync_cm(f):
                return create_synccm_in_async_wrapper(
                    f, self._lifecycle, self._scope, self._cache_key, self._ignore_params
                )
            elif _is_awaitable(f):
                return create_async_wrapper(
                    f, self._lifecycle, self._scope, self._cache_key, self._ignore_params
                )
            else:
                return create_sync_in_async_wrapper(
                    cast(Callable[P, T], f),
                    self._lifecycle,
                    self._scope,
                    self._cache_key,
                    self._ignore_params,
                )

        return add_scope_to_func(func)
