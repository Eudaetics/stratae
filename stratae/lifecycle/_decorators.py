"""
Decorator objects returned by `Lifecycle.cache`/`AsyncLifecycle.cache`.

`CacheDecorator` and `AsyncCacheDecorator` inspect the decorated function
to pick the matching codegen'd wrapper from `stratae.lifecycle._wrappers`:
a plain function, a `resource`/`async_resource`-tagged context manager, or
(for `AsyncCacheDecorator`) a sync function used from within an async
lifecycle.
"""

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
    create_synccm_wrapper,
)
from stratae.lifecycle.resource import AUTO_ENTER_ASYNC, AUTO_ENTER_SYNC

if TYPE_CHECKING:
    from stratae.lifecycle.lifecycle import AsyncLifecycle, Lifecycle


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
    """
    Decorator object returned by `Lifecycle.cache`; caches a function within a scope.

    Returned by `Lifecycle.cache(scope)`, not constructed directly. Applying
    it to a function returns a wrapper whose result is cached for the
    lifetime of the named scope's activation.
    """

    __slots__ = ("_scope", "_lifecycle", "_cache_key", "_ignore_params")

    def __init__(
        self,
        scope: str,
        lifecycle: "Lifecycle",
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> None:
        """
        Initialize the CacheDecorator with a specific lifecycle scope.

        Args:
            scope: Name of the scope the cached value should live in.
            lifecycle: The `Lifecycle` owning that scope.
            cache_key: Callable deriving a hashable cache key from the
                decorated function's arguments. When omitted, the key is
                the arguments themselves.
            ignore_params: Cache a single value per scope activation
                regardless of arguments, instead of keying by argument
                values.

        """
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
        """
        Wrap `func` so its result is cached for the lifetime of the scope's activation.

        Args:
            func: The function to cache. A `resource`-tagged context
                manager function is entered automatically, with its yielded
                value cached in place of the context manager itself.

        Returns:
            A wrapper matching `func`'s signature, minus context-manager
            entry for a `resource`-tagged `func`.

        """

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
    """
    Decorator object returned by `AsyncLifecycle.cache`; caches a function within a scope.

    Returned by `AsyncLifecycle.cache(scope)`, not constructed directly.
    Applying it to a function returns a wrapper whose result is cached for
    the lifetime of the named scope's activation. Accepts sync functions,
    async functions, and `resource`/`async_resource`-tagged context
    managers of either flavor.
    """

    __slots__ = ("_scope", "_lifecycle", "_cache_key", "_ignore_params")

    def __init__(
        self,
        scope: str,
        lifecycle: "AsyncLifecycle",
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> None:
        """
        Initialize the AsyncCacheDecorator with a specific lifecycle scope.

        Args:
            scope: Name of the scope the cached value should live in.
            lifecycle: The `AsyncLifecycle` owning that scope.
            cache_key: Callable deriving a hashable cache key from the
                decorated function's arguments. When omitted, the key is
                the arguments themselves.
            ignore_params: Cache a single value per scope activation
                regardless of arguments, instead of keying by argument
                values.

        """
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
        """
        Wrap `func` so its result is cached for the lifetime of the scope's activation.

        Args:
            func: The function to cache - sync, async, or a
                `resource`/`async_resource`-tagged context manager function
                of either flavor. A tagged function is entered
                automatically, with its yielded value cached in place of
                the context manager itself.

        Returns:
            A wrapper matching `func`'s signature, minus context-manager
            entry for a tagged `func`. Async unless `func` is a plain sync
            function.

        """

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
                return create_synccm_wrapper(
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
