"""
Decorator objects returned by `Scope.cache` and `AsyncScope.cache`.

A function may be cached in only one scope, so its cache always has one fixed home.
"""

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from inspect import unwrap
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Hashable, TypeGuard, cast, overload

from stratae._callables import is_async_callable
from stratae.lifecycle._wrappers import (
    Cached,
    create_async_wrapper,
    create_asynccm_wrapper,
    create_sync_in_async_wrapper,
    create_sync_wrapper,
    create_synccm_wrapper,
)
from stratae.lifecycle.exceptions import LifecycleConfigurationError
from stratae.lifecycle.resource import AUTO_ENTER_ASYNC, AUTO_ENTER_SYNC

if TYPE_CHECKING:
    from stratae.lifecycle.scope import AsyncScope, BaseScope, Scope


def _is_awaitable[**P, T](
    f: Callable[P, Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T],
) -> TypeGuard[Callable[P, Awaitable[T]]]:
    """Narrow func to its async form."""
    return is_async_callable(f)


def _is_auto_sync_cm[**P, T](
    f: Callable[P, Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T],
) -> TypeGuard[Callable[P, AbstractContextManager[T]]]:
    """Narrow func to a sync resource."""
    return getattr(unwrap(f), "__auto_enter__", None) == AUTO_ENTER_SYNC


def _is_auto_async_cm[**P, T, U](
    f: Callable[P, U | Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[U]],
) -> TypeGuard[Callable[P, AbstractAsyncContextManager[U]]]:
    """Narrow func to an async resource."""
    return getattr(unwrap(f), "__auto_enter__", None) == AUTO_ENTER_ASYNC


def _mark_as_scoped(func: Callable[..., Any], scope: "BaseScope") -> None:
    """
    Mark func's unwrapped identity as cached in scope.

    :raises LifecycleConfigurationError: If func is already cached, or can't hold the mark.
    """
    target = unwrap(func)
    existing: "BaseScope | None" = getattr(target, "__cache_scope__", None)
    if existing is not None:
        raise LifecycleConfigurationError(
            f"{func!r} is already cached in scope {existing.name!r}; "
            f"cannot also cache it in {scope.name!r}."
        )
    try:
        target.__cache_scope__ = scope
    except (AttributeError, TypeError):
        raise LifecycleConfigurationError(
            f"Cannot cache {func!r} in scope {scope.name!r}: builtins, bound methods, and other "
            "objects that can't hold attributes can't be cached. Wrap it in a function instead."
        ) from None


class CacheDecorator:
    """Decorator returned by `Scope.cache()`."""

    __slots__ = ("_scope", "_cache_key", "_ignore_params")

    def __init__(
        self,
        scope: "Scope",
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> None:
        """
        Bind the decorator to a scope.

        :param scope: The owning {py:class}`Scope <stratae.lifecycle.scope.Scope>`.
        :param cache_key: Derives a hashable key from the arguments.
        :param ignore_params: Cache one value per activation regardless of arguments.
        """
        self._scope = scope
        self._cache_key = cache_key
        self._ignore_params = ignore_params

    @overload
    def __call__[**P, T](
        self, func: Callable[P, AbstractContextManager[T]]
    ) -> Cached[P, T, AbstractContextManager[T]]: ...

    @overload
    def __call__[**P, T](self, func: Callable[P, T]) -> Cached[P, T, T]: ...

    def __call__[**P, T](
        self,
        func: Callable[P, T | AbstractContextManager[T]],
    ) -> Cached[P, T, T | AbstractContextManager[T]]:
        """
        Wrap `func` so its result is cached per scope activation.

        :param func: The function, or a {py:func}`resource <stratae.lifecycle.resource.resource>`
            to enter.
        :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
        :raises LifecycleConfigurationError: If `func` is already cached in a scope.
        """
        _mark_as_scoped(func, self._scope)

        def add_scope_to_func(
            f: Callable[P, T | AbstractContextManager[T]],
        ) -> Cached[P, T, T | AbstractContextManager[T]]:
            if _is_auto_sync_cm(f):
                return create_synccm_wrapper(f, self._scope, self._cache_key, self._ignore_params)
            return cast(
                Cached[P, T, T | AbstractContextManager[T]],
                create_sync_wrapper(f, self._scope, self._cache_key, self._ignore_params),
            )

        return add_scope_to_func(func)


class AsyncCacheDecorator:
    """Decorator returned by `AsyncScope.cache()`."""

    __slots__ = ("_scope", "_cache_key", "_ignore_params")

    def __init__(
        self,
        scope: "AsyncScope",
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> None:
        """
        Bind the decorator to a scope.

        :param scope: The owning {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`.
        :param cache_key: Derives a hashable key from the arguments.
        :param ignore_params: Cache one value per activation regardless of arguments.
        """
        self._scope = scope
        self._cache_key = cache_key
        self._ignore_params = ignore_params

    @overload
    def __call__[**P, T](
        self, func: Callable[P, AbstractContextManager[T]]
    ) -> Cached[P, T, AbstractContextManager[T]]: ...

    @overload
    def __call__[**P, T](
        self, func: Callable[P, AbstractAsyncContextManager[T]]
    ) -> Cached[P, Awaitable[T], AbstractAsyncContextManager[T]]: ...

    @overload
    def __call__[**P, T](
        self, func: Callable[P, Awaitable[T]]
    ) -> Cached[P, Awaitable[T], Awaitable[T]]: ...

    @overload
    def __call__[**P, T](self, func: Callable[P, T]) -> Cached[P, T, T]: ...

    def __call__[**P, T](
        self,
        func: Callable[
            P, Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T
        ],
    ) -> Cached[
        P,
        Awaitable[T] | T,
        Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T,
    ]:
        """
        Wrap `func` so its result is cached per scope activation.

        :param func: A sync or async function, or a resource of either kind to enter.
        :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
            Async unless `func` is a plain sync function.
        :raises LifecycleConfigurationError: If `func` is already cached in a scope.
        """
        _mark_as_scoped(func, self._scope)

        def add_scope_to_func(
            f: Callable[
                P, Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T
            ],
        ) -> Cached[
            P,
            Awaitable[T] | T,
            Awaitable[T] | AbstractAsyncContextManager[T] | AbstractContextManager[T] | T,
        ]:
            if _is_auto_async_cm(f):
                return create_asynccm_wrapper(f, self._scope, self._cache_key, self._ignore_params)
            elif _is_auto_sync_cm(f):
                return create_synccm_wrapper(f, self._scope, self._cache_key, self._ignore_params)
            elif _is_awaitable(f):
                return create_async_wrapper(f, self._scope, self._cache_key, self._ignore_params)
            else:
                return create_sync_in_async_wrapper(
                    cast(Callable[P, T], f),
                    self._scope,
                    self._cache_key,
                    self._ignore_params,
                )

        return add_scope_to_func(func)
