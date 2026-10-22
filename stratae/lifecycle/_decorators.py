"""
Decorator objects returned by `Scope.cache`/`AsyncScope.cache`.

Each decorator binds its `Scope`/`AsyncScope` object directly, and enforces that a function
is only ever cached in one scope: caching a function's result in more than one scope has no
fixed, declared home for it - which scope wins would depend on which scopes happen to be
active when it's called, exactly the kind of runtime-resolved cache placement this design
rejects elsewhere.
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
    """Type guard to narrow func type when it is awaitable."""
    return is_async_callable(f)


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


def _mark_as_scoped(func: Callable[..., Any], scope: "BaseScope") -> None:
    """
    Mark func's underlying identity as cached in scope, raising if it's already claimed.

    Marks the same target `resource`/`async_resource` tag - `unwrap(func)` - so caching a
    `resource`-wrapped generator is checked against its true identity, not the
    `contextlib.contextmanager` wrapper `resource()` produces. A target that can't hold
    the mark is rejected rather than left unchecked.

    :raises LifecycleConfigurationError: If func is already cached in a different (or the same)
        scope, or can't hold the mark - a builtin or a bound method, say.
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
    """
    Decorator object returned by `Scope.cache`; caches a function within that scope.

    Returned by `Scope.cache()`, not constructed directly. Applying it to a function
    returns a wrapper whose result is cached for the lifetime of the scope's active
    activation.
    """

    __slots__ = ("_scope", "_cache_key", "_ignore_params")

    def __init__(
        self,
        scope: "Scope",
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> None:
        """
        Initialize the CacheDecorator bound to a specific scope.

        :param scope: The {py:class}`Scope <stratae.lifecycle.scope.Scope>` owning the cached value.
        :param cache_key: Callable deriving a hashable cache key from the decorated function's
            arguments. When omitted, the key is the arguments themselves.
        :param ignore_params: Cache a single value per scope activation regardless of arguments,
            instead of keying by argument values.
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
        Wrap `func` so its result is cached for the lifetime of the scope's activation.

        :param func: The function to cache. A
            {py:func}`resource <stratae.lifecycle.resource.resource>`-tagged context manager
            function is entered automatically, with its yielded value cached in place of the
            context manager itself.
        :returns: A wrapper matching `func`'s signature, minus context-manager entry for a
            {py:func}`resource <stratae.lifecycle.resource.resource>`-tagged `func`. Its `uncached`
            attribute is `func` itself, callable whether or not the scope is active.
        :raises LifecycleConfigurationError: If `func` is already cached in some scope.
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
    """
    Decorator object returned by `AsyncScope.cache`; caches a function within that scope.

    Returned by `AsyncScope.cache()`, not constructed directly. Applying it to a function
    returns a wrapper whose result is cached for the lifetime of the scope's active
    activation. Accepts sync functions, async functions, and `resource`/`async_resource`
    -tagged context managers of either flavor.
    """

    __slots__ = ("_scope", "_cache_key", "_ignore_params")

    def __init__(
        self,
        scope: "AsyncScope",
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> None:
        """
        Initialize the AsyncCacheDecorator bound to a specific scope.

        :param scope: The {py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` owning the
            cached value.
        :param cache_key: Callable deriving a hashable cache key from the decorated function's
            arguments. When omitted, the key is the arguments themselves.
        :param ignore_params: Cache a single value per scope activation regardless of arguments,
            instead of keying by argument values.
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
        Wrap `func` so its result is cached for the lifetime of the scope's activation.

        :param func: The function to cache - sync, async, or a
            {py:func}`resource <stratae.lifecycle.resource.resource>`/
            {py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged context
            manager function of either flavor. A tagged function is entered automatically, with
            its yielded value cached in place of the context manager itself.
        :returns: A wrapper matching `func`'s signature, minus context-manager entry for a tagged
            `func`. Async unless `func` is a plain sync function. Its `uncached` attribute is `func`
            itself, callable whether or not the scope is active.
        :raises LifecycleConfigurationError: If `func` is already cached in some scope.
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
