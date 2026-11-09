"""
Decorator objects returned by `Scope.cache` and `AsyncScope.cache`.

A function may be cached in only one scope, so its cache always has one fixed home. How it's
wrapped is chosen once, when it's decorated. A generator function, sync or async, has its
generator entered and the yielded value cached. A function whose decorator hides a generator
function checks what each miss returns. Any other function has its return value cached.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Hashable
from inspect import isasyncgenfunction, isgeneratorfunction, unwrap
from typing import TYPE_CHECKING, Any, Literal, overload

from stratae._callables import is_async_callable
from stratae.lifecycle._wrappers import (
    Cached,
    create_async_generator_wrapper,
    create_async_wrapper,
    create_checked_wrapper,
    create_generator_wrapper,
    create_sync_in_async_wrapper,
    create_sync_wrapper,
)
from stratae.lifecycle.exceptions import (
    AlreadyCachedError,
    AsyncGeneratorInSyncScopeError,
    UncacheableError,
)

if TYPE_CHECKING:
    from stratae.lifecycle.scope import AsyncScope, BaseScope, Scope


def _hidden_generator(func: Callable[..., Any]) -> Literal["sync", "async"] | None:
    """
    Return the kind of generator function a decorator over `func` hides, if any.

    Found through `__wrapped__`, which `functools.wraps` sets. A decorator that doesn't set it
    hides nothing this can see, so its function is cached like any other.
    """
    inner = unwrap(func)
    if inner is func:
        return None
    if isasyncgenfunction(inner):
        return "async"
    if isgeneratorfunction(inner):
        return "sync"
    return None


def _mark_as_scoped(func: Callable[..., Any], scope: "BaseScope") -> None:
    """
    Mark func's unwrapped identity as cached in scope.

    :raises LifecycleConfigurationError: If func is already cached, or can't hold the mark.
    """
    target = unwrap(func)
    existing: BaseScope | None = getattr(target, "__cache_scope__", None)
    if existing is not None:
        raise AlreadyCachedError(func, existing.name, scope.name)
    try:
        target.__cache_scope__ = scope
    except (AttributeError, TypeError):
        raise UncacheableError(func, scope.name) from None


class CacheDecorator:
    """Decorator returned by `Scope.cache()`."""

    __slots__ = ("_cache_key", "_ignore_params", "_scope")

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
        self, func: Callable[P, Generator[T, Any, Any]]
    ) -> Cached[P, T, Generator[T, Any, Any]]: ...

    @overload
    def __call__[**P, T](self, func: Callable[P, T]) -> Cached[P, T, T]: ...

    def __call__(self, func: Callable[..., Any]) -> Cached[..., Any, Any]:
        """
        Wrap `func` so its result is cached per scope activation.

        :param func: The function. A generator function's generator is entered, its yielded
            value cached, and it's closed when the activation ends.
        :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
        :raises AsyncGeneratorInSyncScopeError: If `func` is, or a decorator hides, an async
            generator function.
        :raises LifecycleConfigurationError: If `func` is already cached in a scope.
        """
        scope, cache_key, ignore_params = self._scope, self._cache_key, self._ignore_params
        hidden = None if isgeneratorfunction(func) else _hidden_generator(func)
        if isasyncgenfunction(func) or hidden == "async":
            raise AsyncGeneratorInSyncScopeError(func, scope.name)
        _mark_as_scoped(func, scope)
        if isgeneratorfunction(func):
            return create_generator_wrapper(func, scope, cache_key, ignore_params)
        if hidden == "sync":
            return create_checked_wrapper(func, scope, cache_key, ignore_params, is_async=False)
        return create_sync_wrapper(func, scope, cache_key, ignore_params)


class AsyncCacheDecorator:
    """Decorator returned by `AsyncScope.cache()`."""

    __slots__ = ("_cache_key", "_ignore_params", "_scope")

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
        self, func: Callable[P, AsyncGenerator[T, Any]]
    ) -> Cached[P, Awaitable[T], AsyncGenerator[T, Any]]: ...

    @overload
    def __call__[**P, T](
        self, func: Callable[P, Generator[T, Any, Any]]
    ) -> Cached[P, T, Generator[T, Any, Any]]: ...

    @overload
    def __call__[**P, T](
        self, func: Callable[P, Awaitable[T]]
    ) -> Cached[P, Awaitable[T], Awaitable[T]]: ...

    @overload
    def __call__[**P, T](self, func: Callable[P, T]) -> Cached[P, T, T]: ...

    def __call__(self, func: Callable[..., Any]) -> Cached[..., Any, Any]:
        """
        Wrap `func` so its result is cached per scope activation.

        :param func: A sync or async function. A generator function's generator, sync or
            async, is entered, its yielded value cached, and it's closed when the activation
            ends.
        :returns: A wrapper matching `func`'s signature, with `func` as its `uncached` attribute.
            Async if `func` is async or an async generator function, or a decorator hides one.
        :raises LifecycleConfigurationError: If `func` is already cached in a scope.
        """
        scope, cache_key, ignore_params = self._scope, self._cache_key, self._ignore_params
        _mark_as_scoped(func, scope)
        if isasyncgenfunction(func):
            return create_async_generator_wrapper(func, scope, cache_key, ignore_params)
        if isgeneratorfunction(func):
            return create_generator_wrapper(func, scope, cache_key, ignore_params)
        if is_async_callable(func):
            return create_async_wrapper(func, scope, cache_key, ignore_params)
        hidden = _hidden_generator(func)
        if hidden is not None:
            return create_checked_wrapper(
                func, scope, cache_key, ignore_params, is_async=hidden == "async"
            )
        return create_sync_in_async_wrapper(func, scope, cache_key, ignore_params)
