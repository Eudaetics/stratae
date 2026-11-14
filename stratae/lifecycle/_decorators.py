"""
Decorator objects returned by `Scope.cache` and `AsyncScope.cache`.

Any callable whose signature can be read may be cached, in any number of scopes. Each
`cache()` call builds its own wrapper and storage, and nothing is written onto the callable.

How it's wrapped is chosen once, when it's decorated, from what calling it gives, as
{py:func}`call_result <stratae.callables.call_result>` tells it. A value is cached as it is, and a
coroutine's result is cached once it's awaited. A generator of either kind is entered, and the
value it yields is cached. A function whose decorator hides one giving something else checks
what each miss returns. Both scopes choose the same way. A sync scope can't await, so it
refuses a coroutine or an async generator.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Hashable
from typing import Any, overload

from stratae.callables import Gives, call_result
from stratae.lifecycle._slots import SlotStore
from stratae.lifecycle._wrappers import (
    Cached,
    create_async_generator_wrapper,
    create_async_wrapper,
    create_checked_async_wrapper,
    create_checked_sync_wrapper,
    create_generator_wrapper,
    create_sync_wrapper,
)
from stratae.lifecycle.exceptions import AsyncInSyncScopeError

_WRAPPERS: dict[Gives, Callable[..., Cached[..., Any, Any]]] = {
    Gives.VALUE: create_sync_wrapper,
    Gives.COROUTINE: create_async_wrapper,
    Gives.GENERATOR: create_generator_wrapper,
    Gives.ASYNC_GENERATOR: create_async_generator_wrapper,
}
"""The wrapper for a callable known to give each kind of thing."""


def _wrap(
    func: Callable[..., Any],
    store: SlotStore,
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
    *,
    can_await: bool,
) -> Cached[..., Any, Any]:
    """
    Wrap `func` for its scope, from what calling it gives.

    :param can_await: Whether the scope can await, as an async scope can.
    :raises AsyncInSyncScopeError: If `func` gives a coroutine or an async generator, and the
        scope can't await.
    """
    gives, hidden = call_result(func)
    if hidden:
        if gives.awaits and can_await:
            return create_checked_async_wrapper(func, store, cache_key, ignore_params)
        return create_checked_sync_wrapper(func, store, cache_key, ignore_params)
    if gives.awaits and not can_await:
        raise AsyncInSyncScopeError(func, store.name, gives)
    return _WRAPPERS[gives](func, store, cache_key, ignore_params)


class _CacheDecorator:
    """The arguments to a scope's `cache()`, bound to the scope's slot storage."""

    __slots__ = ("_cache_key", "_ignore_params", "_store")

    def __init__(
        self,
        store: SlotStore,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> None:
        """
        Bind the decorator to a scope.

        :param store: The owning scope's slot storage.
        :param cache_key: Derives a hashable key from the arguments.
        :param ignore_params: Cache one value per activation regardless of arguments.
        """
        self._store = store
        self._cache_key = cache_key
        self._ignore_params = ignore_params


class CacheDecorator(_CacheDecorator):
    """Decorator returned by `Scope.cache()`."""

    __slots__ = ()

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
        :raises AsyncInSyncScopeError: If calling `func` gives a coroutine or an async
            generator.
        :raises UnreadableSignatureError: If `func`'s signature can't be read.
        """
        return _wrap(func, self._store, self._cache_key, self._ignore_params, can_await=False)


class AsyncCacheDecorator(_CacheDecorator):
    """Decorator returned by `AsyncScope.cache()`."""

    __slots__ = ()

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
            Async if calling `func` gives a coroutine or an async generator, or a decorator
            hides a function that does.
        :raises UnreadableSignatureError: If `func`'s signature can't be read.
        """
        return _wrap(func, self._store, self._cache_key, self._ignore_params, can_await=True)
