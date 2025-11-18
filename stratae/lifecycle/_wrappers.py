"""Wrappers for lifecycle-managed functions and context managers."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from inspect import unwrap
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Hashable, cast

from stratae.cache.util import get_function_key

if TYPE_CHECKING:
    from stratae.lifecycle.async_lifecycle import AsyncLifecycle
    from stratae.lifecycle.lifecycle import Lifecycle


def _make_key(
    key: Hashable,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
):
    if cache_key:
        return (key, cache_key(*args, **kwargs))
    elif ignore_params or not (args or kwargs):
        return key
    else:
        return (key, args, frozenset(kwargs.items()))


def create_sync_wrapper[**P, T](
    func: Callable[P, T],
    lifecycle: Lifecycle,
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    key = get_function_key(func)

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        def factory() -> T:
            return func(*args, **kwargs)

        return lifecycle.get_cache(scope).get_or_set(
            _make_key(key, args, kwargs, cache_key, ignore_params), factory
        )

    original = unwrap(func)
    original.__outermost__ = wrapper
    return wrapper


def create_synccm_wrapper[**P, T](
    func: Callable[P, AbstractContextManager[T]],
    lifecycle: Lifecycle,
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    key = get_function_key(func)

    @wraps(func)
    def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        def factory() -> T:
            ctx = func(*args, **kwargs)
            value = lifecycle.get_exit_stack(scope).enter_context(ctx)
            return value

        return lifecycle.get_cache(scope).get_or_set(
            _make_key(key, args, kwargs, cache_key, ignore_params), factory
        )

    original = unwrap(func)
    original.__outermost__ = gen_wrapper
    return gen_wrapper


def create_sync_in_async_wrapper[**P, T](
    func: Callable[P, T],
    lifecycle: AsyncLifecycle,
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    key = get_function_key(func)

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        def factory() -> T:
            return func(*args, **kwargs)

        return lifecycle.get_cache(scope).get_or_set(
            _make_key(key, args, kwargs, cache_key, ignore_params), factory
        )

    original = unwrap(func)
    original.__outermost__ = wrapper
    return wrapper


def create_synccm_in_async_wrapper[**P, T](
    func: Callable[P, AbstractContextManager[T]],
    lifecycle: AsyncLifecycle,
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    key = get_function_key(func)

    @wraps(func)
    def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        def factory() -> T:
            ctx = func(*args, **kwargs)
            value = lifecycle.get_exit_stack(scope).enter_context(ctx)
            return value

        return lifecycle.get_cache(scope).get_or_set(
            _make_key(key, args, kwargs, cache_key, ignore_params), factory
        )

    original = unwrap(func)
    original.__outermost__ = gen_wrapper
    return gen_wrapper


def create_asynccm_wrapper[**P, T](
    func: Callable[P, AbstractAsyncContextManager[T]],
    lifecycle: AsyncLifecycle,
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, Awaitable[T]]:
    key = get_function_key(func)

    @wraps(func)
    async def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        async def factory() -> T:
            ctx = func(*args, **kwargs)
            value = await lifecycle.get_exit_stack(scope).enter_async_context(ctx)
            return value

        return await lifecycle.get_cache(scope).aget_or_set(
            _make_key(key, args, kwargs, cache_key, ignore_params), factory
        )

    original = unwrap(func)
    original.__outermost__ = gen_wrapper
    return gen_wrapper


def create_async_wrapper[**P, T](
    func: Callable[P, Awaitable[T] | AsyncGenerator[T, None]],
    lifecycle: AsyncLifecycle,
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, Awaitable[T]]:
    key = get_function_key(func)

    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        async def factory() -> T:
            return await cast(Callable[P, Awaitable[T]], func)(*args, **kwargs)

        return await lifecycle.get_cache(scope).aget_or_set(
            _make_key(key, args, kwargs, cache_key, ignore_params), factory
        )

    original = unwrap(func)
    original.__outermost__ = wrapper
    return wrapper
