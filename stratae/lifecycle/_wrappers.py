"""Wrappers for lifecycle-managed functions and context managers."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from inspect import unwrap
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Hashable, cast

if TYPE_CHECKING:
    from stratae.lifecycle.async_lifecycle import AsyncLifecycle
    from stratae.lifecycle.lifecycle import Lifecycle


def _select_key_func(
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
):
    if cache_key is not None:

        def make_key_with_cache_key(key: Hashable, args: tuple[Any, ...], kwargs: dict[str, Any]):
            return (key, cache_key(*args, **kwargs))

        return make_key_with_cache_key
    elif ignore_params:

        def make_key_ignore_params(key: Hashable, *_: Any):
            return key

        return make_key_ignore_params
    else:

        def make_key_default(key: Hashable, args: tuple[Any, ...], kwargs: dict[str, Any]):
            return key if not (args or kwargs) else (key, args, frozenset(kwargs.items()))

        return make_key_default


def create_sync_wrapper[**P, T](
    func: Callable[P, T],
    lifecycle: Lifecycle,
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    key = id(func)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        def factory() -> T:
            return func(*args, **kwargs)

        return lifecycle.get_cache(scope).get_or_set(key_func(key, args, kwargs), factory)

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
    key = id(func)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        def factory() -> T:
            ctx = func(*args, **kwargs)
            value = lifecycle.get_exit_stack(scope).enter_context(ctx)
            return value

        return lifecycle.get_cache(scope).get_or_set(key_func(key, args, kwargs), factory)

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
    key = id(func)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        def factory() -> T:
            return func(*args, **kwargs)

        return lifecycle.get_cache(scope).get_or_set(key_func(key, args, kwargs), factory)

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
    key = id(func)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        def factory() -> T:
            ctx = func(*args, **kwargs)
            value = lifecycle.get_exit_stack(scope).enter_context(ctx)
            return value

        return lifecycle.get_cache(scope).get_or_set(key_func(key, args, kwargs), factory)

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
    key = id(func)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    async def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        async def factory() -> T:
            ctx = func(*args, **kwargs)
            value = await lifecycle.get_exit_stack(scope).enter_async_context(ctx)
            return value

        return await lifecycle.get_cache(scope).aget_or_set(key_func(key, args, kwargs), factory)

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
    key = id(func)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        async def factory() -> T:
            return await cast(Callable[P, Awaitable[T]], func)(*args, **kwargs)

        return await lifecycle.get_cache(scope).aget_or_set(key_func(key, args, kwargs), factory)

    original = unwrap(func)
    original.__outermost__ = wrapper
    return wrapper
