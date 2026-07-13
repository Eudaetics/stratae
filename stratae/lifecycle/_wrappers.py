"""Wrappers for lifecycle-managed functions and context managers."""

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from typing import TYPE_CHECKING, Any, AsyncGenerator, Awaitable, Callable, Hashable, cast

from stratae.lifecycle._scope import UNSET

if TYPE_CHECKING:
    from stratae.lifecycle.async_lifecycle import AsyncLifecycle
    from stratae.lifecycle.lifecycle import Lifecycle


def _select_key_func(
    cache_key: Callable[..., Hashable] | None,
    ignore_params: bool,
):
    if cache_key is not None:

        def make_key_with_cache_key(args: tuple[Any, ...], kwargs: dict[str, Any]):
            return cache_key(*args, **kwargs)

        return make_key_with_cache_key
    elif ignore_params:

        def make_key_ignore_params(*_: Any):
            return None

        return make_key_ignore_params
    else:

        def make_key_default(args: tuple[Any, ...], kwargs: dict[str, Any]):
            return None if not (args or kwargs) else (args, frozenset(kwargs.items()))

        return make_key_default


def _resolve_cache(lifecycle: Any, scope: str, slot: int) -> dict[Hashable, Any]:
    """Get the function's dedicated cache dict from its slot, creating it on first use."""
    slots = lifecycle.get_slots(scope)
    cache: dict[Hashable, Any] = slots[slot]
    if cache is UNSET:
        cache = slots[slot] = {}
    return cache


def create_sync_wrapper[**P, T](
    func: Callable[P, T],
    lifecycle: "Lifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    slot = lifecycle.allocate_slot(scope)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        cache = _resolve_cache(lifecycle, scope, slot)
        key = key_func(args, kwargs)
        if key in cache:
            return cache[key]
        value = func(*args, **kwargs)
        cache[key] = value
        return value

    return wrapper


def create_synccm_wrapper[**P, T](
    func: Callable[P, AbstractContextManager[T]],
    lifecycle: "Lifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    slot = lifecycle.allocate_slot(scope)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        cache = _resolve_cache(lifecycle, scope, slot)
        key = key_func(args, kwargs)
        if key in cache:
            return cache[key]
        ctx = func(*args, **kwargs)
        value = lifecycle.get_exit_stack(scope).enter_context(ctx)
        cache[key] = value
        return value

    return gen_wrapper


def create_sync_in_async_wrapper[**P, T](
    func: Callable[P, T],
    lifecycle: "AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    slot = lifecycle.allocate_slot(scope)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        cache = _resolve_cache(lifecycle, scope, slot)
        key = key_func(args, kwargs)
        if key in cache:
            return cache[key]
        value = func(*args, **kwargs)
        cache[key] = value
        return value

    return wrapper


def create_synccm_in_async_wrapper[**P, T](
    func: Callable[P, AbstractContextManager[T]],
    lifecycle: "AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, T]:
    slot = lifecycle.allocate_slot(scope)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        cache = _resolve_cache(lifecycle, scope, slot)
        key = key_func(args, kwargs)
        if key in cache:
            return cache[key]
        ctx = func(*args, **kwargs)
        value = lifecycle.get_exit_stack(scope).enter_context(ctx)
        cache[key] = value
        return value

    return gen_wrapper


def create_asynccm_wrapper[**P, T](
    func: Callable[P, AbstractAsyncContextManager[T]],
    lifecycle: "AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, Awaitable[T]]:
    slot = lifecycle.allocate_slot(scope)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    async def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        cache = _resolve_cache(lifecycle, scope, slot)
        key = key_func(args, kwargs)
        if key in cache:
            return cache[key]
        ctx = func(*args, **kwargs)
        value = await lifecycle.get_exit_stack(scope).enter_async_context(ctx)
        cache[key] = value
        return value

    return gen_wrapper


def create_async_wrapper[**P, T](
    func: Callable[P, Awaitable[T] | AsyncGenerator[T, None]],
    lifecycle: "AsyncLifecycle",
    scope: str,
    cache_key: Callable[..., Hashable] | None = None,
    ignore_params: bool = False,
) -> Callable[P, Awaitable[T]]:
    slot = lifecycle.allocate_slot(scope)
    key_func = _select_key_func(cache_key, ignore_params)

    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        cache = _resolve_cache(lifecycle, scope, slot)
        key = key_func(args, kwargs)
        if key in cache:
            return cache[key]
        value = await cast(Callable[P, Awaitable[T]], func)(*args, **kwargs)
        cache[key] = value
        return value

    return wrapper
