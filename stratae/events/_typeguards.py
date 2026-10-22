"""Type guards for distinguishing factory shapes across the events package."""

from __future__ import annotations

from typing import Awaitable, Callable, TypeGuard

from stratae._callables import is_async_callable


def is_async_factory[**P, R](
    factory: Callable[P, R] | Callable[P, Awaitable[R]],
) -> TypeGuard[Callable[P, Awaitable[R]]]:
    return is_async_callable(factory)


def is_sync_factory[**P, R](
    factory: Callable[P, R] | Callable[P, Awaitable[R]],
) -> TypeGuard[Callable[P, R]]:
    return not is_async_callable(factory)
