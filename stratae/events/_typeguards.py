"""Type guards for distinguishing factory shapes across the events package."""

from __future__ import annotations

import inspect
from typing import Awaitable, Callable, TypeGuard


def is_async_factory[**P, R](
    factory: Callable[P, R] | Callable[P, Awaitable[R]],
) -> TypeGuard[Callable[P, Awaitable[R]]]:
    return inspect.iscoroutinefunction(factory)


def is_sync_factory[**P, R](
    factory: Callable[P, R] | Callable[P, Awaitable[R]],
) -> TypeGuard[Callable[P, R]]:
    return not inspect.iscoroutinefunction(factory)
