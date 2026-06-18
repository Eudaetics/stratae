"""Type guards for distinguishing factory shapes across the events package."""

from __future__ import annotations

import inspect
from typing import Awaitable, Callable, TypeGuard


def is_class_factory[**P, E: object](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[type[E]]:
    return inspect.isclass(factory)


def is_async_factory[**P, E: object](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[Callable[P, Awaitable[E]]]:
    return inspect.iscoroutinefunction(factory)


def is_sync_factory[**P, E: object](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[Callable[P, E]]:
    return not inspect.iscoroutinefunction(factory)
