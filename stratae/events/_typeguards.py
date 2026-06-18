"""Type guards for distinguishing factory shapes across the events package."""

from __future__ import annotations

import inspect
import typing
from typing import Any, Awaitable, Callable, TypeGuard


def is_class_factory[**P, E: Any](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[type[E]]:
    if inspect.isclass(factory):
        return True
    origin = typing.get_origin(factory)
    return origin is not None and inspect.isclass(origin)


def is_async_factory[**P, E: Any](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[Callable[P, Awaitable[E]]]:
    return inspect.iscoroutinefunction(factory)


def is_sync_factory[**P, E: Any](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[Callable[P, E]]:
    return not inspect.iscoroutinefunction(factory)
