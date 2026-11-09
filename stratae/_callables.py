"""Inspection helpers for callables, shared across stratae's packages."""

from collections.abc import Awaitable, Callable
from inspect import iscoroutinefunction
from typing import TypeGuard


def is_async_callable[**P, R](
    fn: Callable[P, R] | Callable[P, Awaitable[R]],
) -> TypeGuard[Callable[P, Awaitable[R]]]:
    """
    Whether calling `fn` returns a coroutine, including an object with `async def __call__`.

    Narrows `fn` to its async form for type checkers.
    """
    return iscoroutinefunction(fn) or iscoroutinefunction(fn.__call__)
