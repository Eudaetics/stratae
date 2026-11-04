"""Inspection helpers for callables, shared across stratae's packages."""

from collections.abc import Callable
from inspect import iscoroutinefunction
from typing import Any


def is_async_callable(fn: Callable[..., Any]) -> bool:
    """Whether calling `fn` returns a coroutine, including an object with `async def __call__`."""
    return iscoroutinefunction(fn) or iscoroutinefunction(fn.__call__)
