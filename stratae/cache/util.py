"""Utility functions for cache management."""

from typing import Any, Awaitable, Callable, Hashable


def get_function_key(func: Callable[..., Any] | Callable[..., Awaitable[Any]]) -> Hashable:
    """Generate a unique key for a function based on its name and module."""
    return (func.__module__, func.__qualname__, id(func))
