"""Lifecycle context manager decorators."""

from contextlib import AbstractContextManager, asynccontextmanager, contextmanager
from functools import wraps
from inspect import unwrap
from typing import AsyncGenerator, Callable, Generator

AUTO_ENTER_SYNC = object()
AUTO_ENTER_ASYNC = object()


def managed[**P, T](func: Callable[P, Generator[T, None, None]]):
    """Decorate a function to automatically enter a contextmanager using lifecycle."""
    unwrap(func).__auto_enter__ = AUTO_ENTER_SYNC
    cm_factory: Callable[P, AbstractContextManager[T]] = contextmanager(func)

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs):
        return cm_factory(*args, **kwargs)

    return wrapper


def async_managed[**P, T](func: Callable[P, AsyncGenerator[T, None]]):
    """Decorate an async function to automatically enter a contextmanager using lifecycle."""
    unwrap(func).__auto_enter__ = AUTO_ENTER_ASYNC
    cm_factory = asynccontextmanager(func)

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs):
        return cm_factory(*args, **kwargs)

    return wrapper
