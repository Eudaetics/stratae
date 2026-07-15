"""Lifecycle resource decorators for auto-entered context managers."""

from contextlib import asynccontextmanager, contextmanager
from inspect import unwrap
from typing import AsyncGenerator, Callable, Generator

AUTO_ENTER_SYNC = object()
AUTO_ENTER_ASYNC = object()


def resource[**P, T](func: Callable[P, Generator[T, None, None]]):
    """Decorate a function to automatically enter a contextmanager using lifecycle."""
    unwrap(func).__auto_enter__ = AUTO_ENTER_SYNC
    return contextmanager(func)


def async_resource[**P, T](func: Callable[P, AsyncGenerator[T, None]]):
    """Decorate an async function to automatically enter a contextmanager using lifecycle."""
    unwrap(func).__auto_enter__ = AUTO_ENTER_ASYNC
    return asynccontextmanager(func)
