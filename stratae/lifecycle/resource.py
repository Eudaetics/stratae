"""
Decorators marking a generator function as an auto-entered lifecycle resource.

`resource` and `async_resource` wrap a generator function with
`contextlib.contextmanager`/`contextlib.asynccontextmanager`, same as
calling those directly, but also tag the result so `@lifecycle.cache(...)`
recognizes it as a context manager to enter automatically rather than a
plain value to cache as-is. The entered value is what gets cached; exit is
deferred to the owning scope's exit stack, which unwinds when that scope
deactivates.
"""

from contextlib import asynccontextmanager, contextmanager
from inspect import unwrap
from typing import AsyncGenerator, Callable, Generator

AUTO_ENTER_SYNC = object()
"""Sentinel tagging a function as a sync context manager for lifecycle to auto-enter."""

AUTO_ENTER_ASYNC = object()
"""Sentinel tagging a function as an async context manager for lifecycle to auto-enter."""


def resource[**P, T](func: Callable[P, Generator[T, None, None]]):
    """
    Mark a generator function as an auto-entered context manager for lifecycle caching.

    Wraps `func` with `contextlib.contextmanager`, same as calling that
    directly, and tags the result so `@lifecycle.cache(...)` enters it
    automatically instead of caching the raw context manager object.

    Args:
        func: Generator function yielding exactly one value, in the shape
            `contextlib.contextmanager` expects.

    Returns:
        The wrapped context manager function, tagged for auto-entry.

    For example::

    ```python
    from stratae.lifecycle import Lifecycle, Scope, resource

    lifecycle = Lifecycle([Scope("request", "context")])

    @lifecycle.cache("request")
    @resource
    def get_session():
        session = Session()
        try:
            yield session
        finally:
            session.close()
    ```

    """
    unwrap(func).__auto_enter__ = AUTO_ENTER_SYNC
    return contextmanager(func)


def async_resource[**P, T](func: Callable[P, AsyncGenerator[T, None]]):
    """
    Mark an async generator function as an auto-entered async context manager.

    Wraps `func` with `contextlib.asynccontextmanager`, same as calling that
    directly, and tags the result so `@lifecycle.cache(...)` enters it
    automatically instead of caching the raw context manager object.

    Args:
        func: Async generator function yielding exactly one value, in the
            shape `contextlib.asynccontextmanager` expects.

    Returns:
        The wrapped async context manager function, tagged for auto-entry.

    For example::

    ```python
    from stratae.lifecycle import AsyncLifecycle, Scope, async_resource

    lifecycle = AsyncLifecycle([Scope("request", "context")])

    @lifecycle.cache("request")
    @async_resource
    async def get_session():
        session = await Session.open()
        try:
            yield session
        finally:
            await session.close()
    ```

    """
    unwrap(func).__auto_enter__ = AUTO_ENTER_ASYNC
    return asynccontextmanager(func)
