"""
Decorators marking a generator function as an auto-entered lifecycle resource.

{py:func}`resource` and {py:func}`async_resource` wrap a generator function
with `contextlib.contextmanager`/`contextlib.asynccontextmanager`, same as
calling those directly, but also tags the result with a marker. This marker lets
{py:meth}`Lifecycle.cache <stratae.lifecycle.lifecycle.Lifecycle.cache>` (or
{py:meth}`AsyncLifecycle.cache <stratae.lifecycle.lifecycle.AsyncLifecycle.cache>`)
recognize it as a context manager to enter automatically rather than a plain
value to cache as-is. The entered value is what gets cached; exit is deferred
to the owning scope's exit stack, which unwinds when that scope deactivates.

```{rubric} Example:
```
```{code-block} python
:caption: A connection pool resource is opened once and reused across scope activations

from stratae.lifecycle import Lifecycle, Scope, resource

lifecycle = Lifecycle([Scope("application", "shared")])

class ConnectionPool:
    open_count = 0

    def __init__(self):
        ConnectionPool.open_count += 1
        self.closed = False

    def close(self):
        self.closed = True

@lifecycle.cache("application")
@resource
def get_pool():
    pool = ConnectionPool()
    try:
        yield pool
    finally:
        pool.close()

with lifecycle.start("application"):
    pool = get_pool()
    pool_again = get_pool()  # same pool, not reopened

assert pool is pool_again
assert ConnectionPool.open_count == 1
assert pool.closed  # closed once the "application" activation ends
```

See {py:func}`resource` and {py:func}`async_resource` for additional examples.
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
    directly, and tags the result so
    {py:meth}`Lifecycle.cache <stratae.lifecycle.lifecycle.Lifecycle.cache>`
    enters it automatically instead of caching the raw context manager
    object. Use {py:func}`async_resource` for an async generator function
    instead.

    :param func: Generator function yielding exactly one value, in the shape
        `contextlib.contextmanager` expects.
    :returns: The wrapped context manager function, tagged for auto-entry.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Open a temp file once per request scope, closing it when the scope exits

    import tempfile
    from stratae.lifecycle import Lifecycle, Scope, resource

    lifecycle = Lifecycle([Scope("request", "context")])

    @lifecycle.cache("request")
    @resource
    def get_temp_file():
        file = tempfile.TemporaryFile()
        try:
            yield file
        finally:
            file.close()

    with lifecycle.start("request"):
        file = get_temp_file()
        assert not file.closed

    assert file.closed
    ```

    """
    unwrap(func).__auto_enter__ = AUTO_ENTER_SYNC
    return contextmanager(func)


def async_resource[**P, T](func: Callable[P, AsyncGenerator[T, None]]):
    """
    Mark an async generator function as an auto-entered async context manager.

    Wraps `func` with `contextlib.asynccontextmanager`, same as calling that
    directly, and tags the result so
    {py:meth}`AsyncLifecycle.cache <stratae.lifecycle.lifecycle.AsyncLifecycle.cache>`
    enters it automatically instead of caching the raw context manager
    object. Use {py:func}`resource` for a sync generator function instead.

    :param func: Async generator function yielding exactly one value, in the
        shape `contextlib.asynccontextmanager` expects.
    :returns: The wrapped async context manager function, tagged for auto-entry.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Open a remote client once per request scope, closing it when the scope exits

    import asyncio
    from stratae.lifecycle import AsyncLifecycle, Scope, async_resource

    class RemoteClient:
        connected = False

        async def connect(self):
            self.connected = True

        async def disconnect(self):
            self.connected = False

    lifecycle = AsyncLifecycle([Scope("request", "context")])

    @lifecycle.cache("request")
    @async_resource
    async def get_client():
        client = RemoteClient()
        await client.connect()
        try:
            yield client
        finally:
            await client.disconnect()

    async def main():
        async with lifecycle.start("request"):
            client = await get_client()
            assert client.connected
        assert not client.connected

    asyncio.run(main())
    ```

    """
    unwrap(func).__auto_enter__ = AUTO_ENTER_ASYNC
    return asynccontextmanager(func)
