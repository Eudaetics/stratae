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

````{example} Caching an auto-entered resource
```{code-block} python
from stratae.lifecycle import Lifecycle, Scope, resource

class AuditLog:
    def __init__(self, path):
        self.path = path
        print(f"Opening {path}")

    def write(self, line):
        print(f"{self.path}: {line}")

    def close(self):
        print(f"Closing {self.path}")

lifecycle = Lifecycle([Scope("request", "context")])

@lifecycle.cache("request")
@resource
def get_audit_log():
    log = AuditLog("audit.log")
    try:
        yield log
    finally:
        log.close()

with lifecycle.start("request"):
    get_audit_log().write("user logged in")
    get_audit_log().write("user viewed dashboard")
```
```{output}
Opening audit.log
audit.log: user logged in
audit.log: user viewed dashboard
Closing audit.log
```
````

See {py:func}`resource` and {py:func}`async_resource` for the rest of the module's API.
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
    """
    unwrap(func).__auto_enter__ = AUTO_ENTER_ASYNC
    return asynccontextmanager(func)
