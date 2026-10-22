"""
Decorators marking a generator function as an auto-entered lifecycle resource.

{py:func}`resource` and {py:func}`async_resource` wrap a generator function like
`contextlib.contextmanager` and tag it, so
{py:meth}`Scope.cache <stratae.lifecycle.scope.Scope.cache>` enters it and caches the
yielded value. It exits when the scope deactivates.

````{example} Caching an auto-entered resource
```{code-block} python
from stratae.lifecycle import Scope, resource

class AuditLog:
    def __init__(self, path):
        self.path = path
        print(f"Opening {path}")

    def write(self, line):
        print(f"{self.path}: {line}")

    def close(self):
        print(f"Closing {self.path}")

request = Scope("request")

@request.cache()
@resource
def get_audit_log():
    log = AuditLog("audit.log")
    try:
        yield log
    finally:
        log.close()

with request:
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
"""

from contextlib import asynccontextmanager, contextmanager
from inspect import unwrap
from typing import Any, AsyncGenerator, Callable, Generator, cast

AUTO_ENTER_SYNC = object()
"""Sentinel tagging a function as a sync context manager for lifecycle to auto-enter."""

AUTO_ENTER_ASYNC = object()
"""Sentinel tagging a function as an async context manager for lifecycle to auto-enter."""


def resource[**P, T](func: Callable[P, Generator[T, None, None]]):
    """
    Mark a generator function as a resource that a scope cache enters automatically.

    :param func: A generator function yielding one value, as for `contextlib.contextmanager`.
    :returns: The tagged context manager function.
    """
    unwrap(func).__auto_enter__ = AUTO_ENTER_SYNC
    helper = contextmanager(func)
    cast(Any, helper).__resource_generator__ = func
    return helper


def async_resource[**P, T](func: Callable[P, AsyncGenerator[T, None]]):
    """
    Mark an async generator function as a resource that a scope cache enters automatically.

    :param func: An async generator function yielding one value, as for
        `contextlib.asynccontextmanager`.
    :returns: The tagged async context manager function.
    """
    unwrap(func).__auto_enter__ = AUTO_ENTER_ASYNC
    helper = asynccontextmanager(func)
    cast(Any, helper).__resource_generator__ = func
    return helper


def resource_generator(func: Callable[..., Any]) -> Callable[..., Any] | None:
    """
    Return the generator function behind `func`, if `func` is exactly a resource helper.

    A cache drives that generator itself, skipping the context manager. A decorator
    wrapping the helper copies the attribute but not the identity, so it's still called.
    """
    generator = getattr(func, "__resource_generator__", None)
    if generator is not None and getattr(func, "__wrapped__", None) is generator:
        return generator
    return None
