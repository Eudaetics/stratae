"""
Inspecting callables.

{py:func}`is_async_callable` tells whether calling something gives a coroutine. Stratae uses
it wherever it treats sync and async callables differently: checks, dependencies, cached
functions, event handlers, hooks and bindings. Libraries built on Stratae can use it to make
the same choices the same way.

{py:func}`call_result` tells what calling something gives: a value, a coroutine, a generator or an
async generator, as a {py:class}`Gives`. It looks through the same shapes as
`is_async_callable`. It also marks a callable whose decorator hides a function that gives
something else, since that callable may still give it.

{py:func}`unwrap_partial` gives the callable behind any `functools.partial` objects. That's
the callable `is_async_callable` inspects, and the one to check for its kind, such as whether
it's a function.

````{example} Telling sync and async callables apart
```{code-block} python
from functools import partial
from stratae.callables import is_async_callable

class Client:
    async def __call__(self) -> str:
        return "sent"

async def send(to: str) -> str:
    return f"sent to {to}"

print(is_async_callable(send))
print(is_async_callable(partial(send, "ops")))
print(is_async_callable(Client()))
print(is_async_callable(Client))
```
```{output}
True
True
True
False
```
````

The last is false because calling `Client` builds an instance; it doesn't give a coroutine.
"""

from collections.abc import Awaitable, Callable
from enum import Enum
from functools import partial
from inspect import isasyncgenfunction, iscoroutinefunction, isgeneratorfunction, unwrap
from typing import Any, NamedTuple, TypeGuard


def unwrap_partial(fn: Callable[..., Any]) -> Callable[..., Any]:
    """
    Return the callable a `functools.partial` calls, through any number of them.

    Anything that isn't a `functools.partial` is returned as it is.

    :param fn: The callable to unwrap.
    :returns: The callable behind every `functools.partial` around it.
    """
    target = fn
    while isinstance(target, partial):
        target = target.func
    return target


def is_async_callable[**P, R](
    fn: Callable[P, R] | Callable[P, Awaitable[R]],
) -> TypeGuard[Callable[P, Awaitable[R]]]:
    """
    Tell whether calling `fn` gives a coroutine.

    True for async functions and methods, objects whose class defines `async def __call__`,
    functions marked with `inspect.markcoroutinefunction`, and `functools.partial` objects of
    any of these. False for everything else, including classes, since calling a class builds
    an instance, and async generator functions, since calling one gives an async generator.

    Narrows `fn` to its async form for type checkers.

    :param fn: The callable to inspect.
    :returns: Whether calling `fn` gives a coroutine.
    """
    return _gives(unwrap_partial(fn)) is Gives.COROUTINE


class Gives(Enum):
    """What calling a callable gives."""

    VALUE = "a value"
    COROUTINE = "a coroutine"
    GENERATOR = "a generator"
    ASYNC_GENERATOR = "an async generator"

    @property
    def awaits(self) -> bool:
        """Whether what's given needs an event loop: it's awaited, or iterated with `async for`."""
        return self is Gives.COROUTINE or self is Gives.ASYNC_GENERATOR


class CallResult(NamedTuple):
    """What calling a callable gives, as {py:func}`call_result` tells it."""

    gives: Gives
    """What calling it gives."""
    hidden: bool
    """Whether that's only what a function its decorator hides gives. The decorator may have
    changed it, so each result has to be checked."""


def _gives(target: object) -> Gives:
    """
    Return what calling `target` gives, read from it or from its class's `__call__`.

    Python looks `__call__` up on the type, so a class's own `__call__` is its metaclass's. A
    coroutine function is checked for first, so `inspect.markcoroutinefunction` overrides what
    the function's code says.
    """
    for candidate in (target, type(target).__call__):
        if iscoroutinefunction(candidate):
            return Gives.COROUTINE
        if isasyncgenfunction(candidate):
            return Gives.ASYNC_GENERATOR
        if isgeneratorfunction(candidate):
            return Gives.GENERATOR
    return Gives.VALUE


def call_result(fn: Callable[..., Any]) -> CallResult:
    """
    Tell what calling `fn` gives.

    Reads `fn` the way {py:func}`is_async_callable` does: through `functools.partial` objects,
    and from the `__call__` of an object's class. A callable that gives a value, but whose
    decorator hides a function giving something else, is marked `hidden`, with what the hidden
    function gives. The decorator is found through `__wrapped__`, which `functools.wraps` sets.

    :param fn: The callable to inspect.
    :returns: What calling `fn` gives, and whether that's hidden behind a decorator.
    """
    target = unwrap_partial(fn)
    gives = _gives(target)
    if gives is not Gives.VALUE:
        return CallResult(gives, hidden=False)
    inner = unwrap(target)
    hidden = Gives.VALUE if inner is target else _gives(inner)
    return CallResult(hidden, hidden=hidden is not Gives.VALUE)
