"""
Inspecting callables.

{py:func}`is_async_callable` tells whether calling something gives a coroutine. Stratae uses
it wherever it treats sync and async callables differently: checks, dependencies, cached
functions, event handlers, hooks and bindings. Libraries built on Stratae can use it to make
the same choices the same way.

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
from functools import partial
from inspect import iscoroutinefunction
from typing import TypeGuard


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
    target: object = fn
    while isinstance(target, partial):
        target = target.func
    # Python looks __call__ up on the type, so a class's own __call__ is its metaclass's.
    return iscoroutinefunction(target) or iscoroutinefunction(type(target).__call__)
