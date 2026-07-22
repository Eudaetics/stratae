"""
Wrap event handler callables with async detection and identity semantics.

{py:class}`Handler` stores a callable together with its adapter-specific
routing config, and detects at construction time whether the callable is a
coroutine function. Dispatchers can branch on `is_async` once instead of
re-inspecting the callable on every dispatch. Calling a {py:class}`Handler`
delegates directly to the wrapped callable, so an async handler's coroutine
comes back from `__call__` for the caller to await.

Registration produces a distinct `Handler` instance each time, using plain
object identity rather than equality on the wrapped callable. That instance
is the token a caller holds onto to unregister later: `remove` takes back
the exact `Handler` a prior `handle` call returned. See
{py:class}`Consumer <stratae.events.protocols.Consumer>` for how adapters
expose `handle` and `remove`.

```{rubric} Example:
```
```{code-block} python
:caption: Wrapping a sync callable and detecting its async-ness

from stratae.events.handler import Handler

def greet(name: str) -> str:
    return f"hello {name}"

handler = Handler(greet, config="on-greet")
assert handler.is_async is False
assert handler(name="Sam") == "hello Sam"
```

"""

from inspect import iscoroutinefunction
from typing import Any, Callable


class Handler[**P, HandlerConfig: Any, R]:
    """
    Wrap an event handler callable with async detection and identity semantics.

    Stores the callable together with `HandlerConfig`, the adapter-specific
    routing config used to key registrations, and detects at construction
    time whether the callable is a coroutine function, so dispatchers can
    check `is_async` once instead of re-inspecting the callable on every
    dispatch.

    Identity is plain object identity. Each registration produces a
    distinct `Handler` instance, so the same callable can be registered
    multiple times, with different config or independently, without being
    deduplicated. Callers remove a registration by passing back the exact
    `Handler` instance it returned.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Wrapping an async callable for a dispatcher to detect

    import asyncio
    from stratae.events.handler import Handler

    async def notify(order_id: int) -> None:
        await asyncio.sleep(0)

    handler = Handler(notify, config="order-placed")
    assert handler.is_async is True
    asyncio.run(handler(order_id=42))
    ```

    """

    __slots__ = ("call", "config", "is_async")

    def __init__(self, call: Callable[P, R], config: HandlerConfig) -> None:
        """
        Store the callable and its routing config, detecting async-ness once.

        :param call: The sync or async callable to wrap. Must accept a single
            payload instance as its argument.
        :param config: The adapter-specific routing config for this handler.
        """
        self.call = call
        self.config = config
        self.is_async: bool = iscoroutinefunction(call)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Invoke the wrapped callable with the given arguments.

        For a sync handler, returns its return value directly. For an async
        handler, returns the coroutine object; the caller must await it,
        since a sync `__call__` wrapping an async callable naturally returns
        the coroutine without `__call__` itself needing to be `async`.

        :returns: The wrapped callable's return value for a sync handler, or
            an unawaited coroutine for an async handler.
        """
        return self.call(*args, **kwargs)
