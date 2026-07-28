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

````{example} Using a returned Handler to unregister it later
```{code-block} python
from stratae.events import DirectBus, Event, PubSub

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

log_message_event = Event(LogMessage, PubSub)
bus = DirectBus()
log = bus.bind(log_message_event, factory=LogMessage)

@bus.handle(log_message_event)
def write_to_log(entry: LogMessage) -> None:
    print(f"log: {entry.text}")

log(text="first")

# handle's decorator form returns the Handler in place of the function it
# wraps, so write_to_log now holds the exact token remove needs.
bus.remove(write_to_log)
log(text="second")
```
```{output}
log: first
```
````

"""

from inspect import iscoroutinefunction
from typing import Callable


class Handler[**P, HandlerConfig, R]:
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
