"""
Wrap event handler callables with their routing config, compared by identity.

{py:class}`Handler` pairs a callable with its routing config and records whether it's async.
Each registration returns a distinct `Handler`, which is the token `remove` takes back.
{py:class}`Handlers` holds an event's handlers as an immutable tuple, which a bus replaces
when a handler is added or removed. {py:func}`invoke` calls one, sync or async, from async
code.

````{example} Using a returned Handler to unregister it later
```{code-block} python
from stratae.events import DirectBus, PubSub

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

log_message_event = PubSub(LogMessage)
bus = DirectBus()
log = bus.bind(log_message_event, factory=LogMessage)

@bus.handle(log_message_event)
def write_to_log(entry: LogMessage) -> None:
    print(f"log: {entry.text}")

log(text="first")

# The decorator returned the Handler, so write_to_log is the token.
bus.remove(write_to_log)
log(text="second")
```
```{output}
log: first
```
````

"""

from collections.abc import Callable, Coroutine, Iterable
from typing import Any, ClassVar, Literal, Self

from stratae.events.middleware import HandleCall


class SyncHandler[Config, Result]:
    """
    A sync event handler with its routing config: calling it gives its result.

    Compared by identity, so the same callable can be registered more than once.
    """

    __slots__ = ("call", "config")

    is_async: ClassVar[Literal[False]] = False
    """Whether calling it gives an awaitable: never."""

    def __init__(self, call: Callable[[HandleCall], Result], config: Config) -> None:
        """
        Wrap a sync handler.

        :param call: Callable taking the message being handled: what the bus invokes, which
            wraps the registered function, and any hooks and middleware.
        :param config: The handler's routing config.
        """
        self.call: Callable[[HandleCall], Result] = call
        self.config: Config = config

    def __call__(self, message: HandleCall, /) -> Result:
        """Call the handler with a message."""
        return self.call(message)


class AsyncHandler[Config, Result]:
    """
    An async event handler with its routing config: calling it gives a coroutine of its result.

    Compared by identity, so the same callable can be registered more than once.
    """

    __slots__ = ("call", "config")

    is_async: ClassVar[Literal[True]] = True
    """Whether calling it gives an awaitable: always."""

    def __init__(
        self, call: Callable[[HandleCall], Coroutine[Any, Any, Result]], config: Config
    ) -> None:
        """
        Wrap an async handler.

        :param call: Callable taking the message being handled and returning a coroutine:
            what the bus invokes, which wraps the registered function, and any hooks and
            middleware.
        :param config: The handler's routing config.
        """
        self.call: Callable[[HandleCall], Coroutine[Any, Any, Result]] = call
        self.config: Config = config

    def __call__(self, message: HandleCall, /) -> Coroutine[Any, Any, Result]:
        """Call the handler with a message, returning a coroutine of its result."""
        return self.call(message)


type Handler[Config, Result] = SyncHandler[Config, Result] | AsyncHandler[Config, Result]
"""
A registered event handler, sync or async, typed by its config and what it produces.

A bus's `handle` returns one, to pass to `remove`. Its `is_async` says which kind it is.
"""


class Handlers[Config, Result](tuple[Handler[Config, Result], ...]):
    """
    An event's registered handlers, as an immutable tuple.

    A bus never changes it. Registering or removing a handler makes a new one, so a dispatcher
    can run the handlers it was given while others register. The sync and async handlers are
    also kept apart, once, for dispatchers that run them differently. A sync bus has only sync
    handlers.
    """

    sync: tuple[SyncHandler[Config, Result], ...]
    """The sync handlers."""
    asynchronous: tuple[AsyncHandler[Config, Result], ...]
    """The async handlers."""

    def __new__(cls, handlers: Iterable[Handler[Config, Result]] = ()) -> Self:
        """
        Hold `handlers`, keeping the sync and async ones apart.

        :param handlers: The registered handlers.
        """
        instance = super().__new__(cls, handlers)
        sync: list[SyncHandler[Config, Result]] = []
        asynchronous: list[AsyncHandler[Config, Result]] = []
        for handler in instance:
            if handler.is_async is True:
                asynchronous.append(handler)
            else:
                sync.append(handler)
        instance.sync = tuple(sync)
        instance.asynchronous = tuple(asynchronous)
        return instance

    def adding(self, handler: Handler[Config, Result]) -> Self:
        """Return these handlers with `handler` added."""
        return type(self)((*self, handler))

    def removing(self, handler: Handler[Config, Result]) -> Self:
        """Return these handlers without `handler`, or these same handlers if it isn't one."""
        if handler not in self:
            return self
        return type(self)(registered for registered in self if registered is not handler)


async def invoke[Config, Result](handler: Handler[Config, Result], message: HandleCall) -> Result:
    """
    Call `handler` with `message`, awaiting it if it's async.

    How an async adapter calls a registered handler, sync or async, such as on each delivery.

    :param handler: The registered handler.
    :param message: The message being handled.
    :returns: What the handler returns, or resolves to.
    """
    if handler.is_async is True:
        return await handler.call(message)
    return handler.call(message)
