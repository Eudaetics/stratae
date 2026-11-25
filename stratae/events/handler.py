"""
Event handlers as values: a function, its hooks and its routing config.

{py:class}`Handler` is what a bus registers. It holds the function given to `handle`, its
hooks, and its routing config, such as the event. It also holds the call the bus invokes,
built from them with the bus's handle middleware, and records whether that call is async.
Handlers are compared and hashed by their function, hooks and config, so two registrations of
the same function with the same hooks are equal. A bus registers equal handlers once, and
`remove` takes back the handler `handle` returned, or any equal one.
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

from collections.abc import Awaitable, Callable, Coroutine, Iterable
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, Self

from stratae.events.hooks import Hooks
from stratae.events.middleware import HandleCall


@dataclass(frozen=True, slots=True)
class SyncHandler[HandlerConfig, Payload, **P, Result]:
    """
    A sync event handler: calling it gives its result.

    Typed by its config, the payload its hooks see, the function's parameters and its result.
    Compared and hashed by its function, hooks and config, never by the call built from them.
    """

    fn: Callable[P, Result]
    """The function given to `handle`."""
    hooks: Hooks[Payload, Result] | None
    """The hooks run around it, or `None`."""
    call: Callable[[HandleCall], Result] = field(compare=False)
    """What the bus invokes: `fn` inside its hooks and the bus's handle middleware."""
    config: HandlerConfig
    """The config it was registered with, such as its event."""
    is_async: ClassVar[Literal[False]] = False
    """Whether calling it gives an awaitable: never."""

    def __call__(self, message: HandleCall, /) -> Result:
        """Call the handler with a message."""
        return self.call(message)


@dataclass(frozen=True, slots=True)
class AsyncHandler[HandlerConfig, Payload, **P, Result]:
    """
    An async event handler: calling it gives a coroutine of its result.

    Typed by its config, the payload its hooks see, the function's parameters and its result.
    Compared and hashed by its function, hooks and config, never by the call built from them.
    """

    fn: Callable[P, Result] | Callable[P, Awaitable[Result]]
    """The function given to `handle`, sync or async."""
    hooks: Hooks[Payload, Result] | None
    """The hooks run around it, or `None`."""
    call: Callable[[HandleCall], Coroutine[Any, Any, Result]] = field(compare=False)
    """What the bus invokes: `fn` inside its hooks and the bus's handle middleware."""
    config: HandlerConfig
    """The config it was registered with, such as its event."""
    is_async: ClassVar[Literal[True]] = True
    """Whether calling it gives an awaitable: always."""

    def __call__(self, message: HandleCall, /) -> Coroutine[Any, Any, Result]:
        """Call the handler with a message, returning a coroutine of its result."""
        return self.call(message)


type Handler[HandlerConfig, Payload, **P, Result] = (
    SyncHandler[HandlerConfig, Payload, P, Result] | AsyncHandler[HandlerConfig, Payload, P, Result]
)
"""
A registered event handler, sync or async, typed as its kinds are.

A bus's `handle` returns one, to pass to `remove`. Its `is_async` says which kind it is.
"""


class Handlers[HandlerConfig, Payload, Result](
    tuple[Handler[HandlerConfig, Payload, ..., Result], ...]
):
    """
    An event's registered handlers, as an immutable tuple.

    A bus never changes it. Registering or removing a handler makes a new one, so a dispatcher
    can run the handlers it was given while others register. The sync and async handlers are
    also kept apart, once, for dispatchers that run them differently. A sync bus has only sync
    handlers.
    """

    sync: tuple[SyncHandler[HandlerConfig, Payload, ..., Result], ...]
    """The sync handlers."""
    asynchronous: tuple[AsyncHandler[HandlerConfig, Payload, ..., Result], ...]
    """The async handlers."""
    _members: frozenset[Handler[HandlerConfig, Payload, ..., Result]]

    def __new__(cls, handlers: Iterable[Handler[HandlerConfig, Payload, ..., Result]] = ()) -> Self:
        """
        Hold `handlers`, keeping the sync and async ones apart.

        :param handlers: The registered handlers.
        :raises TypeError: If a handler's function, hooks or config is unhashable.
        """
        instance = super().__new__(cls, handlers)
        sync: list[SyncHandler[HandlerConfig, Payload, ..., Result]] = []
        asynchronous: list[AsyncHandler[HandlerConfig, Payload, ..., Result]] = []
        for handler in instance:
            if handler.is_async is True:
                asynchronous.append(handler)
            else:
                sync.append(handler)
        instance.sync = tuple(sync)
        instance.asynchronous = tuple(asynchronous)
        instance._members = frozenset(instance)
        return instance

    def __contains__(self, handler: object, /) -> bool:
        """
        Whether a handler equal to `handler` is among these, found by its hash.

        :raises TypeError: If `handler`'s function, hooks or config is unhashable.
        """
        return handler in self._members

    def adding(self, handler: Handler[HandlerConfig, Payload, ..., Result]) -> Self:
        """Return these handlers with `handler` added."""
        return type(self)((*self, handler))

    def removing(self, handler: Handler[HandlerConfig, Payload, ..., Result]) -> Self:
        """Return these handlers without `handler`, or these same handlers if it isn't one."""
        if handler not in self:
            return self
        return type(self)(registered for registered in self if registered != handler)


async def invoke[HandlerConfig, Payload, Result](
    handler: Handler[HandlerConfig, Payload, ..., Result], message: HandleCall
) -> Result:
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
