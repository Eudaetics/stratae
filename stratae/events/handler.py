"""
Event handlers as values: a function, its hooks and its routing config.

{py:class}`Handler` is what a bus registers. It holds the function given to `handle`, its
hooks, and its routing config, such as the event. It also holds the call the bus invokes,
built from them with the bus's handle middleware, and records whether that call is async.
Handlers are compared and hashed by their function, hooks and config, so two registrations of
the same function with the same hooks are equal. A bus registers equal handlers once, and
`remove` takes back the handler `handle` returned, or any equal one.
{py:class}`Handlers` holds an event's handlers as an immutable value, which a bus replaces
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

from collections.abc import Awaitable, Callable, Coroutine, Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal, Self, cast

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


class Handlers[HandlerConfig, Payload, Result]:
    """
    An event's registered handlers, as an immutable value.

    A bus never changes it. Registering or removing a handler makes a new one, so a dispatcher
    can run the handlers it was given while others register. It has a length, iterates over its
    handlers in the order they were registered, and finds one by its hash with `in`. The sync
    and async handlers are also kept apart, once, for dispatchers that run them differently. A
    sync bus has only sync handlers. Two are equal when they hold the same handlers, in order.
    """

    __slots__ = ("_handlers", "_members", "asynchronous", "sync")

    sync: tuple[SyncHandler[HandlerConfig, Payload, ..., Result], ...]
    """The sync handlers."""
    asynchronous: tuple[AsyncHandler[HandlerConfig, Payload, ..., Result], ...]
    """The async handlers."""
    _handlers: tuple[Handler[HandlerConfig, Payload, ..., Result], ...]
    _members: frozenset[Handler[HandlerConfig, Payload, ..., Result]]

    def __init__(
        self, handlers: Iterable[Handler[HandlerConfig, Payload, ..., Result]] = ()
    ) -> None:
        """
        Hold `handlers`, keeping the sync and async ones apart.

        :param handlers: The registered handlers.
        :raises TypeError: If a handler's function, hooks or config is unhashable.
        """
        held = tuple(handlers)
        sync: list[SyncHandler[HandlerConfig, Payload, ..., Result]] = []
        asynchronous: list[AsyncHandler[HandlerConfig, Payload, ..., Result]] = []
        for handler in held:
            if handler.is_async is True:
                asynchronous.append(handler)
            else:
                sync.append(handler)
        self._handlers = held
        self.sync = tuple(sync)
        self.asynchronous = tuple(asynchronous)
        self._members = frozenset(held)

    def __len__(self) -> int:
        """Return how many handlers are registered."""
        return len(self._handlers)

    def __iter__(self) -> Iterator[Handler[HandlerConfig, Payload, ..., Result]]:
        """Iterate over the handlers, in the order they were registered."""
        return iter(self._handlers)

    def __contains__(self, handler: object, /) -> bool:
        """
        Whether a handler equal to `handler` is among these, found by its hash.

        :raises TypeError: If `handler`'s function, hooks or config is unhashable.
        """
        return handler in self._members

    def __eq__(self, other: object) -> bool:
        """Whether `other` holds the same handlers, in order."""
        if not isinstance(other, Handlers):
            return NotImplemented
        return self._handlers == cast("Handlers[Any, Any, Any]", other)._handlers

    def __hash__(self) -> int:
        """Return the hash of the handlers held."""
        return hash(self._handlers)

    def __repr__(self) -> str:
        """Show the handlers held."""
        return f"Handlers({self._handlers!r})"

    def adding(self, handler: Handler[HandlerConfig, Payload, ..., Result]) -> Self:
        """Return these handlers with `handler` added."""
        return type(self)((*self._handlers, handler))

    def removing(self, handler: Handler[HandlerConfig, Payload, ..., Result]) -> Self:
        """Return these handlers without `handler`, or these same handlers if it isn't one."""
        if handler not in self:
            return self
        return type(self)(registered for registered in self._handlers if registered != handler)


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
