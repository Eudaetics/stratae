"""
Wrap event handler callables with their routing config, compared by identity.

{py:class}`Handler` pairs a callable with its routing config and records whether it's async.
Each registration returns a distinct `Handler`, which is the token `remove` takes back.
{py:class}`Handlers` holds an event's handlers as an immutable tuple, which a bus replaces
when a handler is added or removed. {py:class}`WithoutPayload` adapts a payload-less event's
handler, so an adapter can call every handler with one argument, and {py:func}`invoke` calls
one, sync or async, from async code.

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

from collections.abc import Callable, Iterable
from typing import Any, Self


class Handler[**P, HandlerConfig, R]:
    """
    An event handler callable with its routing config.

    Compared by identity, so the same callable can be registered more than once.
    """

    __slots__ = ("call", "config", "is_async")

    def __init__(self, call: Callable[P, R], config: HandlerConfig, is_async: bool) -> None:
        """
        Wrap a handler.

        :param call: Sync or async callable taking the payload: what the bus invokes, which may
            wrap the registered function, e.g. in hooks.
        :param config: The handler's routing config.
        :param is_async: Whether `call` is async: whether calling it gives a coroutine.
        """
        self.call = call
        self.config = config
        self.is_async = is_async

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Call the handler. An async handler returns an unawaited coroutine."""
        return self.call(*args, **kwargs)


class Handlers[H: Handler[Any, Any, Any]](tuple[H, ...]):
    """
    An event's registered handlers, as an immutable tuple.

    A bus never changes it. Registering or removing a handler makes a new one, so a dispatcher
    can run the handlers it was given while others register. The sync and async handlers are
    also kept apart, once, for dispatchers that run them differently.
    """

    sync: tuple[H, ...]
    """The handlers that aren't async."""
    asynchronous: tuple[H, ...]
    """The async handlers."""

    def __new__(cls, handlers: Iterable[H] = ()) -> Self:
        """
        Hold `handlers`, keeping the sync and async ones apart.

        :param handlers: The registered handlers.
        """
        instance = super().__new__(cls, handlers)
        instance.sync = tuple(handler for handler in instance if not handler.is_async)
        instance.asynchronous = tuple(handler for handler in instance if handler.is_async)
        return instance

    def adding(self, handler: H) -> Self:
        """Return these handlers with `handler` added."""
        return type(self)((*self, handler))

    def removing(self, handler: H) -> Self:
        """Return these handlers without `handler`, or these same handlers if it isn't one."""
        if handler not in self:
            return self
        return type(self)(registered for registered in self if registered is not handler)


class WithoutPayload:
    """
    Adapt a payload-less event's handler to take, and ignore, one argument.

    Lets an adapter call every handler with one argument, whatever the event.
    """

    __slots__ = ("fn",)

    def __init__(self, fn: Callable[[], Any]) -> None:
        """
        Wrap a handler.

        :param fn: The handler, taking no arguments.
        """
        self.fn = fn

    def __call__(self, _: object, /) -> Any:
        """Call the handler without the argument."""
        return self.fn()


async def invoke(handler: Handler[..., Any, Any], *args: Any) -> Any:
    """
    Call `handler(*args)`, awaiting it if it's async.

    How an async adapter calls a registered handler, sync or async, such as on each delivery.

    :param handler: The registered handler.
    :returns: What the handler returns, or resolves to.
    """
    if handler.is_async:
        return await handler(*args)
    return handler(*args)
