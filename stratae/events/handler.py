"""
Wrap event handler callables with async detection and identity semantics.

{py:class}`Handler` pairs a callable with its routing config and records whether it's async.
Each registration returns a distinct `Handler`, which is the token `remove` takes back.

````{example} Using a returned Handler to unregister it later
```{code-block} python
from stratae.events import DirectBus, Event, PubSub

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

log_message_event = Event(PubSub, LogMessage)
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

from typing import Callable

from stratae._callables import is_async_callable


class Handler[**P, HandlerConfig, R]:
    """
    An event handler callable with its routing config.

    Compared by identity, so the same callable can be registered more than once.
    """

    __slots__ = ("call", "config", "is_async")

    def __init__(
        self, call: Callable[P, R], config: HandlerConfig, is_async: bool | None = None
    ) -> None:
        """
        Wrap a handler.

        :param call: Sync or async callable taking the payload.
        :param config: The handler's routing config.
        :param is_async: Whether `call` is async. Detected if omitted.
        """
        self.call = call
        self.config = config
        self.is_async = is_async_callable(call) if is_async is None else is_async

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Call the handler. An async handler returns an unawaited coroutine."""
        return self.call(*args, **kwargs)
