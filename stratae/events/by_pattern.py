"""
A value for each event pattern an adapter supports, found by an event's class.

{py:class}`ByPattern` is how every adapter says which patterns it takes, and refuses the rest
the same way, with `TypeError`. A registry holds a dispatcher for each pattern it delivers, a
routed producer what makes a route for each pattern it publishes, and a consumer whether it
handles a pattern at all. A subclass of a pattern is that pattern, unless given a value of its
own.

````{example} Supporting one pattern, and refusing the rest
```{code-block} python
from stratae.events import PubSub, Request
from stratae.events.by_pattern import ByPattern

supported = ByPattern("Publisher", [(PubSub, "publish")])

print(supported(PubSub(name="tick")))
try:
    supported(Request(name="ping"))
except TypeError as error:
    print(error)
```
```{output}
publish
Publisher doesn't support Request events
```
````
"""

from collections.abc import Callable, Iterable
from functools import singledispatch
from typing import Any, cast

from stratae.events.protocols import AnyEvent

_UNSUPPORTED = "{owner} doesn't support {pattern} events"

_BECAUSE = "{refusal}: {reason}"

_NO_VALUE = "a pattern's value can't be None; give a value that says what the owner does with it"


def _unset(_pattern: type) -> Any:
    """Mark a pattern with no value. Stored as singledispatch's default, never called."""
    ...


class ByPattern[Value]:
    """
    A value for each pattern, found by an event's class; later pairs win.

    Immutable once built. Calling it with an event returns that event's pattern's value.
    """

    __slots__ = ("_find", "_owner", "_reason")

    def __init__(
        self, owner: str, pairs: Iterable[tuple[type, Value]], *, reason: str | None = None
    ) -> None:
        """
        Hold a value for each pattern.

        :param owner: The owner's class name, for the refusal's message.
        :param pairs: Each pattern class and its value. A later pair for the same pattern wins.
        :param reason: Why the owner supports only these patterns, added to the refusal.
        :raises ValueError: If a value is `None`.
        """
        find = singledispatch(_unset)
        for pattern, value in pairs:
            if value is None:
                raise ValueError(_NO_VALUE)
            find.register(pattern, cast(Callable[..., Any], value))
        self._find: Callable[[type], Any] = find.dispatch
        self._owner = owner
        self._reason = reason

    def __call__(self, event: AnyEvent) -> Value:
        """
        Return the value for the event's pattern.

        :raises TypeError: If the owner supports no pattern the event is.
        """
        value = self._find(type(event))
        if value is _unset:
            refusal = _UNSUPPORTED.format(owner=self._owner, pattern=type(event).__name__)
            if self._reason is not None:
                refusal = _BECAUSE.format(refusal=refusal, reason=self._reason)
            raise TypeError(refusal)
        return value
