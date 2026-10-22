"""
Define events as payload types bound to a dispatch pattern.

An {py:class}`Event` pairs a payload schema with a dispatch pattern, independent of any
bus. {py:class}`PubSub` is fire-and-forget. {py:class}`Request` is request/reply,
subscripted with the reply type, e.g. `Request[BookFound]`. {py:func}`is_request` and
{py:func}`reply_type` inspect an event's pattern.

````{example} Reusing one event definition across bus instances
```{code-block} python
from stratae.events import DirectBus, Event, PubSub

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

log_message_event = Event(PubSub, LogMessage)

primary = DirectBus()
audit = DirectBus()

primary_log = primary.bind(log_message_event, factory=LogMessage)
audit_log = audit.bind(log_message_event, factory=LogMessage)

@primary.handle(log_message_event)
def write_to_log(entry: LogMessage) -> None:
    print(f"log: {entry.text}")

primary_log(text="hello")
```
```{output}
log: hello
```
````
"""

from __future__ import annotations

from typing import Any, Literal, cast, get_args, get_origin, overload

_UNSUBSCRIPTED_REQUEST = "Request must be subscripted with its reply type (e.g. Request[BookFound])"
_NOT_A_REQUEST = "event does not carry a subscripted Request discriminant"
_NOT_A_DISPATCH_PATTERN = (
    "pattern must be a DispatchPattern subclass (e.g. PubSub or Request[...]), got {!r}"
)


class DispatchPattern[EmitR, HandleR]:
    """Marker base class for dispatch pattern discriminants."""


class PubSub(DispatchPattern[None, Any]):
    """Fire-and-forget dispatch pattern. Handler return values are ignored."""


class Request[Reply](DispatchPattern[Reply, Reply]):
    """
    Request/reply dispatch pattern, subscripted with the reply type, e.g. `Request[BookFound]`.

    {py:class}`Event` rejects a bare `Request`, since the reply type is needed at runtime.
    """


def _is_dispatch_pattern(discriminant: object) -> bool:
    """Return whether `discriminant` is a `DispatchPattern` subclass."""
    return isinstance(discriminant, type) and issubclass(discriminant, DispatchPattern)


def _is_unsubscripted_request(pattern: type[DispatchPattern[Any, Any]], origin: object) -> bool:
    """Return whether `pattern` is a bare, unsubscripted `Request`."""
    return origin is None and issubclass(pattern, Request)


def _validate_pattern(pattern: type[DispatchPattern[Any, Any]]):
    """Raise `TypeError` if `pattern` isn't a valid `DispatchPattern` subclass."""
    origin = get_origin(pattern)
    if not _is_dispatch_pattern(origin or pattern):
        raise TypeError(_NOT_A_DISPATCH_PATTERN.format(pattern))
    if _is_unsubscripted_request(pattern, origin):
        raise TypeError(_UNSUBSCRIPTED_REQUEST)


class NoPayload:
    """Schema for an event with no payload, distinct from a payload of `None`."""


class Event[T: DispatchPattern[Any, Any], S, Signal: bool]:
    """
    Bus-agnostic event definition binding a schema to a dispatch pattern.

    Shared by a producer's `bind` and a consumer's `handle`. Without a `schema`, the event
    carries no payload and binds to a zero-argument callable. `Signal` is a type-only
    parameter marking payload-less events.
    """

    __slots__ = ("name", "_pattern", "schema")

    @overload
    def __init__[T2: DispatchPattern[Any, Any]](
        self: Event[T2, NoPayload, Literal[True]],
        pattern: type[T2],
        *,
        name: str,
    ) -> None: ...
    @overload
    def __init__[T2: DispatchPattern[Any, Any], S2](
        self: Event[T2, S2, Literal[False]],
        pattern: type[T2],
        schema: type[S2],
        *,
        name: str | None = None,
    ) -> None: ...
    def __init__(
        self,
        pattern: type[Any],
        schema: type[Any] = NoPayload,
        *,
        name: str | None = None,
    ) -> None:
        """
        Define an event.

        :param pattern: The dispatch pattern.
        :param schema: The payload type. Omit for a payload-less event.
        :param name: Identifier for the event. Defaults to `schema.__name__`; required without
            a schema.
        :raises TypeError: If `pattern` is a bare `Request`, or a payload-less event has no
            `name`.
        """
        _validate_pattern(pattern)
        if name is None and schema is NoPayload:
            raise TypeError(
                "A payload-less Event needs a name, e.g. Event(PubSub, name='heartbeat')."
            )
        self._pattern = pattern
        self.schema = schema
        self.name = name if name is not None else schema.__name__

    @property
    def pattern(self) -> type[T]:
        """The dispatch pattern; read-only so `T` is covariant."""
        return self._pattern


def is_request[T: DispatchPattern[Any, Any], S, Signal: bool](event: Event[T, S, Signal]) -> bool:
    """
    Whether the event's pattern is a subscripted {py:class}`Request`.

    :param event: Any {py:class}`Event`.
    :returns: `True` for a request event, otherwise `False`.
    """
    origin: object = get_origin(event.pattern)
    return isinstance(origin, type) and issubclass(origin, Request)


def reply_type[R, S, Signal: bool](event: Event[Request[R], S, Signal]) -> type[R]:
    """
    Return the reply type of a request event.

    :param event: An {py:class}`Event` with a subscripted {py:class}`Request` pattern.
    :returns: The reply type.
    :raises TypeError: If the event isn't a request event.
    """
    if not is_request(event):
        raise TypeError(_NOT_A_REQUEST)
    return cast(type[R], get_args(event.pattern)[0])
