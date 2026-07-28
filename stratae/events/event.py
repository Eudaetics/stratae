"""
Define events as payload types bound to a dispatch pattern.

An event definition captures what an event IS: a payload schema and a
dispatch pattern, independent of any bus, factory, or routing config.
{py:class}`DispatchPattern` subclasses describe the dispatch pattern.
{py:class}`PubSub` is fire and forget. {py:class}`Request` is
request/reply; subscript it with the reply type, e.g. `Request[BookFound]`.

{py:class}`Event` binds a payload schema to a `DispatchPattern`
discriminant. {py:func}`is_request` and {py:func}`reply_type` inspect an
`Event`'s discriminant: {py:func}`is_request` reports whether it's a
subscripted `Request`, and {py:func}`reply_type` recovers the reply type
from one.

````{example} Reusing one event definition across bus instances
```{code-block} python
from stratae.events import DirectBus, Event, PubSub

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

log_message_event = Event(PubSub, LogMessage)

# The same Event carries no bus-specific state, so it binds to any
# number of different bus instances.
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

See {py:class}`Event`, {py:func}`is_request`, and {py:func}`reply_type` for
the rest of the module's API.

"""

from __future__ import annotations

from typing import cast, get_args, get_origin

_UNSUBSCRIPTED_REQUEST = "Request must be subscripted with its reply type (e.g. Request[BookFound])"
_NOT_A_REQUEST = "event does not carry a subscripted Request discriminant"


class DispatchPattern:
    """Marker base class for dispatch pattern discriminants."""


class PubSub(DispatchPattern):
    """
    Fire-and-forget dispatch pattern discriminant.

    Emitting an event with this discriminant returns immediately; there is
    no reply to wait for. Pass it as the `pattern` argument to
    {py:class}`Event`.
    """


class Request[Reply](DispatchPattern):
    """
    Request/reply dispatch pattern discriminant.

    Emitting an event with this discriminant blocks until a responder
    returns a `Reply` value. Always subscript with the reply type, e.g.
    `Request[BookFound]`.

    {py:class}`Event` rejects a bare, unsubscripted `Request` since the
    reply type could not be recovered at runtime for dispatch or
    deserialization. Use {py:func}`reply_type` to recover it from a
    request event's discriminant.
    """


def _validate_pattern(pattern: type[DispatchPattern]):
    """Raise `TypeError` if `pattern` is an unsubscripted `Request`."""
    if get_origin(pattern) is None and issubclass(pattern, Request):
        raise TypeError(_UNSUBSCRIPTED_REQUEST)


class Event[T: DispatchPattern, E]:
    """
    Bus-agnostic event definition binding a schema to a dispatch pattern.

    An `Event` captures what an event IS: the payload schema and the
    dispatch pattern, independent of any bus, factory, or routing config.
    It's the shareable definition that both a producer's `bind` and a
    consumer's `handle` reference.
    """

    __slots__ = ("name", "pattern", "schema")

    def __init__(
        self,
        pattern: type[T],
        schema: type[E],
        *,
        name: str | None = None,
    ) -> None:
        """
        Define an event with a dispatch pattern and schema type.

        :param pattern: The dispatch pattern discriminant class.
        :param schema: The payload type this event carries.
        :param name: Human-readable identifier for this event. Defaults to
            `schema.__name__`.
        :raises TypeError: If `pattern` is an unsubscripted `Request`.

        """
        _validate_pattern(pattern)
        self.pattern = pattern
        self.schema = schema
        self.name = name if name is not None else schema.__name__


def is_request[T: DispatchPattern, S](event: Event[T, S]) -> bool:
    """
    Report whether the event carries a subscripted Request discriminant.

    Adapters branch on this at dispatch time to select request/reply
    semantics over fire-and-forget dispatch.

    :param event: Any {py:class}`Event`.
    :returns: `True` when the event's discriminant is a subscripted
        {py:class}`Request` (or a subscripted subclass of it), `False`
        otherwise.
    """
    origin: object = get_origin(event.pattern)
    return isinstance(origin, type) and issubclass(origin, Request)


def reply_type[R, S](event: Event[Request[R], S]) -> type[R]:
    """
    Recover the reply type from a request event's discriminant.

    Adapters use this to know what a blocking emit resolves to at runtime.
    For example, to deserialize a reply arriving from the far side of a
    broker.

    :param event: An {py:class}`Event` whose discriminant is a subscripted
        {py:class}`Request`.
    :returns: The type `Request` was subscripted with when the event was
        defined.
    :raises TypeError: If the event's discriminant is not a subscripted
        `Request`. Unreachable for type-checked callers; guards dynamic
        construction paths.
    """
    if not is_request(event):
        raise TypeError(_NOT_A_REQUEST)
    return cast(type[R], get_args(event.pattern)[0])
