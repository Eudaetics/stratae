"""
Define events as payload types bound to a dispatch pattern.

An event definition captures what an event IS: a payload schema and a
dispatch pattern, independent of any bus, factory, or routing config.
{py:class}`DispatchPattern` subclasses describe the dispatch pattern.
{py:class}`PubSub` is fire and forget. {py:class}`Request` is
request/reply; subscript it with the reply type, e.g. `Request[BookFound]`.

{py:class}`Event` binds a payload schema to a `DispatchPattern`
discriminant. Omitting the schema leaves it `NoPayload`, for events whose
occurrence is the whole message. {py:func}`is_request` and
{py:func}`reply_type` inspect an `Event`'s discriminant:
{py:func}`is_request` reports whether it's a subscripted `Request`, and
{py:func}`reply_type` recovers the reply type from one.

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

from typing import Any, Literal, cast, get_args, get_origin, overload

_UNSUBSCRIPTED_REQUEST = "Request must be subscripted with its reply type (e.g. Request[BookFound])"
_NOT_A_REQUEST = "event does not carry a subscripted Request discriminant"
_NOT_A_DISPATCH_PATTERN = (
    "pattern must be a DispatchPattern subclass (e.g. PubSub or Request[...]), got {!r}"
)


class DispatchPattern[EmitR, HandleR]:
    """Marker base class for dispatch pattern discriminants."""


class PubSub(DispatchPattern[None, Any]):
    """
    Fire-and-forget dispatch pattern discriminant.

    Emitting an event with this discriminant returns immediately; there is
    no reply to wait for. A handler's return value is unconstrained and
    ignored. Pass it as the `pattern` argument to {py:class}`Event`.
    """


class Request[Reply](DispatchPattern[Reply, Reply]):
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


def _is_dispatch_pattern(discriminant: object) -> bool:
    """Return whether `discriminant` is a `DispatchPattern` subclass."""
    return isinstance(discriminant, type) and issubclass(discriminant, DispatchPattern)


def _is_unsubscripted_request(pattern: type[DispatchPattern[Any, Any]], origin: object) -> bool:
    """Return whether `pattern` is the bare, unsubscripted `Request` (sub)class."""
    return origin is None and issubclass(pattern, Request)


def _validate_pattern(pattern: type[DispatchPattern[Any, Any]]):
    """Raise `TypeError` if `pattern` isn't a valid `DispatchPattern` subclass."""
    origin = get_origin(pattern)
    if not _is_dispatch_pattern(origin or pattern):
        raise TypeError(_NOT_A_DISPATCH_PATTERN.format(pattern))
    if _is_unsubscripted_request(pattern, origin):
        raise TypeError(_UNSUBSCRIPTED_REQUEST)


class NoPayload:
    """
    Sentinel schema for an Event that carries no payload at all.

    Distinct from `None`/`NoneType`, which remain available as an ordinary,
    if degenerate, payload type. Using a dedicated sentinel lets the type
    system tell "no payload" and "payload is `None`" apart, which collapsing
    onto `NoneType` could not.
    """


class Event[T: DispatchPattern[Any, Any], S, Signal: bool]:
    """
    Bus-agnostic event definition binding a schema to a dispatch pattern.

    An `Event` captures what an event IS: the payload schema and the
    dispatch pattern, independent of any bus, factory, or routing config.
    It's the shareable definition that both a producer's `bind` and a
    consumer's `handle` reference.

    `Signal` is a phantom type parameter, never read at runtime: `Literal[True]`
    for a payload-less event, `Literal[False]` for a schema'd one.

    Omitting `schema` leaves it `NoPayload`, defining an event that carries
    no payload at all. Its occurrence is the whole message: a heartbeat, a
    cache invalidation, a shutdown notice. Binding one produces a callable
    taking no arguments, since there's nothing to pass.
    """

    __slots__ = ("name", "_pattern", "schema")

    @overload
    def __init__[T2: DispatchPattern[Any, Any]](
        self: Event[T2, NoPayload, Literal[True]],
        pattern: type[T2],
        *,
        name: str | None = None,
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
        Define an event with a dispatch pattern and schema type.

        :param pattern: The dispatch pattern discriminant class.
        :param schema: The payload type this event carries. Omit it for a
            payload-less event, leaving the schema `NoPayload`.
        :param name: Human-readable identifier for this event. Defaults to
            `schema.__name__`.
        :raises TypeError: If `pattern` is an unsubscripted `Request`.

        """
        _validate_pattern(pattern)
        self._pattern = pattern
        self.schema = schema
        self.name = name if name is not None else schema.__name__

    @property
    def pattern(self) -> type[T]:
        """The dispatch pattern discriminant, read-only so pyright can infer `T` as covariant."""
        return self._pattern


def is_request[T: DispatchPattern[Any, Any], S, Signal: bool](event: Event[T, S, Signal]) -> bool:
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


def reply_type[R, S, Signal: bool](event: Event[Request[R], S, Signal]) -> type[R]:
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
