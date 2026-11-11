"""
Define events as classes, one per dispatch pattern.

An event pairs a payload schema with a dispatch pattern, independent of any bus.
{py:class}`PubSub` is fire-and-forget: every handler runs, and returns `None`.
{py:class}`Request` is request/reply: exactly one responder, whose reply the emitter gets.
A `Request` without a `reply` type is a command, replying `None`. Without a schema, an event
carries no payload and binds to a zero-argument callable.

Events are immutable data, compared by value. Two separately built events with the same
pattern, name, payload type and reply type are equal and share a hash. A bus matches them as
one event, so a handler registered through one receives emits through the other.

How an event is delivered belongs to each bus, which supports the patterns it has a
dispatcher for. Buses type against {py:class}`EventType <stratae.events.protocols.EventType>`,
which every pattern implements.

Subclassing a pattern means "this is the same pattern": a `class Command(Request)` is
delivered exactly as a `Request` everywhere, unless a direct bus is given a dispatcher for it.
A subclass is never equal to its base, so the two are still separate events.

Write a new pattern as its own class instead. Declare it as a dataclass with
`init=False, unsafe_hash=True, slots=True`, using {py:class}`Immutable` to refuse changes. It
then compares by value, as buses expect. A pattern declared any other way still works, but
only the same object matches it. Don't use `frozen=True`: buses need the fields to be plain
attributes, as {py:class}`EventType <stratae.events.protocols.EventType>` explains.

````{example} Reusing one event definition across bus instances
```{code-block} python
from stratae.events import DirectBus, PubSub, Request

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

class ArchiveLogs:
    def __init__(self, before: str) -> None:
        self.before = before

log_message_event = PubSub(LogMessage)
archive_logs_event = Request(ArchiveLogs)  # a command: no reply type

primary = DirectBus()
audit = DirectBus()

primary_log = primary.bind(log_message_event, factory=LogMessage)
audit_log = audit.bind(log_message_event, factory=LogMessage)
archive = primary.bind(archive_logs_event, factory=ArchiveLogs)

@primary.handle(log_message_event)
def write_to_log(entry: LogMessage) -> None:
    print(f"log: {entry.text}")

@primary.handle(archive_logs_event)
def archive_old_logs(command: ArchiveLogs) -> None:
    print(f"archived logs before {command.before}")

primary_log(text="hello")
print(archive(before="2026-01-01"))
```
```{output}
log: hello
archived logs before 2026-01-01
None
```
````
"""

from dataclasses import dataclass, field
from types import NoneType
from typing import Any, Literal, NoReturn, overload

_NAME_REQUIRED = "A payload-less {0} needs a name, e.g. {0}(name='heartbeat')."

_IMMUTABLE = "{} is immutable"


class NoPayload:
    """Schema for an event with no payload, distinct from a payload of `None`."""


def _event_name(pattern: str, schema: type[Any], name: str | None) -> str:
    """Return `name`, or the schema's name if omitted."""
    if name is not None:
        return name
    if schema is NoPayload:
        raise TypeError(_NAME_REQUIRED.format(pattern))
    return schema.__name__


class Immutable:
    """
    Make a slotted class immutable once constructed.

    Assigning or deleting an attribute raises `AttributeError`. Set fields in `__init__` with
    `_assign`. Buses rely on events not changing, so a custom pattern should use it too.
    """

    __slots__ = ()

    def _assign(self, **fields: object) -> None:
        """Set fields, for use in `__init__`."""
        for field_name, value in fields.items():
            object.__setattr__(self, field_name, value)

    def __setattr__(self, name: str, value: object) -> NoReturn:
        """Refuse assignment: the object is immutable."""
        raise AttributeError(_IMMUTABLE.format(type(self).__name__))

    def __delattr__(self, name: str) -> NoReturn:
        """Refuse deletion: the object is immutable."""
        raise AttributeError(_IMMUTABLE.format(type(self).__name__))


@dataclass(init=False, slots=True)
class Request[S, R, Signal: bool](Immutable):
    """
    Request/reply event: exactly one responder, whose `reply` the emitter receives.

    Immutable once constructed. Compares and hashes by its class, name, payload type and reply
    type. `Signal` is a type-only parameter marking a payload-less event. `emit_returns` and
    `handler_returns` both hold the reply type: they carry it for buses and adapters; read it
    as `reply`.
    """

    name: str
    schema: type[S]
    is_signal: Signal = field(compare=False, repr=False)
    emit_returns: type[R]
    """The reply type, as {py:class}`EventType <stratae.events.protocols.EventType>` needs it."""
    handler_returns: type[R] = field(compare=False, repr=False)
    """The reply type, as {py:class}`EventType <stratae.events.protocols.EventType>` needs it."""
    _hash: int = field(compare=False, repr=False)

    @overload
    def __init__(
        self: "Request[NoPayload, None, Literal[True]]", *, name: str, reply: None = None
    ) -> None: ...
    @overload
    def __init__[R2](
        self: "Request[NoPayload, R2, Literal[True]]", *, name: str, reply: type[R2]
    ) -> None: ...
    @overload
    def __init__[S2](
        self: "Request[S2, None, Literal[False]]",
        schema: type[S2],
        *,
        name: str | None = None,
        reply: None = None,
    ) -> None: ...
    @overload
    def __init__[S2, R2](
        self: "Request[S2, R2, Literal[False]]",
        schema: type[S2],
        *,
        name: str | None = None,
        reply: type[R2],
    ) -> None: ...
    def __init__(
        self,
        schema: type[Any] = NoPayload,
        *,
        name: str | None = None,
        reply: type[Any] | None = None,
    ) -> None:
        """
        Define a request event.

        :param schema: The payload type. Omit for a payload-less event.
        :param name: Identifier for the event. Defaults to `schema.__name__`; required without
            a schema.
        :param reply: The reply type. Omit for a command, which replies `None`.
        :raises TypeError: If a payload-less event has no `name`.
        """
        reply_type = NoneType if reply is None else reply
        event_name = _event_name("Request", schema, name)
        self._assign(
            name=event_name,
            schema=schema,
            is_signal=schema is NoPayload,
            emit_returns=reply_type,
            handler_returns=reply_type,
            _hash=hash((event_name, schema, reply_type)),
        )

    def __hash__(self) -> int:
        """Return the hash of the name, payload type and reply type, made once."""
        return self._hash

    @property
    def reply(self) -> type[R]:
        """What the responder returns and the emitter gets; `NoneType` for a command."""
        return self.emit_returns


@dataclass(init=False, slots=True)
class PubSub[S, Signal: bool](Immutable):
    """
    Fire-and-forget event: every handler runs, and returns `None`.

    Nothing reads a pub/sub handler's result: an event that needs an answer is a
    {py:class}`Request`. So a handler is typed as returning `None`, or an awaitable resolving
    to it, and a type checker flags one that returns a value.

    Immutable once constructed. Compares and hashes by its class, name and payload type.
    `Signal` is a type-only parameter marking a payload-less event.
    """

    name: str
    schema: type[S]
    is_signal: Signal = field(compare=False, repr=False)
    emit_returns: type[None] = field(compare=False, repr=False)
    """`NoneType`: emitting a pub/sub event returns nothing."""
    handler_returns: type[None] = field(compare=False, repr=False)
    """`NoneType`: a pub/sub handler returns nothing."""
    _hash: int = field(compare=False, repr=False)

    @overload
    def __init__(self: "PubSub[NoPayload, Literal[True]]", *, name: str) -> None: ...
    @overload
    def __init__[S2](
        self: "PubSub[S2, Literal[False]]", schema: type[S2], *, name: str | None = None
    ) -> None: ...
    def __init__(self, schema: type[Any] = NoPayload, *, name: str | None = None) -> None:
        """
        Define a pub/sub event.

        :param schema: The payload type. Omit for a payload-less event.
        :param name: Identifier for the event. Defaults to `schema.__name__`; required without
            a schema.
        :raises TypeError: If a payload-less event has no `name`.
        """
        event_name = _event_name("PubSub", schema, name)
        self._assign(
            name=event_name,
            schema=schema,
            is_signal=schema is NoPayload,
            emit_returns=NoneType,
            handler_returns=NoneType,
            _hash=hash((event_name, schema)),
        )

    def __hash__(self) -> int:
        """Return the hash of the name and payload type, made once."""
        return self._hash
