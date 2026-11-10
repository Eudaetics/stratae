"""
Structural protocols for the event system.

{py:class}`EventType` is what every event pattern implements, and what buses type against;
{py:data}`AnyEvent`, {py:data}`PayloadEvent` and {py:data}`SignalEvent` abbreviate it.
{py:class}`Producer` and {py:class}`Consumer` describe the emit and handle sides of a bus
adapter. {py:class}`EmitCallable` describes a single emitter with a concrete return type.
{py:class}`Serializer` and {py:class}`Deserializer` describe the codecs an adapter takes for
payloads that cross a process boundary. `msgspec.json.encode` and `msgspec.json.decode` are
one such pair. {py:class}`Routed` describes a routing config that says where its messages go,
as a {py:class}`Destination`, so tracing and other observers can name it without knowing the
adapter.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, Protocol, runtime_checkable

from stratae.events.hooks import Hooks
from stratae.events.patterns import NoPayload


class EventType[S, EmitR, HandleR, Signal: bool](Protocol):
    """
    Structural protocol for an event pattern; buses type against it.

    Implemented by {py:class}`Request <stratae.events.patterns.Request>` and
    {py:class}`PubSub <stratae.events.patterns.PubSub>`. `S` is the payload type, `EmitR`
    what emitting returns, `HandleR` what a handler returns, and `Signal` is `Literal[True]`
    for a payload-less event.

    The set of patterns is open: an event is data, and each adapter decides which patterns it
    delivers. The direct buses dispatch any pattern they have a dispatcher for, and take more
    with `dispatchers=`. A broker adapter supports the patterns it implements a transport for.
    Either refuses an unsupported event when it's registered or bound. Adapters match a
    pattern by class, so a subclass of a pattern is the same pattern.

    `emit_returns` and `handler_returns` carry `EmitR` and `HandleR` for type checking; a
    pattern can expose friendlier names, such as `Request.reply`. The members are plain
    attributes rather than read-only properties: read-only members would let a type checker
    widen `S`, `EmitR` and `HandleR` until a mismatched handler fits. Events are immutable at
    runtime instead.
    """

    name: str
    """The event's identifier."""
    schema: type[S]
    """The payload type, or {py:class}`NoPayload <stratae.events.patterns.NoPayload>`."""
    is_signal: Signal
    """Whether the event carries no payload."""
    emit_returns: type[EmitR]
    """What emitting the event returns."""
    handler_returns: type[HandleR]
    """What a handler for the event returns."""


type AnyEvent = EventType[Any, Any, Any, Any]
"""Any event, of any pattern."""

type PayloadEvent[S, EmitR, HandleR] = EventType[S, EmitR, HandleR, Literal[False]]
"""An event carrying a payload of type `S`."""

type SignalEvent[EmitR, HandleR] = EventType[NoPayload, EmitR, HandleR, Literal[True]]
"""An event carrying no payload."""


@runtime_checkable
class EmitCallable[S, C, R, Signal: bool](Protocol):
    """Structural protocol for one emitter, shaped like {py:meth}`Producer.emit`."""

    def __call__(
        self,
        event: EventType[S, Any, Any, Signal],
        config: C,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, Any] | None = None,
    ) -> R:
        """
        Dispatch a payload.

        :param event: The event being emitted.
        :param config: Adapter-specific routing config.
        :param payload: The payload to dispatch.
        :param serializer: Encodes the payload; defaults to the adapter's own.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this emit.
            `bind` passes it only when hooks are given, so an untyped emitter without it still
            works until someone binds with hooks, then fails loudly.
        :returns: Adapter-defined result, or a coroutine resolving to it.
        """
        ...


@runtime_checkable
class Producer(Protocol):
    """Structural protocol for the emit side of a bus adapter, sync or async."""

    def emit[S](
        self,
        event: EventType[S, Any, Any, Any],
        config: Any,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, Any] | None = None,
    ) -> Any:
        """
        Dispatch a payload.

        :param event: The event being emitted.
        :param config: Adapter-specific routing config.
        :param payload: The payload to dispatch.
        :param serializer: Encodes the payload; defaults to the adapter's own.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this emit.
            `bind` passes it only when hooks are given, so an untyped emitter without it still
            works until someone binds with hooks, then fails loudly.
        :returns: Adapter-defined result, or a coroutine resolving to it.
        """
        ...


@runtime_checkable
class Consumer(Protocol):
    """Structural protocol for the handle side of a bus adapter."""

    def handle(self, config: Any, fn: Callable[[Any], Any] | None = None) -> Any:
        """
        Register a handler, as a decorator or direct call.

        :param config: The routing key for the handler.
        :param fn: The handler. If omitted, returns a decorator.
        """
        ...


class Serializer[T](Protocol):
    """
    Structural protocol for a payload serializer: an object to bytes.

    An adapter that sends payloads over a transport takes one, such as `msgspec.json.encode`.
    """

    def __call__(self, obj: T, /) -> bytes:
        """
        Serialize `obj` to bytes.

        :param obj: The payload to serialize.
        :returns: The serialized payload.
        """
        ...


class Deserializer(Protocol):
    """
    Structural protocol for a type-directed deserializer: bytes and a target type to an object.

    An adapter that receives payloads takes one, such as `msgspec.json.decode`. It's called
    with the payload type of the event being handled.
    """

    def __call__[S](self, data: bytes, /, *, type: type[S]) -> S:
        """
        Deserialize `data` into an instance of `type`.

        :param data: The raw bytes to decode.
        :param type: The type to reconstruct.
        :returns: The reconstructed `type` instance.
        """
        ...


type AttributeValue = str | bool | int | float
"""A value an observer can record about a destination, as OpenTelemetry attributes take."""

_NO_ATTRIBUTES: Mapping[str, AttributeValue] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class Destination:
    """
    Where a routing config's messages go, or come from, as plain data.

    A config describes itself with one, as {py:class}`Routed` says. The fields follow
    OpenTelemetry's messaging semantic conventions, but nothing here depends on OpenTelemetry.
    """

    system: str
    """The messaging system, such as `"rabbitmq"`: `messaging.system`."""
    name: str | None = None
    """The destination's name, if it has one: `messaging.destination.name`."""
    anonymous: bool = False
    """Whether the destination was made for one consumer and has no lasting name, such as a
    server-named queue: `messaging.destination.anonymous`."""
    attributes: Mapping[str, AttributeValue] = field(default=_NO_ATTRIBUTES)
    """Attributes particular to the system, named as the conventions name them, such as
    `messaging.rabbitmq.destination.routing_key`."""


class Routed(Protocol):
    """
    Structural protocol for a routing config that says where its messages go.

    An adapter's config implements it to take part in messaging spans: a publish config names
    where it sends, a consume config where it receives from. It's read on each emit or handler
    call, so a config that can change computes it when read. A config without it is still
    routed; observers just can't name its destination.
    """

    @property
    def messaging_destination(self) -> Destination:
        """Where this config's messages go, or come from."""
        ...
