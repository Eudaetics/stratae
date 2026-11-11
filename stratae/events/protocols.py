"""
Structural protocols for the event system.

{py:class}`EventType` is what every event pattern implements, and what buses type against;
{py:data}`AnyEvent`, {py:data}`PayloadEvent` and {py:data}`SignalEvent` abbreviate it.
{py:class}`Producer` and {py:class}`Consumer` describe the emit and handle sides of a bus
adapter. {py:class}`PubSubProducer` describes the emit side of one that emits only PubSub
events. {py:class}`EmitCallable` describes a single emitter, typed by the events it takes.
{py:class}`Serializer` and {py:class}`Deserializer` describe the codecs an adapter takes for
payloads that cross a process boundary. `msgspec.json.encode` and `msgspec.json.decode` are
one such pair. {py:class}`Serializing` describes a routing config with its own serializer.
{py:class}`Routed` describes a routing config that says where its messages go, as a
{py:class}`Destination`, so tracing and other observers can name it without knowing the
adapter.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, Protocol, runtime_checkable

from stratae.events.hooks import Hooks
from stratae.events.patterns import NoPayload, PubSub


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
class EmitCallable[Ev, S, H, R](Protocol):
    """
    Structural protocol for one emitter, shaped like {py:meth}`Producer.emit`.

    `Ev` is the event type it takes, `S` its payload type, `H` the hooks it takes and `R` what
    it returns. Binding an event to it checks the emitter takes that event. So an emitter for
    PubSub events only, such as
    {py:meth}`RabbitMQPublisher.emit <stratae.integrations.rabbitmq.RabbitMQPublisher.emit>`,
    binds PubSub events and refuses a request.
    """

    def __call__(self, event: Ev, payload: S, *, hooks: H | None = None) -> R:
        """
        Emit a payload on the event's route.

        :param event: The event being emitted.
        :param payload: The payload to emit.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this emit.
            `bind` passes it only when hooks are given, so an untyped emitter without it still
            works until someone binds with hooks, then fails loudly.
        :returns: Adapter-defined result, or a coroutine resolving to it.
        """
        ...


@runtime_checkable
class Producer(Protocol):
    """
    Structural protocol for the emit side of a bus adapter, sync or async.

    Where a message goes isn't chosen per emit. An adapter that routes, such as a broker's,
    holds a route for each event, registered by `bind` or by its own `route` method.
    """

    def emit[S](
        self,
        event: EventType[S, Any, Any, Any],
        payload: S,
        *,
        hooks: Hooks[S, Any] | None = None,
    ) -> Any:
        """
        Emit a payload on the event's route.

        :param event: The event being emitted.
        :param payload: The payload to emit.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this emit.
        :returns: Adapter-defined result, or a coroutine resolving to it.
        """
        ...


@runtime_checkable
class PubSubProducer(Protocol):
    """
    Structural protocol for the emit side of a bus adapter that emits only PubSub events.

    Its `emit` takes {py:class}`PubSub <stratae.events.patterns.PubSub>` events alone, so
    emitting a request on it is a type error. A request without a reply has the same shape as
    a PubSub event, so the pattern is matched by its class.

    Every {py:class}`Producer` matches it too, since a `Producer` emits any event.
    {py:class}`RabbitMQPublisher <stratae.integrations.rabbitmq.RabbitMQPublisher>` and
    {py:class}`OutboxProducer <stratae.events.outbox.OutboxProducer>` match only this one.
    """

    def emit[S](
        self,
        event: PubSub[S, Any],
        payload: S,
        *,
        hooks: Hooks[S, None] | None = None,
    ) -> Any:
        """
        Emit a payload on the event's route.

        :param event: The PubSub event being emitted.
        :param payload: The payload to emit.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around this emit.
        :returns: Adapter-defined result, or a coroutine resolving to it.
        """
        ...


@dataclass(frozen=True, slots=True)
class Encoded:
    """
    A payload that's already serialized, sent as its bytes instead of being encoded again.

    An adapter that serializes payloads sends an `Encoded` payload's `body` unchanged. The
    outbox relay re-publishes stored records this way. A route that addresses each message from
    its payload can't read an encoded one, so the address worked out when it was encoded comes
    with it.
    """

    body: bytes
    """The serialized payload."""
    address: str | None = None
    """The address its route gave the payload, such as a routing key, or `None` for a route
    that sends every message to the same place."""


@runtime_checkable
class Addressed(Protocol):
    """
    Structural protocol for a routing config that can address each message from its payload.

    A RabbitMQ config whose routing key is a function of the payload is one. The outbox records
    each message's address when it writes it, since the relay only has the encoded body, and
    passes it back with the body as an {py:class}`Encoded` payload.
    """

    def address(self, payload: Any, /) -> str | None:
        """
        Return the payload's address, or `None` if the route sends every message to one place.

        :param payload: The payload being emitted.
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


@runtime_checkable
class Serializing(Protocol):
    """
    Structural protocol for a routing config that can carry its own serializer.

    A RabbitMQ config is one. The outbox encodes each payload when it writes it, on the
    publisher's behalf. So it uses the serializer of the route's config, if it has one.
    """

    @property
    def serializer(self) -> Serializer[Any] | None:
        """Encodes this route's payloads, or `None` to use the adapter's serializer."""
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
