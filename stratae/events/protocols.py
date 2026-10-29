"""
Structural protocols for the event system.

{py:class}`EventType` is what every event pattern implements, and what buses type against;
{py:data}`AnyEvent`, {py:data}`PayloadEvent` and {py:data}`SignalEvent` abbreviate it.
{py:class}`Producer` and {py:class}`Consumer` describe the emit and handle sides of a bus
adapter. {py:class}`EmitCallable` describes a single emitter with a concrete return type.
"""

from typing import Any, Callable, Literal, Protocol, runtime_checkable

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
