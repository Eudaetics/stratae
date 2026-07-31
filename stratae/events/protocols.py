"""
Structural protocols for the stratae event system.

{py:class}`Producer` and {py:class}`Consumer` describe the emit and handle
sides of a bus adapter structurally. Any class with a compatible method
satisfies the protocol, sync or async, without inheriting from it.
{py:class}`EmitCallable` describes a single bound emit call. It's
parameterized over a concrete return type `R`, so a specific binding, e.g.
the `emitter` behind a callable returned by
{py:func}`bind <stratae.events.bind.bind>`, can be checked against its own
return type instead of `Any`.
"""

from typing import Any, Callable, Protocol, runtime_checkable

from stratae.events.event import DispatchPattern, Event


@runtime_checkable
class EmitCallable[T: DispatchPattern[Any, Any], S, C, R, Signal: bool](Protocol):
    """
    Structural protocol for a single bound emit call.

    Captures the call shape of {py:meth}`Producer.emit`: event, config, and
    payload in, some adapter-defined result out. It's parameterized over a
    concrete `R` instead of `Any`, though. That lets a specific binding,
    e.g. the `emitter` behind a callable returned by
    {py:func}`bind <stratae.events.bind.bind>`, be checked against its
    own return type. `Signal` is likewise concrete per binding.
    """

    def __call__(
        self,
        event: Event[T, S, Signal],
        config: C,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R:
        """
        Dispatch a payload.

        :param event: The {py:class}`Event <stratae.events.event.Event>`
            definition being emitted.
        :param config: Adapter-specific routing configuration.
        :param payload: The constructed payload instance to dispatch.
        :param serializer: Serializer the payload will be sent to prior to routing.
        :returns: Adapter-defined result of dispatching the payload.
        """
        ...


@runtime_checkable
class Producer(Protocol):
    """
    Structural protocol for the emit side of the event system.

    Any class with a compatible `emit` method satisfies this protocol,
    whether sync or async. A callable returned by
    {py:func}`bind <stratae.events.bind.bind>` calls `emit` when invoked.
    Adapters implement it to perform the actual dispatch.
    """

    def emit[T: DispatchPattern[Any, Any], S, Signal: bool](
        self,
        event: Event[T, S, Signal],
        config: Any,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> Any:
        """
        Dispatch a payload.

        :param event: The {py:class}`Event <stratae.events.event.Event>`
            definition being emitted.
        :param config: Adapter-specific routing configuration.
        :param payload: The constructed payload instance to dispatch.
        :param serializer: Encodes `payload` before dispatch. Format is
            adapter-defined (bytes, a JSON string, etc.). When omitted,
            the adapter falls back to its own default serializer, if any.
        :returns: Adapter-defined result of dispatching the payload. Sync
            implementations return it directly; async implementations
            return a coroutine.
        """
        ...


@runtime_checkable
class Consumer(Protocol):
    """
    Structural protocol for the receive side of the event system.

    Any class with a compatible `handle` method satisfies this protocol.
    `handle` is the user-facing API for registering handlers against a
    config key, replacing the former `subscribe`. It covers every consumer
    pattern: pub/sub handlers, repliers, RPC responders, and so on.

    The internal dispatch mechanism is an implementation detail of each
    adapter and isn't part of this protocol. That includes how a queued
    message, or an in-process event, actually triggers the registered
    handlers.
    """

    def handle(self, config: Any, fn: Callable[[Any], Any] | None = None) -> Any:
        """
        Register a handler for the given config.

        Can be called directly, passing the handler function as `fn`, or
        used as a decorator factory by omitting `fn` and applying the
        result to a function definition instead.

        :param config: The adapter-specific key used to route events to handlers.
        :param fn: When supplied, registers `fn` directly and returns it.
            When omitted, returns a decorator that registers and returns
            the handler.
        """
        ...
