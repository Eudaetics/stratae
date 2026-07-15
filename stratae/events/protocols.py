"""Structural protocols for the stratae event system."""

from typing import Any, Callable, Protocol, runtime_checkable

from stratae.events.event import EventConfig, EventType


@runtime_checkable
class EmitCallable[**P, S: Any, T: EventType, C: Any, R: Any](Protocol):
    """
    Structural protocol for a single bound emit call.

    Captures the call shape of ``Producer.emit`` — payload, event, and
    config in, some adapter-defined result out — but parameterized over a
    concrete ``R`` instead of ``Any``, so a specific binding (e.g. a
    ``BoundEvent``'s ``emitter``) can be checked against its own return type.
    """

    def __call__(
        self,
        payload: S,
        event: EventConfig[P, S, T],
        config: C,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R:
        """
        Dispatch a constructed event payload.

        Args:
            payload:    The constructed payload instance to dispatch.
            event:      The ``Event`` definition being emitted.
            config:     Adapter-specific routing configuration.
            serializer: Serializer the payload will be sent to prior to routing.

        Returns:
            Adapter-defined result of dispatching the payload.

        """
        ...


@runtime_checkable
class Producer(Protocol):
    """
    Structural protocol for the emit side of the event system.

    Any class with a compatible ``emit`` method satisfies this protocol,
    whether sync or async.  ``BoundEvent`` calls ``emit`` when invoked;
    adapters implement it to perform the actual dispatch.
    """

    def emit[**P, S: Any, T: EventType](
        self,
        payload: S,
        event: EventConfig[P, S, T],
        config: Any,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> Any:
        """
        Dispatch a constructed event payload.

        Args:
            payload:    The constructed payload instance to dispatch.
            event:      The ``Event`` definition being emitted.
            config:     Adapter-specific routing configuration.
            serializer: Encodes ``payload`` before dispatch. Format is
                        adapter-defined (bytes, a JSON string, etc.) — when
                        omitted, the adapter falls back to its own default
                        serializer, if any.

        Returns:
            Adapter-defined; sync implementations return directly, async
            implementations return a coroutine.

        """
        ...


@runtime_checkable
class Consumer(Protocol):
    """
    Structural protocol for the receive side of the event system.

    Any class with a compatible ``handle`` method satisfies this protocol.
    ``handle`` is the user-facing API for registering handlers against a
    config key, replacing the former ``subscribe``.  It covers all consumer
    patterns: pub/sub handlers, repliers, RPC responders, etc.

    The internal dispatch mechanism — how a queued message or in-process
    event actually triggers the registered handlers — is an implementation
    detail of each adapter and is not part of this protocol.
    """

    def handle(self, config: Any, fn: Callable[[Any], Any] | None = None) -> Any:
        """
        Register a handler for the given config.

        Can be called directly or used as a decorator factory::

            bus.handle(emit_order, on_order)

            @bus.handle(emit_order)
            def on_order(payload: OrderPlaced) -> None: ...

        Args:
            config: The adapter-specific key used to route events to handlers.
            fn:     When supplied, registers ``fn`` directly and returns it.
                    When omitted, returns a decorator that registers and
                    returns the handler.

        """
        ...
