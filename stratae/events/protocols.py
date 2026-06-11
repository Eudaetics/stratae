"""Structural protocols for the stratae event system."""

from typing import Any, Callable, Protocol, runtime_checkable

from stratae.events.event import Event, EventSchema, EventType


@runtime_checkable
class Producer(Protocol):
    """
    Structural protocol for the emit side of the event system.

    Any class with a compatible ``emit`` method satisfies this protocol,
    whether sync or async.  ``BoundEvent`` calls ``emit`` when invoked;
    adapters implement it to perform the actual dispatch.
    """

    def emit[E: EventSchema, T: EventType](
        self, payload: E, event: Event[E, T], config: Any
    ) -> Any:
        """
        Dispatch a constructed event payload.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            event:   The ``Event`` definition being emitted.
            config:  Adapter-specific routing configuration.

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

    def handle(self, config: Any, fn: Callable[[EventSchema], Any] | None = None) -> Any:
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
