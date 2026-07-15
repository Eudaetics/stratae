"""Shared handler registry and responder resolution for direct bus adapters."""

from collections import defaultdict
from typing import Any, Callable, overload

from stratae.events.event import EventConfig
from stratae.events.exceptions import MultipleRespondersError, NoResponderError
from stratae.events.handler import Handler

AnyEventConfig = EventConfig[Any, Any, Any]


class BaseDirectBus:
    """
    Shared handler registry for the direct bus adapters.

    Holds the config-keyed handler registrations and the request responder
    resolution common to ``DirectBus`` and ``AsyncDirectBus``.  Subclasses
    own their emit/bind surfaces and dispatch semantics.
    """

    def __init__(self) -> None:
        """Initialise the handler registry."""
        self._handlers: dict[AnyEventConfig, set[Handler[Any, AnyEventConfig, Any]]] = defaultdict(
            set
        )

    @overload
    def handle[**P, R](
        self,
        config: AnyEventConfig,
        fn: Callable[P, R],
    ) -> Handler[P, AnyEventConfig, R]: ...

    @overload
    def handle[**P, R](
        self,
        config: AnyEventConfig,
        fn: None = None,
    ) -> Callable[[Callable[P, R]], Handler[P, AnyEventConfig, R]]: ...

    def handle[**P, R](
        self,
        config: AnyEventConfig,
        fn: Callable[P, R] | None = None,
    ) -> Handler[P, AnyEventConfig, R] | Callable[[Callable[P, R]], Handler[P, AnyEventConfig, R]]:
        """
        Register a handler callable for a config, as a decorator or direct call.

        Returns the ``Handler`` instance in both forms so callers can pass it
        to ``remove`` later.

        Args:
            config: The ``EventConfig`` used as the handler routing key.
            fn:     When supplied, registers ``fn`` directly and returns its
                    ``Handler``.  When omitted, returns a decorator that
                    registers and returns the ``Handler``.

        """

        def decorator(f: Callable[P, R]) -> Handler[P, AnyEventConfig, R]:
            handler: Handler[P, AnyEventConfig, R] = Handler(f, config)
            self._handlers[config].add(handler)
            return handler

        if fn is not None:
            return decorator(fn)
        return decorator

    def remove(self, handler: Handler[Any, AnyEventConfig, Any]) -> None:
        """
        Remove a previously registered handler.

        Args:
            handler: The ``Handler`` instance returned by ``handle``.

        """
        self._handlers[handler.config].discard(handler)

    def _single_responder(self, event: AnyEventConfig) -> Handler[Any, AnyEventConfig, Any]:
        responders = self._handlers.get(event)
        if not responders:
            raise NoResponderError(f"no responder registered for request event '{event.name}'")
        if len(responders) > 1:
            raise MultipleRespondersError(
                f"request event '{event.name}' has {len(responders)} responders;"
                " exactly one is required"
            )
        (responder,) = responders
        return responder
