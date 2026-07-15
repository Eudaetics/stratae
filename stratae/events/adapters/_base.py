"""Shared handler registry and responder resolution for direct bus adapters."""

from collections import defaultdict
from typing import Any, Callable, Protocol

from stratae.events.event import EventConfig
from stratae.events.exceptions import MultipleRespondersError, NoResponderError
from stratae.events.handler import Handler

AnyEventConfig = EventConfig[Any, Any, Any]


class HandlerDecorator[S: Any](Protocol):
    """Decorator form of ``handle`` for pub/sub events: registers and returns the ``Handler``."""

    def __call__[R](self, fn: Callable[[S], R]) -> Handler[[S], AnyEventConfig, R]: ...


class BaseDirectBus:
    """
    Shared handler registry for the direct bus adapters.

    Holds the config-keyed handler registrations and the request responder
    resolution common to ``DirectBus`` and ``AsyncDirectBus``.  Subclasses
    own their emit/bind surfaces, their typed ``handle`` overloads, and
    dispatch semantics; registration delegates to ``_register``.
    """

    def __init__(self) -> None:
        """Initialise the handler registry."""
        self._handlers: dict[AnyEventConfig, set[Handler[Any, AnyEventConfig, Any]]] = defaultdict(
            set
        )

    def _register[**P, R](
        self, config: AnyEventConfig, fn: Callable[P, R]
    ) -> Handler[P, AnyEventConfig, R]:
        handler: Handler[P, AnyEventConfig, R] = Handler(fn, config)
        self._handlers[config].add(handler)
        return handler

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
