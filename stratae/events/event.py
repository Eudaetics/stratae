"""Base event and schemas for the stratae event system."""

from __future__ import annotations

from typing import Callable


class EventType:
    """Marker base class for event type discriminants."""


class PubSub(EventType):
    """Pub/sub pattern discriminant — fire and forget, no return value."""


class Payload:
    """
    Marker base class for event payload schemas.

    Subclass ``Payload`` to define the data shape carried by an event.
    The contract is that subclasses must be serializable and deserializable —
    the library does not enforce how, so any approach works: plain classes,
    ``dataclasses``, ``msgspec.Struct``, ``pydantic.BaseModel``, etc.

    Payloads carry no routing config and are reusable across adapters.

    Example::

        class OrderPlaced(Payload):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id
    """


class EventConfig[**P, E: Payload, T: EventType]:
    """
    Bus-agnostic event definition binding a payload type to a dispatch pattern.

    An ``Event`` captures what an event IS — the payload schema and the
    dispatch pattern — independently of any bus or routing config.  It is
    the shareable definition that one or more bus bindings can reference.

    Type parameters:
        E: The ``Payload`` subclass carried by this event.
        T: The ``EventType`` discriminant describing the dispatch pattern.

    Example::

        @event(PubSub)
        class OrderPlaced(Payload):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id

    """

    def __init__(self, factory: Callable[P, E], event_type: type[T]) -> None:
        """
        Define an event with a schema type and dispatch pattern.

        Args:
            factory:    A factory used to create a Payload.
            event_type: The dispatch pattern discriminant class.

        """
        self._factory = factory
        self.event_type = event_type

    @property
    def factory(self) -> Callable[P, E]:
        """Access the factory for use in generating payloads for events."""
        return self._factory


def event[**P, E: Payload, T: EventType](
    event_type: type[T],
) -> Callable[[Callable[P, E]], EventConfig[P, E, T]]:
    """
    Wrap a ``Payload`` subclass as an ``Event``.

    Args:
        event_type: The dispatch pattern discriminant class.

    Returns:
        A decorator that accepts a ``Payload`` subclass and returns an
        ``Event`` binding it to ``event_type``.

    Example::

        @event(PubSub)
        class OrderPlaced(Payload):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id

    """

    def decorator(schema: Callable[P, E]) -> EventConfig[P, E, T]:
        return EventConfig(schema, event_type)

    return decorator
