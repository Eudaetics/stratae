"""Base event and schemas for the stratae event system."""

from __future__ import annotations

from typing import Callable


class EventType:
    """Marker base class for event type discriminants."""


class PubSub(EventType):
    """Pub/sub pattern discriminant — fire and forget, no return value."""


class EventSchema:
    """
    Marker base class for event payload schemas.

    Subclass ``EventSchema`` to define the data shape carried by an event.
    The contract is that subclasses must be serializable and deserializable —
    the library does not enforce how, so any approach works: plain classes,
    ``dataclasses``, ``msgspec.Struct``, ``pydantic.BaseModel``, etc.

    Schemas carry no routing config and are reusable across adapters.

    Example::

        class OrderPlaced(EventSchema):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id
    """


class Event[E: EventSchema, T: EventType]:
    """
    Bus-agnostic event definition binding a schema type to a dispatch pattern.

    An ``Event`` captures what an event IS — the payload schema and the
    dispatch pattern — independently of any bus or routing config.  It is
    the shareable definition that one or more bus bindings can reference.

    Type parameters:
        E: The ``EventSchema`` subclass carried by this event.
        T: The ``EventType`` discriminant describing the dispatch pattern.

    Example::

        @event(PubSub)
        class OrderPlaced(EventSchema):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id

    """

    def __init__(self, schema: type[E], event_type: type[T]) -> None:
        """
        Define an event with a schema type and dispatch pattern.

        Args:
            schema:     The ``EventSchema`` subclass carried by this event.
            event_type: The dispatch pattern discriminant class.

        """
        self.schema = schema
        self.event_type = event_type


def event[E: EventSchema, T: EventType](event_type: type[T]) -> Callable[[type[E]], Event[E, T]]:
    """
    Wrap an ``EventSchema`` subclass as an ``Event``.

    Args:
        event_type: The dispatch pattern discriminant class.

    Returns:
        A decorator that accepts an ``EventSchema`` subclass and returns an
        ``Event`` binding it to ``event_type``.

    Example::

        @event(PubSub)
        class OrderPlaced(EventSchema):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id

    """

    def decorator(schema: type[E]) -> Event[E, T]:
        return Event(schema, event_type)

    return decorator
