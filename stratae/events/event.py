"""Base event and schemas for the stratae event system."""

from __future__ import annotations

import inspect
from typing import Awaitable, Callable, Protocol, TypeGuard, overload


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


def _is_payload_class[**P, E: Payload](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[type[E]]:
    return isinstance(factory, type) and issubclass(factory, Payload)


def _is_async_factory[**P, E: Payload](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[Callable[P, Awaitable[E]]]:
    return inspect.iscoroutinefunction(factory)


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

    __slots__ = ("_factory", "event_type", "name", "payload_type")

    def __init__(
        self,
        factory: Callable[P, E] | Callable[P, Awaitable[E]],
        event_type: type[T],
        *,
        name: str | None = None,
        payload_type: type[E] | None = None,
    ) -> None:
        """
        Define an event with a schema type and dispatch pattern.

        Args:
            factory:      A factory used to create a Payload.
            event_type:   The dispatch pattern discriminant class.
            payload_type: The concrete ``Payload`` subclass this event carries.
                          Derived from ``factory`` when ``factory`` is itself a
                          ``Payload`` subclass; must be provided explicitly otherwise.
            name:         Human-readable identifier for this event.
                          Defaults to ``factory.__name__``.

        """
        self._factory = factory
        self.event_type = event_type

        self.name = name if name is not None else factory.__name__

        if payload_type is None:
            if not _is_payload_class(factory):
                raise TypeError(
                    "payload_type must be provided when factory is not a Payload subclass"
                )
            payload_type = factory
        self.payload_type = payload_type

    @property
    def factory(self) -> Callable[P, E] | Callable[P, Awaitable[E]]:
        """Access the factory for use in generating payloads for events."""
        return self._factory


class AsyncEventConfig[**P, E: Payload, T: EventType](EventConfig[P, E, T]):
    """EventConfig for async factories; ``factory`` is typed as returning ``Awaitable[E]``."""

    __slots__ = ("_async_factory",)

    def __init__(
        self,
        factory: Callable[P, Awaitable[E]],
        event_type: type[T],
        *,
        name: str | None = None,
        payload_type: type[E] | None = None,
    ) -> None:
        """
        Define an event with an async factory.

        Args:
            factory:      An async factory used to create a Payload.
            event_type:   The dispatch pattern discriminant class.
            payload_type: The concrete ``Payload`` subclass this event carries.
                          Required — async factories cannot self-derive it.
            name:         Human-readable identifier for this event.
                          Defaults to ``factory.__name__``.

        """
        if payload_type is None:
            raise TypeError("payload_type must be provided for async factories")
        super().__init__(factory, event_type, name=name, payload_type=payload_type)
        self._async_factory = factory

    @property
    def factory(self) -> Callable[P, Awaitable[E]]:
        """Access the async factory for use in generating payloads for events."""
        return self._async_factory


class _EventDecorator[T: EventType](Protocol):
    """Return type of ``event()`` when ``payload_type`` is not given."""

    def __call__[**P, E: Payload](self, schema: Callable[P, E]) -> EventConfig[P, E, T]: ...


class _EventDecoratorWithPayload[E: Payload, T: EventType](Protocol):
    """Return type of ``event()`` when ``payload_type`` is given."""

    @overload
    def __call__[**P](self, schema: Callable[P, E]) -> EventConfig[P, E, T]: ...
    @overload
    def __call__[**P](self, schema: Callable[P, Awaitable[E]]) -> AsyncEventConfig[P, E, T]: ...


@overload
def event[T: EventType](
    event_type: type[T],
    *,
    name: str | None = None,
) -> _EventDecorator[T]: ...


@overload
def event[E: Payload, T: EventType](
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E],
) -> _EventDecoratorWithPayload[E, T]: ...


def event[E: Payload, T: EventType](
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E] | None = None,
) -> _EventDecorator[T] | _EventDecoratorWithPayload[E, T]:
    """
    Wrap a ``Payload`` subclass as an ``Event``.

    Args:
        event_type:   The dispatch pattern discriminant class.
        name:         Human-readable identifier for this event.
                      Defaults to the decorated callable's ``__name__``.
        payload_type: The concrete ``Payload`` subclass this event carries.
                      Derived from the decorated callable when it is itself a
                      ``Payload`` subclass; must be provided explicitly otherwise.

    Returns:
        A decorator that accepts a ``Payload`` subclass and returns an
        ``Event`` binding it to ``event_type``.

    Example::

        @event(PubSub)
        class OrderPlaced(Payload):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id

    """
    if payload_type is not None:

        @overload
        def decorator_with_payload[**P](schema: Callable[P, E]) -> EventConfig[P, E, T]: ...
        @overload
        def decorator_with_payload[**P](
            schema: Callable[P, Awaitable[E]],
        ) -> AsyncEventConfig[P, E, T]: ...
        def decorator_with_payload[**P](
            schema: Callable[P, E] | Callable[P, Awaitable[E]],
        ) -> EventConfig[P, E, T]:
            if _is_async_factory(schema):
                return AsyncEventConfig(schema, event_type, name=name, payload_type=payload_type)
            return EventConfig(schema, event_type, name=name, payload_type=payload_type)

        return decorator_with_payload

    def decorator[**P, S: Payload](schema: Callable[P, S]) -> EventConfig[P, S, T]:
        return EventConfig(schema, event_type, name=name, payload_type=None)

    return decorator
