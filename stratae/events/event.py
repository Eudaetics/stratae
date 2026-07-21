"""Base event and schemas for the stratae event system."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Protocol, cast, get_args, get_origin, overload

from stratae.events._typeguards import is_async_factory, is_class_factory

_UNSUBSCRIPTED_REQUEST = "Request must be subscripted with its reply type (e.g. Request[BookFound])"
_NOT_A_REQUEST = "event does not carry a subscripted Request discriminant"


class EventType:
    """Marker base class for event type discriminants."""


class PubSub(EventType):
    """Pub/sub pattern discriminant. Fire and forget, no return value."""


class Request[Reply](EventType):
    """
    Request/reply pattern discriminant. Emit blocks until a responder returns ``Reply``.

    Always subscript with the reply type (e.g. ``Request[BookFound]``).
    ``EventConfig`` rejects a bare ``Request`` because the reply type could
    not be recovered at runtime for dispatch or deserialization; use
    ``reply_type`` to recover it from a request event.
    """


def _validate_event_type(event_type: type[EventType]):
    if get_origin(event_type) is None and issubclass(event_type, Request):
        raise TypeError(_UNSUBSCRIPTED_REQUEST)


class EventConfig[**P, E: Any, T: EventType]:
    """
    Bus-agnostic event definition binding a payload type to a dispatch pattern.

    An ``Event`` captures what an event IS (the payload schema and the
    dispatch pattern) independently of any bus or routing config.  It is
    the shareable definition that one or more bus bindings can reference.

    Type parameters:
        E: The payload type carried by this event.
        T: The ``EventType`` discriminant describing the dispatch pattern.

    Note:
        Using a generic class directly as ``factory`` (e.g.
        ``class Wrapped[T]: ...``) is discouraged. ``payload_type`` only
        declares the resulting payload type; it cannot retroactively
        constrain the factory's own constructor signature, so a bare generic
        factory's argument types may end up unchecked even when
        ``payload_type`` looks precise. A generic payload is also harder to
        deserialize on the far side of a real bus without already knowing
        ``T`` from elsewhere. It's still possible to use one this way and
        have it work. The type checker just won't catch as much for you.
        Where practical, prefer a concrete intermediary factory function
        instead (e.g. ``def make_order_page(items: list[OrderPlaced]) ->
        Page[OrderPlaced]: ...``) and pass that as ``factory``.

    Example::

        @event(PubSub)
        class OrderPlaced:
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
            factory:      A factory used to construct the payload.
            event_type:   The dispatch pattern discriminant class.
            payload_type: The concrete payload type this event carries.
                          Derived from ``factory`` when ``factory`` is itself a
                          class; must be provided explicitly otherwise.
            name:         Human-readable identifier for this event.
                          Defaults to ``factory.__name__``.

        """
        _validate_event_type(event_type)
        self._factory = factory
        self.event_type = event_type

        self.name = name if name is not None else factory.__name__

        if payload_type is None:
            if not is_class_factory(factory):
                raise TypeError("payload_type must be provided when factory is not a class")
            payload_type = factory
        self.payload_type = payload_type

    @property
    def factory(self) -> Callable[P, E] | Callable[P, Awaitable[E]]:
        """Access the factory for use in generating payloads for events."""
        return self._factory


class AsyncEventConfig[**P, E: Any, T: EventType](EventConfig[P, E, T]):
    """
    EventConfig for async factories; ``factory`` is typed as returning ``Awaitable[E]``.

    Note:
        Using a generic async factory function (e.g.
        ``async def make_wrapped[T](item: T) -> Wrapped[T]: ...``) directly is
        discouraged for the same reason as a generic class factory:
        ``payload_type`` only declares the resulting payload type, it cannot
        retroactively constrain the factory's own parameter types, so a
        generic factory's argument types may end up unchecked even when
        ``payload_type`` looks precise. A generic payload is also harder to
        deserialize on the far side of a real bus without already knowing
        ``T`` from elsewhere. It's still possible to use one this way and have
        it work. The type checker just won't catch as much for you. Where
        practical, prefer a concrete factory function whose parameters and
        return type are already fully resolved.

    """

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
            factory:      An async factory used to construct the payload.
            event_type:   The dispatch pattern discriminant class.
            payload_type: The concrete payload type this event carries.
                          Required for async factories since they
                          cannot self-derive it.
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

    def __call__[**P, E: Any](self, schema: Callable[P, E]) -> EventConfig[P, E, T]: ...


class _EventDecoratorWithPayload[E: Any, T: EventType](Protocol):
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
def event[E: Any, T: EventType](
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E],
) -> _EventDecoratorWithPayload[E, T]: ...


def event[E: Any, T: EventType](
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E] | None = None,
) -> _EventDecorator[T] | _EventDecoratorWithPayload[E, T]:
    """
    Wrap a class as an ``Event``.

    Args:
        event_type:   The dispatch pattern discriminant class.
        name:         Human-readable identifier for this event.
                      Defaults to the decorated callable's ``__name__``.
        payload_type: The concrete payload type this event carries.
                      Derived from the decorated callable when it is itself a
                      class; must be provided explicitly otherwise.

    Returns:
        A decorator that accepts a class and returns an
        ``Event`` binding it to ``event_type``.

    Note:
        Using a generic class directly as the decorated factory (e.g.
        ``class Wrapped[T]: ...``) is discouraged. ``payload_type`` only
        declares the resulting payload type; it cannot retroactively
        constrain the factory's own constructor signature, so a bare generic
        factory's argument types may end up unchecked even when
        ``payload_type`` looks precise. A generic payload is also harder to
        deserialize on the far side of a real bus without already knowing
        ``T`` from elsewhere. It's still possible to use one this way and
        have it work. The type checker just won't catch as much for you.
        Where practical, prefer a concrete intermediary factory function
        instead (e.g. ``def make_order_page(items: list[OrderPlaced]) ->
        Page[OrderPlaced]: ...``) and decorate or register that.

    Example::

        @event(PubSub)
        class OrderPlaced:
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
            if is_async_factory(schema):
                return AsyncEventConfig(schema, event_type, name=name, payload_type=payload_type)
            return EventConfig(schema, event_type, name=name, payload_type=payload_type)

        return decorator_with_payload

    def decorator[**P, S: Any](schema: Callable[P, S]) -> EventConfig[P, S, T]:
        return EventConfig(schema, event_type, name=name, payload_type=None)

    return decorator


def is_request[**P, S: Any, T: EventType](event: EventConfig[P, S, T]) -> bool:
    """
    Report whether the event carries a subscripted ``Request`` discriminant.

    Adapters branch on this at dispatch time to select request/reply
    semantics over fire-and-forget.

    Args:
        event: Any ``EventConfig``.

    Returns:
        ``True`` when the event's discriminant is a subscripted ``Request``
        (or a subscripted subclass of it), ``False`` otherwise.

    """
    origin: object = get_origin(event.event_type)
    return isinstance(origin, type) and issubclass(origin, Request)


def reply_type[**P, S: Any, R](event: EventConfig[P, S, Request[R]]) -> type[R]:
    """
    Recover the reply type from a request event's discriminant.

    Adapters use this to know what a blocking emit resolves to at runtime,
    e.g. to deserialize a reply arriving from the far side of a broker.

    Args:
        event: An ``EventConfig`` whose discriminant is a subscripted
               ``Request``.

    Returns:
        The type ``Request`` was subscripted with when the event was defined.

    Raises:
        TypeError: When the event's discriminant is not a subscripted
                   ``Request``.  Unreachable for type-checked callers; guards
                   dynamic construction paths.

    Example::

        @event(Request[BookFound])
        class FindBook:
            def __init__(self, query: str) -> None:
                self.query = query


        reply_type(FindBook)  # BookFound

    """
    if not is_request(event):
        raise TypeError(_NOT_A_REQUEST)
    return cast(type[R], get_args(event.event_type)[0])
