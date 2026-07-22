"""Base event and schemas for the stratae event system."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, cast, get_args, get_origin, overload

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
        ``payload_type`` is just an explicit declaration — it doesn't
        retroactively type-check ``factory``'s own parameters. If
        ``factory`` is a generic class (e.g. ``class Wrapped[T]: ...``),
        subscript it at the call site instead (``event(Wrapped[OrderPlaced],
        PubSub)``) so its constructor is fully checked; passing the bare
        class with ``payload_type=Wrapped[OrderPlaced]`` leaves any
        parameter typed with ``T`` unchecked.

    Example::

        class OrderPlaced:
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id

        order_placed = event(OrderPlaced, PubSub)

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


@overload
def event[**P, E: Any, T: EventType](
    factory: Callable[P, E],
    event_type: type[T],
    *,
    name: str | None = None,
) -> EventConfig[P, E, T]: ...


@overload
def event[**P, E: Any, T: EventType](
    factory: Callable[P, Awaitable[E]],
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E],
) -> AsyncEventConfig[P, E, T]: ...


@overload
def event[**P, E: Any, T: EventType](
    factory: Callable[P, E],
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E],
) -> EventConfig[P, E, T]: ...


def event[**P, E: Any, T: EventType](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E] | None = None,
) -> EventConfig[P, E, T] | AsyncEventConfig[P, E, T]:
    """
    Define an ``Event`` binding a factory to a dispatch pattern.

    Args:
        factory:      A factory used to construct the payload.
        event_type:   The dispatch pattern discriminant class.
        name:         Human-readable identifier for this event.
                      Defaults to ``factory.__name__``.
        payload_type: The concrete payload type this event carries.
                      Derived from ``factory`` when ``factory`` is itself a
                      class; must be provided explicitly otherwise.

    Returns:
        An ``EventConfig`` (or ``AsyncEventConfig``, for an async factory)
        binding ``factory`` to ``event_type``.

    Note:
        ``payload_type`` is just an explicit declaration — it doesn't
        retroactively type-check ``factory``'s own parameters. If
        ``factory`` is a generic class (e.g. ``class Wrapped[T]: ...``),
        subscript it at the call site instead (``event(Wrapped[OrderPlaced],
        PubSub)``) so its constructor is fully checked; passing the bare
        class with ``payload_type=Wrapped[OrderPlaced]`` leaves any
        parameter typed with ``T`` unchecked.

    Example::

        class OrderPlaced:
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id

        order_placed = event(OrderPlaced, PubSub)

    """
    if is_async_factory(factory):
        return AsyncEventConfig(factory, event_type, name=name, payload_type=payload_type)
    return EventConfig(factory, event_type, name=name, payload_type=payload_type)


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
