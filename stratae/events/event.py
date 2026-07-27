"""Base event and schemas for the stratae event system."""

from __future__ import annotations

from typing import Any, cast, get_args, get_origin

_UNSUBSCRIPTED_REQUEST = "Request must be subscripted with its reply type (e.g. Request[BookFound])"
_NOT_A_REQUEST = "event does not carry a subscripted Request discriminant"


class DispatchPattern:
    """Marker base class for dispatch pattern discriminants."""


class PubSub(DispatchPattern):
    """Pub/sub pattern discriminant — fire and forget, no return value."""


class Request[Reply](DispatchPattern):
    """
    Request/reply pattern discriminant — emit blocks until a responder returns ``Reply``.

    Always subscript with the reply type (e.g. ``Request[BookFound]``).
    ``Event`` rejects a bare ``Request`` because the reply type could
    not be recovered at runtime for dispatch or deserialization; use
    ``reply_type`` to recover it from a request event.
    """


def _validate_pattern(pattern: type[DispatchPattern]):
    if get_origin(pattern) is None and issubclass(pattern, Request):
        raise TypeError(_UNSUBSCRIPTED_REQUEST)


class Event[E: Any, T: DispatchPattern]:
    """
    Bus-agnostic event definition binding a schema to a dispatch pattern.

    An ``Event`` captures what an event IS — the payload schema and the
    dispatch pattern — independently of any bus, factory, or routing
    config. It is the shareable definition that both a producer's
    ``bind`` and a consumer's ``handle`` reference.

    Type parameters:
        E: The payload type carried by this event.
        T: The ``DispatchPattern`` discriminant.

    Example::

        class OrderPlaced:
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id

        order_placed = Event(OrderPlaced, PubSub)

    """

    __slots__ = ("name", "pattern", "schema")

    def __init__(
        self,
        schema: type[E],
        pattern: type[T],
        *,
        name: str | None = None,
    ) -> None:
        """
        Define an event with a schema type and dispatch pattern.

        Args:
            schema:  The payload type this event carries.
            pattern: The dispatch pattern discriminant class.
            name:    Human-readable identifier for this event.
                     Defaults to ``schema.__name__``.

        """
        _validate_pattern(pattern)
        self.schema = schema
        self.pattern = pattern
        self.name = name if name is not None else schema.__name__


def is_request[S: Any, T: DispatchPattern](event: Event[S, T]) -> bool:
    """
    Report whether the event carries a subscripted ``Request`` discriminant.

    Adapters branch on this at dispatch time to select request/reply
    semantics over fire-and-forget.

    Args:
        event: Any ``Event``.

    Returns:
        ``True`` when the event's discriminant is a subscripted ``Request``
        (or a subscripted subclass of it), ``False`` otherwise.

    """
    origin: object = get_origin(event.pattern)
    return isinstance(origin, type) and issubclass(origin, Request)


def reply_type[S: Any, R](event: Event[S, Request[R]]) -> type[R]:
    """
    Recover the reply type from a request event's discriminant.

    Adapters use this to know what a blocking emit resolves to at runtime —
    e.g. to deserialize a reply arriving from the far side of a broker.

    Args:
        event: An ``Event`` whose discriminant is a subscripted ``Request``.

    Returns:
        The type ``Request`` was subscripted with when the event was defined.

    Raises:
        TypeError: When the event's discriminant is not a subscripted
                   ``Request``.  Unreachable for type-checked callers; guards
                   dynamic construction paths.

    Example::

        class FindBook:
            def __init__(self, query: str) -> None:
                self.query = query

        find_book = Event(FindBook, Request[BookFound])
        reply_type(find_book)  # BookFound

    """
    if not is_request(event):
        raise TypeError(_NOT_A_REQUEST)
    return cast(type[R], get_args(event.pattern)[0])
