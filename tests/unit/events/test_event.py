"""
Unit tests for Event, DispatchPattern, PubSub, Request, is_request, and reply_type.

This test suite verifies the following behaviors:

Event:
- schema and pattern are stored on initialization.
- name defaults to schema.__name__ when not provided.
- name accepts an explicit override.
- Accepts a subscripted Request discriminant.
- Raises TypeError for a bare Request discriminant.
- Raises TypeError for a pattern that isn't a DispatchPattern subclass.

PubSub:
- Is a subclass of DispatchPattern.

Request:
- Is a subclass of DispatchPattern.

is_request:
- Returns True for an event with a subscripted Request discriminant.
- Returns False for an event with a PubSub discriminant.

reply_type:
- Returns the type Request was subscripted with.
- Raises TypeError when the discriminant is not a subscripted Request.
"""

from typing import cast

import pytest

from stratae.events import DispatchPattern, Event, PubSub, Request, is_request, reply_type


class _OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


class _BookFound:
    def __init__(self, title: str) -> None:
        self.title = title


class _FindBook:
    def __init__(self, query: str) -> None:
        self.query = query


def test_event_stores_schema_and_pattern() -> None:
    """
    Test that schema and pattern are stored during initialization.

    Given: A class and a dispatch pattern
    When: An Event is created
    Then: The schema and pattern attributes should reference the supplied objects
    """
    ev = Event(PubSub, _OrderPlaced)

    assert ev.schema is _OrderPlaced
    assert ev.pattern is PubSub


def test_event_derives_name_from_schema() -> None:
    """
    Test that name defaults to schema.__name__ when not provided.

    Given: A class
    When: An Event is created without an explicit name
    Then: name should equal the schema's __name__
    """
    ev = Event(PubSub, _OrderPlaced)

    assert ev.name == "_OrderPlaced"


def test_event_accepts_explicit_name() -> None:
    """
    Test that name accepts an explicit override.

    Given: A class and an explicit name
    When: An Event is created with name provided
    Then: name should equal the supplied string
    """
    ev = Event(PubSub, _OrderPlaced, name="order_placed")

    assert ev.name == "order_placed"


def test_pubsub_is_subclass_of_dispatch_pattern() -> None:
    """
    Test that PubSub is a subclass of DispatchPattern.

    Given: PubSub and DispatchPattern
    When: The class hierarchy is inspected
    Then: PubSub should be a subclass of DispatchPattern
    """
    assert issubclass(PubSub, DispatchPattern)


def test_request_is_subclass_of_dispatch_pattern() -> None:
    """
    Test that Request is a subclass of DispatchPattern.

    Given: Request and DispatchPattern
    When: The class hierarchy is inspected
    Then: Request should be a subclass of DispatchPattern
    """
    assert issubclass(Request, DispatchPattern)


def test_event_accepts_subscripted_request() -> None:
    """
    Test that Event stores a subscripted Request discriminant.

    Given: A payload class and a Request discriminant subscripted with a reply type
    When: An Event is created
    Then: pattern should equal the subscripted discriminant
    """
    ev = Event(Request[_BookFound], _FindBook)

    assert ev.pattern == Request[_BookFound]


def test_event_raises_for_bare_request() -> None:
    """
    Test that Event rejects an unsubscripted Request discriminant.

    Given: A payload class and the bare Request class
    When: An Event is created
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError):
        Event(Request, _FindBook)


def test_event_raises_for_non_dispatch_pattern() -> None:
    """
    Test that Event rejects a pattern that isn't a DispatchPattern subclass.

    Given: A schema class passed where the dispatch pattern belongs, as
        would happen if a caller swapped the constructor's argument order
    When: An Event is created with that class as its pattern
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError):
        Event(_FindBook, _BookFound)  #  pyright: ignore[reportArgumentType]


def test_is_request_true_for_request_event() -> None:
    """
    Test that is_request returns True for an event with a subscripted Request discriminant.

    Given: An Event with a Request discriminant subscripted with a reply type
    When: is_request is called with the event
    Then: The result should be True
    """
    find_book = Event(Request[_BookFound], _FindBook)

    assert is_request(find_book) is True


def test_is_request_false_for_pubsub_event() -> None:
    """
    Test that is_request returns False for an event with a PubSub discriminant.

    Given: An Event with a PubSub discriminant
    When: is_request is called with the event
    Then: The result should be False
    """
    order_placed = Event(PubSub, _OrderPlaced)

    assert is_request(order_placed) is False


def test_reply_type_returns_subscripted_reply() -> None:
    """
    Test that reply_type recovers the type Request was subscripted with.

    Given: An Event with a Request discriminant subscripted with a reply type
    When: reply_type is called with the event
    Then: The result should be the reply type class
    """
    find_book = Event(Request[_BookFound], _FindBook)

    recovered = reply_type(find_book)

    assert recovered is _BookFound


def test_reply_type_raises_for_non_request_discriminant() -> None:
    """
    Test that reply_type rejects an event whose discriminant is not a Request.

    Given: A PubSub Event cast to a request event type
    When: reply_type is called with the event
    Then: A TypeError should be raised
    """
    ev = Event(PubSub, _OrderPlaced)
    mistyped = cast(Event[Request[object], _OrderPlaced], ev)

    with pytest.raises(TypeError):
        reply_type(mistyped)
