"""
Unit tests for Event, EventType, PubSub, and the event decorator.

This test suite verifies the following behaviors:

Event:
- The schema and event_type are stored on initialization.
- Raises TypeError when the schema is not a Payload subclass.
- Raises TypeError when the schema is a callable returning Payload but is not a class.

PubSub:
- Is a subclass of EventType.

event decorator:
- Returns an Event instance.
- The returned Event stores the decorated class as its schema.
- The returned Event stores the supplied event_type.
"""

import pytest

from stratae.events.event import Event, EventType, Payload, PubSub, event


def test_event_stores_schema_and_event_type() -> None:
    """
    Test that schema and event_type are stored during initialization.

    Given: A Payload subclass and an event_type
    When: An Event is created
    Then: The schema and event_type attributes should reference the supplied objects
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    ev = Event(_OrderPlaced, PubSub)

    # Assert
    assert ev.schema is _OrderPlaced
    assert ev.event_type is PubSub


def test_event_raises_type_error_for_non_event_schema() -> None:
    """
    Test that Event raises TypeError when the schema is not a Payload subclass.

    Given: A class that does not subclass Payload
    When: An Event is created with it as the schema
    Then: A TypeError should be raised
    """

    # Arrange
    class _NotAPayload:
        pass

    # Act & Assert
    with pytest.raises(TypeError, match="_NotAPayload.*is not a Payload subclass"):
        Event(_NotAPayload, PubSub)  # pyright: ignore[reportArgumentType]


def test_event_raises_type_error_for_factory_callable() -> None:
    """
    Test that Event raises TypeError when passed a callable returning Payload but not a class.

    Given: A function typed to return a Payload instance
    When: An Event is created with it as the schema
    Then: A TypeError should be raised
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    def _factory(order_id: int) -> _OrderPlaced:
        return _OrderPlaced(order_id)

    # Act & Assert
    with pytest.raises(TypeError, match="is not a Payload subclass"):
        Event(_factory, PubSub)


def test_pubsub_is_subclass_of_event_type() -> None:
    """
    Test that PubSub is a subclass of EventType.

    Given: PubSub and EventType
    When: The class hierarchy is inspected
    Then: PubSub should be a subclass of EventType
    """
    assert issubclass(PubSub, EventType)


def test_event_decorator_returns_event_instance() -> None:
    """
    Test that the event decorator returns an Event instance.

    Given: A Payload subclass decorated with @event
    When: The decorator is applied
    Then: The result should be an Event instance
    """

    # Arrange / Act
    @event(PubSub)
    class _Schema(Payload):
        def __init__(self, value: int) -> None:
            self.value = value

    # Assert
    assert isinstance(_Schema, Event)


def test_event_decorator_stores_schema() -> None:
    """
    Test that the event decorator stores the decorated class as the schema.

    Given: A Payload subclass decorated with @event
    When: The decorator is applied
    Then: The Event's schema should be the decorated class
    """

    # Arrange
    class _Schema(Payload):
        def __init__(self, value: int) -> None:
            self.value = value

    # Act
    decorated = event(PubSub)(_Schema)

    # Assert
    assert decorated.schema is _Schema


def test_event_decorator_stores_event_type() -> None:
    """
    Test that the event decorator stores the supplied event_type.

    Given: A Payload subclass decorated with @event(PubSub)
    When: The decorator is applied
    Then: The Event's event_type should be PubSub
    """

    # Arrange
    class _Schema(Payload):
        def __init__(self, value: int) -> None:
            self.value = value

    # Act
    decorated = event(PubSub)(_Schema)

    # Assert
    assert decorated.event_type is PubSub
