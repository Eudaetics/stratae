"""
Unit tests for Event, EventType, PubSub, and the event decorator.

This test suite verifies the following behaviors:

Event:
- The schema and event_type are stored on initialization.

PubSub:
- Is a subclass of EventType.

event decorator:
- Returns an Event instance.
- The returned Event stores the decorated class as its schema.
- The returned Event stores the supplied event_type.
"""

from stratae.events.event import Event, EventSchema, EventType, PubSub, event


def test_event_stores_schema_and_event_type() -> None:
    """
    Test that schema and event_type are stored during initialization.

    Given: An EventSchema subclass and an event_type
    When: An Event is created
    Then: The schema and event_type attributes should reference the supplied objects
    """

    # Arrange
    class _OrderPlaced(EventSchema):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    ev = Event(_OrderPlaced, PubSub)

    # Assert
    assert ev.schema is _OrderPlaced
    assert ev.event_type is PubSub


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

    Given: An EventSchema subclass decorated with @event
    When: The decorator is applied
    Then: The result should be an Event instance
    """

    # Arrange / Act
    @event(PubSub)
    class _Schema(EventSchema):
        def __init__(self, value: int) -> None:
            self.value = value

    # Assert
    assert isinstance(_Schema, Event)


def test_event_decorator_stores_schema() -> None:
    """
    Test that the event decorator stores the decorated class as the schema.

    Given: An EventSchema subclass decorated with @event
    When: The decorator is applied
    Then: The Event's schema should be the decorated class
    """

    # Arrange
    class _Schema(EventSchema):
        def __init__(self, value: int) -> None:
            self.value = value

    # Act
    decorated = event(PubSub)(_Schema)

    # Assert
    assert decorated.schema is _Schema


def test_event_decorator_stores_event_type() -> None:
    """
    Test that the event decorator stores the supplied event_type.

    Given: An EventSchema subclass decorated with @event(PubSub)
    When: The decorator is applied
    Then: The Event's event_type should be PubSub
    """

    # Arrange
    class _Schema(EventSchema):
        def __init__(self, value: int) -> None:
            self.value = value

    # Act
    decorated = event(PubSub)(_Schema)

    # Assert
    assert decorated.event_type is PubSub
