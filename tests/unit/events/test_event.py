"""
Unit tests for Event, EventType, PubSub, and the event decorator.

This test suite verifies the following behaviors:

EventConfig:
- The factory and event_type are stored on initialization.
- payload_type is derived from factory when factory is a Payload subclass.
- payload_type accepts an explicit override.
- Raises TypeError when factory is not a Payload subclass and payload_type is not provided.
- name defaults to factory.__name__ when not provided.
- name accepts an explicit override.

PubSub:
- Is a subclass of EventType.

event decorator:
- Returns an EventConfig instance.
- The returned EventConfig stores the decorated class as its factory.
- The returned EventConfig stores the supplied event_type.
- payload_type is derived from the decorated class when not provided.
- payload_type accepts an explicit value for factory functions.
- name defaults to the decorated callable's __name__ when not provided.
- name accepts an explicit override.
"""

import pytest

from stratae.events.event import EventConfig, EventType, Payload, PubSub, event


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
    ev = EventConfig(_OrderPlaced, PubSub)

    # Assert
    assert ev.factory is _OrderPlaced
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
    assert isinstance(_Schema, EventConfig)


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
    assert decorated.factory is _Schema


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


def test_event_decorator_with_factory_stores_explicit_payload_type() -> None:
    """
    Test that a factory function decorated with @event stores the explicit payload_type.

    Given: A Payload subclass and a factory function returning it
    When: The factory is decorated with @event specifying payload_type explicitly
    Then: The EventConfig's payload_type should reference the supplied Payload subclass
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    @event(PubSub, payload_type=_OrderPlaced)
    def create_order(order_id: int) -> _OrderPlaced:
        return _OrderPlaced(order_id)

    # Assert
    assert create_order.payload_type is _OrderPlaced


def test_eventconfig_derives_payload_type_from_class_factory() -> None:
    """
    Test that payload_type is derived from factory when factory is a Payload subclass.

    Given: A Payload subclass used directly as the factory
    When: An EventConfig is created without an explicit payload_type
    Then: payload_type should reference the factory class
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    ev = EventConfig(_OrderPlaced, PubSub)

    # Assert
    assert ev.payload_type is _OrderPlaced


def test_eventconfig_accepts_explicit_payload_type() -> None:
    """
    Test that payload_type accepts an explicit override.

    Given: A Payload subclass and a factory function returning it
    When: An EventConfig is created with an explicit payload_type
    Then: payload_type should reference the supplied class
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    def create_order(order_id: int) -> _OrderPlaced:
        return _OrderPlaced(order_id)

    # Act
    ev = EventConfig(create_order, PubSub, payload_type=_OrderPlaced)

    # Assert
    assert ev.payload_type is _OrderPlaced


def test_eventconfig_raises_for_factory_without_payload_type() -> None:
    """
    Test that EventConfig raises TypeError when factory is not a Payload subclass.

    Given: A factory function returning a Payload without explicit payload_type
    When: An EventConfig is created
    Then: A TypeError should be raised
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    def create_order(order_id: int) -> _OrderPlaced:
        return _OrderPlaced(order_id)

    # Act / Assert
    with pytest.raises(TypeError):
        EventConfig(create_order, PubSub)


def test_eventconfig_derives_name_from_factory() -> None:
    """
    Test that name defaults to factory.__name__ when not provided.

    Given: A Payload subclass
    When: An EventConfig is created without an explicit name
    Then: name should equal the factory's __name__
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    ev = EventConfig(_OrderPlaced, PubSub)

    # Assert
    assert ev.name == _OrderPlaced.__name__


def test_eventconfig_accepts_explicit_name() -> None:
    """
    Test that name accepts an explicit override.

    Given: A Payload subclass and an explicit name
    When: An EventConfig is created with name provided
    Then: name should equal the supplied string
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    ev = EventConfig(_OrderPlaced, PubSub, name="order_placed")

    # Assert
    assert ev.name == "order_placed"


def test_event_decorator_derives_payload_type_from_class() -> None:
    """
    Test that the event decorator derives payload_type from the decorated class.

    Given: A Payload subclass decorated with @event
    When: The decorator is applied without explicit payload_type
    Then: The EventConfig's payload_type should reference the decorated class
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    decorated = event(PubSub)(_OrderPlaced)

    # Assert
    assert decorated.payload_type is _OrderPlaced


def test_event_decorator_derives_name_from_class() -> None:
    """
    Test that the event decorator derives name from the decorated class's __name__.

    Given: A Payload subclass decorated with @event
    When: The decorator is applied without explicit name
    Then: The EventConfig's name should equal the class's __name__
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    decorated = event(PubSub)(_OrderPlaced)

    # Assert
    assert decorated.name == "_OrderPlaced"


def test_event_decorator_accepts_explicit_name() -> None:
    """
    Test that the event decorator passes an explicit name through to EventConfig.

    Given: A Payload subclass and an explicit name
    When: The decorator is applied with name provided
    Then: The EventConfig's name should equal the supplied string
    """

    # Arrange
    class _OrderPlaced(Payload):
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    decorated = event(PubSub, name="order_placed")(_OrderPlaced)

    # Assert
    assert decorated.name == "order_placed"
