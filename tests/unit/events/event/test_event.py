"""
Unit tests for the Event base class.

This test suite verifies the following behaviors:
- Subclasses defined with no kwargs have an empty __event_meta__.
- Subclasses defined with kwargs store those kwargs in __event_meta__.
- Sibling subclasses have independent __event_meta__ dicts.
- Child subclasses inherit and can override parent meta keys.
- Class-level kwargs do not interfere with normal instance construction.
"""

from stratae.events.event import Event


def test_subclass_with_no_kwargs_has_empty_meta():
    """
    An Event subclass defined with no kwargs has an empty __event_meta__.

    Given: An Event subclass defined with no class-level keyword arguments
    When: The class is defined
    Then: __event_meta__ should be an empty dict
    """

    # Arrange & Act
    class MyEvent(Event):
        pass

    # Assert
    assert MyEvent.__event_meta__ == {}


def test_subclass_with_kwargs_stores_meta():
    """
    Test that class-level kwargs are stored in __event_meta__.

    Given: An Event subclass defined with keyword arguments
    When: The class is defined
    Then: __event_meta__ should contain exactly those keyword arguments
    """

    # Arrange & Act
    class MyEvent(Event, topic="test.event", priority=1):
        pass

    # Assert
    assert MyEvent.__event_meta__ == {"topic": "test.event", "priority": 1}


def test_sibling_subclasses_have_independent_meta():
    """
    Test that __event_meta__ is not shared across sibling subclasses.

    Given: Two Event subclasses each defined with different kwargs
    When: Both classes are defined
    Then: Each should have its own __event_meta__ with no cross-contamination
    """

    # Arrange & Act
    class EventA(Event, topic="a"):
        pass

    class EventB(Event, topic="b"):
        pass

    # Assert
    assert EventA.__event_meta__ == {"topic": "a"}
    assert EventB.__event_meta__ == {"topic": "b"}


def test_child_meta_overrides_parent_key():
    """
    Test that a child event overrides a parent's meta key with its own value.

    Given: A parent Event subclass with a topic key and a child that redefines that key
    When: Both classes are defined
    Then: The child's __event_meta__ should contain only its own value for that key,
          and the parent's __event_meta__ should be unchanged
    """

    # Arrange & Act
    class EventA(Event, topic="a"):
        pass

    class EventB(EventA, topic="b"):
        pass

    # Assert
    assert EventA.__event_meta__ == {"topic": "a"}
    assert EventB.__event_meta__ == {"topic": "b"}


def test_child_meta_inherits_and_extends_parent():
    """
    Test that a child event inherits the parent's meta and adds its own keys.

    Given: A parent Event subclass with a topic key and a child that adds a new key
    When: Both classes are defined
    Then: The child's __event_meta__ should contain both the parent's key and its own
    """

    # Arrange & Act
    class EventA(Event, topic="a"):
        pass

    class EventB(EventA, version=1):
        pass

    # Assert
    assert EventB.__event_meta__ == {"topic": "a", "version": 1}


def test_child_meta_overrides_and_extends_parent():
    """
    Test that a child event can simultaneously override a parent key and add new keys.

    Given: A parent Event subclass with multiple meta keys and a child that redefines
           one of those keys while also introducing a new one
    When: Both classes are defined
    Then: The child's __event_meta__ should reflect the overridden value, the inherited
          value, and the new key — all together
    """

    # Arrange & Act
    class EventA(Event, topic="a", version=1):
        pass

    class EventB(EventA, topic="b", priority=2):
        pass

    # Assert
    assert EventB.__event_meta__ == {"topic": "b", "version": 1, "priority": 2}


def test_class_kwargs_do_not_reach_init():
    """
    Test that class-level kwargs do not interfere with normal instance construction.

    Given: An Event subclass with class-level kwargs and a custom __init__
    When: An instance is created with __init__ arguments
    Then: The instance should be created without error and hold its own attributes
    """

    # Arrange
    class MyEvent(Event, topic="test"):
        def __init__(self, topic: str) -> None:
            self.topic = topic

    # Act
    event = MyEvent("foo")

    # Assert
    assert event.topic == "foo"
    assert event.__event_meta__ == {"topic": "test"}
