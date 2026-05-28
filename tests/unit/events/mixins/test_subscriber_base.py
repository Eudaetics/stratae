"""
Unit tests for the SubscriberBase mixin.

This test suite verifies the following behaviors:

SubscriberBase:
- Handler storage is initialised empty.
- subscribe adds a handler to the mapping for an event type.
- subscribe can register multiple handlers for the same event type.
- subscribe registers handlers independently for different event types.
- unsubscribe removes a registered handler.
- unsubscribe is a no-op when the handler is not registered.
- unsubscribe is a no-op when the event type has no handlers.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from stratae.events.event import EventSchema
from stratae.events.mixins.subscribe import SubscriberBase


class _ItemShipped(EventSchema):
    def __init__(self, item_id: int, quantity: int) -> None:
        self.item_id = item_id
        self.quantity = quantity

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _ItemShipped):
            return False
        return self.item_id == value.item_id and self.quantity == value.quantity


class _OrderCancelled(EventSchema):
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


@pytest.fixture
def subscriber_base() -> SubscriberBase:
    """Return a SubscriberBase instance for storage behaviour tests."""
    return SubscriberBase()


def test_subscriber_base_initialises_empty_handlers(subscriber_base: SubscriberBase):
    """
    Handler storage should be empty on initialisation.

    Given: A freshly created SubscriberBase instance
    When: The instance is inspected
    Then: Handlers should be empty.
    """
    assert not subscriber_base._handlers  # pyright: ignore[reportPrivateUsage]


def test_subscribe_adds_handler_to_mapping(subscriber_base: SubscriberBase):
    """
    Subscribe should add the handler to the mapping for the given event type.

    Given: A SubscriberBase instance and a handler callable
    When: subscribe is called with an event type and the handler
    Then: The handler should appear in get_handlers for that event type
    """
    # Arrange
    handler = Mock()

    # Act
    subscriber_base.subscribe(_ItemShipped, handler)

    # Assert
    assert handler in subscriber_base.get_handlers(_ItemShipped)


def test_subscribe_adds_multiple_handlers_for_same_event(subscriber_base: SubscriberBase):
    """
    Subscribe should accumulate multiple handlers for the same event type.

    Given: A SubscriberBase instance and two distinct handler callables
    When: Both are subscribed to the same event type
    Then: Both handlers should appear in _handlers for that event type
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()

    # Act
    subscriber_base.subscribe(_ItemShipped, handler_a)
    subscriber_base.subscribe(_ItemShipped, handler_b)

    # Assert
    assert handler_a in subscriber_base.get_handlers(_ItemShipped)
    assert handler_b in subscriber_base.get_handlers(_ItemShipped)


def test_subscribe_registers_handlers_independently_per_event(subscriber_base: SubscriberBase):
    """
    Subscribe should maintain independent handler sets for different event types.

    Given: A SubscriberBase instance and two handlers for two different event types
    When: Each handler is subscribed to its respective event type
    Then: Each event type's handler set should contain only its own handlers
    """
    # Arrange
    handler_a = Mock()
    handler_b = Mock()

    # Act
    subscriber_base.subscribe(_ItemShipped, handler_a)
    subscriber_base.subscribe(_OrderCancelled, handler_b)

    # Assert
    assert handler_a in subscriber_base.get_handlers(_ItemShipped)
    assert handler_b not in subscriber_base.get_handlers(_ItemShipped)
    assert handler_b in subscriber_base.get_handlers(_OrderCancelled)
    assert handler_a not in subscriber_base.get_handlers(_OrderCancelled)


def test_unsubscribe_removes_registered_handler(subscriber_base: SubscriberBase):
    """
    Unsubscribe should remove a previously registered handler.

    Given: A SubscriberBase instance with a registered handler
    When: unsubscribe is called with that handler
    Then: The handler should no longer appear in get_handlers for that event type
    """
    # Arrange
    handler = Mock()
    subscriber_base.subscribe(_ItemShipped, handler)

    # Act
    subscriber_base.unsubscribe(_ItemShipped, handler)

    # Assert
    assert handler not in subscriber_base.get_handlers(_ItemShipped)


def test_unsubscribe_is_noop_when_handler_not_registered(subscriber_base: SubscriberBase):
    """
    Unsubscribe should not raise when the handler is not registered.

    Given: A SubscriberBase instance with one handler registered for an event type
    When: unsubscribe is called with a different, unregistered handler
    Then: No exception should be raised and the registered handler should be unaffected
    """
    # Arrange
    registered = Mock()
    unregistered = Mock()
    subscriber_base.subscribe(_ItemShipped, registered)

    # Act & Assert
    subscriber_base.unsubscribe(_ItemShipped, unregistered)
    assert registered in subscriber_base.get_handlers(_ItemShipped)


def test_unsubscribe_is_noop_when_event_has_no_handlers(subscriber_base: SubscriberBase):
    """
    Unsubscribe should not raise when no handlers exist for the event type.

    Given: A SubscriberBase instance with no handlers registered
    When: unsubscribe is called for an event type that has never been subscribed to
    Then: No exception should be raised
    """
    # Act & Assert
    subscriber_base.unsubscribe(_ItemShipped, Mock())
