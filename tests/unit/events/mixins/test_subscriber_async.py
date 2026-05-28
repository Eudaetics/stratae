"""
Unit tests for the AsyncSubscriber mixin.

This test suite verifies the following behaviors:

AsyncSubscriber:
- AsyncSubscriber cannot be instantiated directly (abstract).
- AsyncSubscriber inherits storage behaviour from SubscriberBase.
- subscribe adds a handler to the mapping for an event type.
- unsubscribe removes a previously registered handler.
"""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from stratae.events.event import Event
from stratae.events.mixins.subscribe import AsyncSubscriber, SubscriberBase


class _ItemShipped(Event):
    def __init__(self, item_id: int, quantity: int) -> None:
        self.item_id = item_id
        self.quantity = quantity

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _ItemShipped):
            return False
        return self.item_id == value.item_id and self.quantity == value.quantity


@pytest.fixture
def async_subscriber() -> AsyncSubscriber:
    """Return an AsyncSubscriber instance with abstract methods cleared for testing."""
    with patch.object(AsyncSubscriber, "__abstractmethods__", frozenset[str]()):
        return AsyncSubscriber()  # pyright: ignore[reportAbstractUsage]


def test_async_subscriber_is_abstract():
    """
    AsyncSubscriber should raise TypeError when instantiated directly.

    Given: The abstract AsyncSubscriber class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncSubscriber"):
        AsyncSubscriber()  # pyright: ignore[reportAbstractUsage]


def test_async_subscriber_inherits_from_subscriber_base(async_subscriber: AsyncSubscriber):
    """
    AsyncSubscriber should be an instance of SubscriberBase.

    Given: An AsyncSubscriber instance with abstract methods cleared
    When: The instance is checked against SubscriberBase
    Then: It should be an instance of SubscriberBase
    """
    assert isinstance(async_subscriber, SubscriberBase)


def test_async_subscriber_subscribe_adds_handler(async_subscriber: AsyncSubscriber):
    """
    Subscribe should add the handler to the mapping for the given event type.

    Given: An AsyncSubscriber instance and a handler callable
    When: subscribe is called with an event type and the handler
    Then: The handler should appear in get_handlers for that event type
    """
    # Arrange
    handler = Mock()

    # Act
    async_subscriber.subscribe(_ItemShipped, handler)

    # Assert
    assert handler in async_subscriber.get_handlers(_ItemShipped)


def test_async_subscriber_unsubscribe_removes_handler(async_subscriber: AsyncSubscriber):
    """
    Unsubscribe should remove a previously registered handler.

    Given: An AsyncSubscriber instance with a registered handler
    When: unsubscribe is called with that handler
    Then: The handler should no longer appear in get_handlers for that event type
    """
    # Arrange
    handler = Mock()
    async_subscriber.subscribe(_ItemShipped, handler)

    # Act
    async_subscriber.unsubscribe(_ItemShipped, handler)

    # Assert
    assert handler not in async_subscriber.get_handlers(_ItemShipped)
