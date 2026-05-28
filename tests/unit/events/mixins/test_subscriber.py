"""
Unit tests for the Subscriber mixin.

This test suite verifies the following behaviors:

Subscriber:
- Subscriber cannot be instantiated directly (abstract).
- Subscriber inherits storage behaviour from SubscriberBase.
"""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from stratae.events.event import Event
from stratae.events.mixins.subscribe import Subscriber, SubscriberBase


class _ItemShipped(Event):
    def __init__(self, item_id: int, quantity: int) -> None:
        self.item_id = item_id
        self.quantity = quantity

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _ItemShipped):
            return False
        return self.item_id == value.item_id and self.quantity == value.quantity


@pytest.fixture
def subscriber() -> Subscriber:
    """Return a Subscriber instance with abstract methods cleared for testing."""
    with patch.object(Subscriber, "__abstractmethods__", frozenset[str]()):
        return Subscriber()  # pyright: ignore[reportAbstractUsage]


def test_subscriber_is_abstract():
    """
    Subscriber should raise TypeError when instantiated directly.

    Given: The abstract Subscriber class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """


with pytest.raises(TypeError, match="Can't instantiate abstract class Subscriber"):
    Subscriber()  # pyright: ignore[reportAbstractUsage]


def test_subscriber_inherits_from_subscriber_base(subscriber: Subscriber):
    """
    Subscriber should be an instance of SubscriberBase.

    Given: A Subscriber instance with abstract methods cleared
    When: The instance is checked against SubscriberBase
    Then: It should be an instance of SubscriberBase
    """
    assert isinstance(subscriber, SubscriberBase)


def test_subscriber_inherits_storage_from_subscriber_base(subscriber: Subscriber):
    """
    Subscriber should inherit subscribe and unsubscribe from SubscriberBase.

    Given: A Subscriber instance with abstract methods cleared
    When: A handler is subscribed and then unsubscribed
    Then: Storage should reflect both operations correctly
    """
    # Arrange
    handler = Mock()

    # Act
    subscriber.subscribe(_ItemShipped, handler)

    # Assert
    assert handler in subscriber.get_handlers(_ItemShipped)

    # Act
    subscriber.unsubscribe(_ItemShipped, handler)

    # Assert
    assert handler not in subscriber.get_handlers(_ItemShipped)
