"""
Unit tests for the Subscriber mixin.

This test suite verifies the following behaviors:

Subscriber:
- Subscriber cannot be instantiated directly (abstract).
- Subscriber inherits storage behaviour from SubscriberBase.
"""

from unittest.mock import Mock, patch

import pytest

from stratae.events.channel import Channel
from stratae.events.mixins.subscribe import Subscriber, SubscriberBase


@pytest.fixture
def subscriber() -> Subscriber[None]:  # pyright: ignore[reportMissingTypeArgument]
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


def test_subscriber_inherits_from_subscriber_base(
    subscriber: Subscriber[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Subscriber should be an instance of SubscriberBase.

    Given: A Subscriber instance with abstract methods cleared
    When: The instance is checked against SubscriberBase
    Then: It should be an instance of SubscriberBase
    """
    assert isinstance(subscriber, SubscriberBase)


def test_subscriber_inherits_storage_from_subscriber_base(
    subscriber: Subscriber[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Subscriber should inherit subscribe and unsubscribe from SubscriberBase.

    Given: A Subscriber instance with abstract methods cleared
    When: A handler is subscribed and then unsubscribed
    Then: Storage should reflect both operations correctly
    """
    # Arrange
    channel = Channel("test")
    fn = Mock()

    # Act
    subscriber.subscribe(channel, None, fn)

    # Assert
    assert fn in subscriber.get_handlers(channel)

    # Act
    subscriber.unsubscribe(channel, fn)

    # Assert
    assert fn not in subscriber.get_handlers(channel)
