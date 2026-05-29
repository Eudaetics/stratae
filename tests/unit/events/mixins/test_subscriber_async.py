"""
Unit tests for the AsyncSubscriber mixin.

This test suite verifies the following behaviors:

AsyncSubscriber:
- AsyncSubscriber cannot be instantiated directly (abstract).
- AsyncSubscriber inherits storage behaviour from SubscriberBase.
- subscribe adds a handler to the mapping for a channel.
- unsubscribe removes a previously registered handler.
"""

from unittest.mock import Mock, patch

import pytest

from stratae.events.channel import Channel
from stratae.events.mixins.subscribe import AsyncSubscriber, SubscriberBase


@pytest.fixture
def async_subscriber() -> AsyncSubscriber[None]:
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


def test_async_subscriber_inherits_from_subscriber_base(async_subscriber: AsyncSubscriber[None]):
    """
    AsyncSubscriber should be an instance of SubscriberBase.

    Given: An AsyncSubscriber instance with abstract methods cleared
    When: The instance is checked against SubscriberBase
    Then: It should be an instance of SubscriberBase
    """
    assert isinstance(async_subscriber, SubscriberBase)


def test_async_subscriber_subscribe_adds_handler(async_subscriber: AsyncSubscriber[None]):
    """
    Subscribe should add the handler to the mapping for the given channel.

    Given: An AsyncSubscriber instance, a channel, and a handler callable
    When: subscribe is called with the channel and handler
    Then: The returned Handler should appear in get_handlers for that channel
    """
    # Arrange
    channel = Channel("test")
    fn = Mock()

    # Act
    handle = async_subscriber.subscribe(channel, fn)

    # Assert
    assert handle in async_subscriber.get_handlers(channel)


def test_async_subscriber_unsubscribe_removes_handler(async_subscriber: AsyncSubscriber[None]):
    """
    Unsubscribe should remove a previously registered handler.

    Given: An AsyncSubscriber instance with a registered handler
    When: unsubscribe is called with the Handler returned by subscribe
    Then: The handler should no longer appear in get_handlers for that channel
    """
    # Arrange
    channel = Channel("test")
    fn = Mock()
    handle = async_subscriber.subscribe(channel, fn)

    # Act
    async_subscriber.unsubscribe(channel, handle)

    # Assert
    assert handle not in async_subscriber.get_handlers(channel)
