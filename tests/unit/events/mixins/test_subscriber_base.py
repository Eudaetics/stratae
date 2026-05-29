"""
Unit tests for the SubscriberBase mixin.

This test suite verifies the following behaviors:

SubscriberBase:
- Handler storage is initialised empty.
- subscribe (direct call) adds a handler to the mapping for a channel.
- subscribe (decorator) adds a handler to the mapping for a channel and returns the callable.
- subscribe can register multiple handlers for the same channel.
- subscribe registers handlers independently for different channels.
- unsubscribe removes a registered handler.
- unsubscribe is a no-op when the handler is not registered.
- unsubscribe is a no-op when the channel has no handlers.
"""

from unittest.mock import Mock

import pytest

from stratae.events.channel import Channel
from stratae.events.mixins.subscribe import SubscriberBase


@pytest.fixture
def subscriber_base() -> SubscriberBase[None]:  # pyright: ignore[reportMissingTypeArgument]
    """Return a SubscriberBase instance for storage behaviour tests."""
    return SubscriberBase()


def test_subscriber_base_initialises_empty_handlers(
    subscriber_base: SubscriberBase[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Handler storage should be empty on initialisation.

    Given: A freshly created SubscriberBase instance
    When: The instance is inspected
    Then: Handlers should be empty.
    """
    assert not subscriber_base._handlers  # pyright: ignore[reportPrivateUsage]


def test_subscribe_direct_call_adds_handler(
    subscriber_base: SubscriberBase[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Subscribe called directly should add the handler to the mapping for the channel.

    Given: A SubscriberBase instance, a channel, and a handler callable
    When: subscribe is called with the channel and handler
    Then: The handler should appear in get_handlers for that channel
    """
    # Arrange
    channel = Channel("test")
    fn = Mock()

    # Act
    subscriber_base.subscribe(channel, None, fn)

    # Assert
    assert fn in subscriber_base.get_handlers(channel)


def test_subscribe_decorator_adds_handler_and_returns_callable(
    subscriber_base: SubscriberBase[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Subscribe used as a decorator should register the handler and return the original callable.

    Given: A SubscriberBase instance and a channel
    When: subscribe is used as a decorator factory
    Then: The handler should appear in get_handlers and the original callable is returned
    """
    # Arrange
    channel = Channel("test")
    fn = Mock()

    # Act
    result = subscriber_base.subscribe(channel)(fn)

    # Assert
    assert fn in subscriber_base.get_handlers(channel)
    assert result is fn


def test_subscribe_adds_multiple_handlers_for_same_channel(
    subscriber_base: SubscriberBase[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Subscribe should accumulate multiple handlers for the same channel.

    Given: A SubscriberBase instance and two distinct handler callables
    When: Both are subscribed to the same channel
    Then: Both handlers should appear in get_handlers for that channel
    """
    # Arrange
    channel = Channel("test")
    fn_a = Mock()
    fn_b = Mock()

    # Act
    subscriber_base.subscribe(channel, None, fn_a)
    subscriber_base.subscribe(channel, None, fn_b)

    # Assert
    assert fn_a in subscriber_base.get_handlers(channel)
    assert fn_b in subscriber_base.get_handlers(channel)


def test_subscribe_registers_handlers_independently_per_channel(
    subscriber_base: SubscriberBase[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Subscribe should maintain independent handler sets for different channels.

    Given: A SubscriberBase instance and two handlers for two different channels
    When: Each handler is subscribed to its respective channel
    Then: Each channel's handler set should contain only its own handlers
    """
    # Arrange
    orders = Channel("orders")
    shipments = Channel("shipments")
    fn_a = Mock()
    fn_b = Mock()

    # Act
    subscriber_base.subscribe(orders, None, fn_a)
    subscriber_base.subscribe(shipments, None, fn_b)

    # Assert
    assert fn_a in subscriber_base.get_handlers(orders)
    assert fn_b not in subscriber_base.get_handlers(orders)
    assert fn_b in subscriber_base.get_handlers(shipments)
    assert fn_a not in subscriber_base.get_handlers(shipments)


def test_unsubscribe_removes_registered_handler(
    subscriber_base: SubscriberBase[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Unsubscribe should remove a previously registered handler.

    Given: A SubscriberBase instance with a registered handler
    When: unsubscribe is called with the original callable
    Then: The handler should no longer appear in get_handlers for that channel
    """
    # Arrange
    channel = Channel("test")
    fn = Mock()
    subscriber_base.subscribe(channel, None, fn)

    # Act
    subscriber_base.unsubscribe(channel, fn)

    # Assert
    assert fn not in subscriber_base.get_handlers(channel)


def test_unsubscribe_is_noop_when_handler_not_registered(
    subscriber_base: SubscriberBase[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Unsubscribe should not raise when the handler is not registered.

    Given: A SubscriberBase instance with one handler registered for a channel
    When: unsubscribe is called with a different, unregistered callable
    Then: No exception should be raised and the registered handler should be unaffected
    """
    # Arrange
    channel = Channel("test")
    registered = Mock()
    unregistered = Mock()
    subscriber_base.subscribe(channel, None, registered)

    # Act & Assert
    subscriber_base.unsubscribe(channel, unregistered)
    assert registered in subscriber_base.get_handlers(channel)


def test_unsubscribe_is_noop_when_channel_has_no_handlers(
    subscriber_base: SubscriberBase[None],  # pyright: ignore[reportMissingTypeArgument]
):
    """
    Unsubscribe should not raise when no handlers exist for the channel.

    Given: A SubscriberBase instance with no handlers registered
    When: unsubscribe is called for a channel that has never been subscribed to
    Then: No exception should be raised
    """
    # Arrange
    channel = Channel("test")

    # Act & Assert
    subscriber_base.unsubscribe(channel, Mock())
