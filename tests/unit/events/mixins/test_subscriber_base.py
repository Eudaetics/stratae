"""
Unit tests for the SubscriberBase mixin.

This test suite verifies the following behaviors:

SubscriberBase:
- Handler storage is initialised empty.
- subscribe (direct call) adds a handler to the mapping for a config and returns the Handler.
- subscribe (decorator) adds a handler to the mapping for a config and returns the Handler.
- subscribe can register multiple handlers for the same config.
- subscribe registers handlers independently for different configs.
- subscribe stores config on the handler when passed as a keyword argument (direct call).
- subscribe stores config on the handler when passed as a keyword argument (decorator).
- unsubscribe removes a registered handler.
- unsubscribe is a no-op when the handler is not registered.
- unsubscribe is a no-op when the config has no handlers.
"""

from typing import Any
from unittest.mock import Mock

import pytest

from stratae.events.event import EventSchema
from stratae.events.handler import Handler
from stratae.events.mixins.subscribe import SubscriberBase


@pytest.fixture
def subscriber_base() -> SubscriberBase[Any]:
    """Return a SubscriberBase instance for storage behaviour tests."""
    return SubscriberBase()


def test_subscriber_base_initialises_empty_handlers(subscriber_base: SubscriberBase[Any]):
    """
    Handler storage should be empty on initialisation.

    Given: A freshly created SubscriberBase instance
    When: The instance is inspected
    Then: Handlers should be empty.
    """
    assert not subscriber_base._handlers  # pyright: ignore[reportPrivateUsage]


def test_subscribe_direct_call_adds_handler(subscriber_base: SubscriberBase[Any]):
    """
    Subscribe called directly should add the handler to the mapping for the config.

    Given: A SubscriberBase instance, a config, and a handler callable
    When: subscribe is called with the handler and config
    Then: The returned Handler should appear in get_handlers for that config
    """
    config = object()
    fn = Mock()

    handle = subscriber_base.subscribe(config, fn)

    assert handle in subscriber_base.get_handlers(config)


def test_subscribe_decorator_adds_handler_and_returns_handler(
    subscriber_base: SubscriberBase[Any],
):
    """
    Subscribe used as a decorator should register the handler and return the Handler.

    Given: A SubscriberBase instance and a config
    When: subscribe is used as a decorator factory
    Then: The Handler should appear in get_handlers and it wraps the decorated callable
    """
    config = object()
    fn = Mock()

    handle = subscriber_base.subscribe(config)(fn)

    assert handle in subscriber_base.get_handlers(config)
    assert handle.call is fn


def test_subscribe_adds_multiple_handlers_for_same_config(subscriber_base: SubscriberBase[Any]):
    """
    Subscribe should accumulate multiple handlers for the same config.

    Given: A SubscriberBase instance and two distinct handler callables
    When: Both are subscribed with the same config
    Then: Both Handlers should appear in get_handlers for that config
    """
    config = object()
    fn_a = Mock()
    fn_b = Mock()

    handle_a = subscriber_base.subscribe(config, fn_a)
    handle_b = subscriber_base.subscribe(config, fn_b)

    assert handle_a in subscriber_base.get_handlers(config)
    assert handle_b in subscriber_base.get_handlers(config)


def test_subscribe_registers_handlers_independently_per_config(
    subscriber_base: SubscriberBase[Any],
):
    """
    Subscribe should maintain independent handler sets for different configs.

    Given: A SubscriberBase instance and two handlers for two different configs
    When: Each handler is subscribed with its respective config
    Then: Each config's handler set should contain only its own handlers
    """
    config_a = object()
    config_b = object()
    fn_a = Mock()
    fn_b = Mock()

    handle_a = subscriber_base.subscribe(config_a, fn_a)
    handle_b = subscriber_base.subscribe(config_b, fn_b)

    assert handle_a in subscriber_base.get_handlers(config_a)
    assert handle_b not in subscriber_base.get_handlers(config_a)
    assert handle_b in subscriber_base.get_handlers(config_b)
    assert handle_a not in subscriber_base.get_handlers(config_b)


def test_subscribe_direct_call_stores_config(subscriber_base: SubscriberBase[Any]):
    """
    Subscribe should store the provided config on the registered handler.

    Given: A SubscriberBase instance, a handler, and a config object
    When: subscribe is called with the handler and config as a keyword argument
    Then: The returned Handler's config attribute should be the provided config instance
    """
    config = object()
    fn = Mock()

    handle = subscriber_base.subscribe(config, fn)

    assert handle.config is config


def test_subscribe_decorator_stores_config(subscriber_base: SubscriberBase[Any]):
    """
    Subscribe used as a decorator should store the provided config on the registered handler.

    Given: A SubscriberBase instance and a config object
    When: subscribe is used as a decorator factory with config as a keyword argument
    Then: The returned Handler's config attribute should be the provided config instance
    """
    config = object()

    @subscriber_base.subscribe(config)
    def fn(_: EventSchema) -> None: ...

    assert fn.config is config


def test_unsubscribe_removes_registered_handler(subscriber_base: SubscriberBase[Any]):
    """
    Unsubscribe should remove a previously registered handler.

    Given: A SubscriberBase instance with a registered handler
    When: unsubscribe is called with the Handler returned by subscribe
    Then: The handler should no longer appear in get_handlers for that config
    """
    config = object()
    fn = Mock()
    handle = subscriber_base.subscribe(config, fn)

    subscriber_base.unsubscribe(handle)

    assert handle not in subscriber_base.get_handlers(config)


def test_unsubscribe_is_noop_when_handler_not_registered(subscriber_base: SubscriberBase[Any]):
    """
    Unsubscribe should not raise when the handler is not registered.

    Given: A SubscriberBase instance with one handler registered
    When: unsubscribe is called with a different, unregistered Handler
    Then: No exception should be raised and the registered handler should be unaffected
    """
    config = object()
    registered = Mock()
    handle = subscriber_base.subscribe(config, registered)
    unregistered_handle = Handler(Mock(), object())

    subscriber_base.unsubscribe(unregistered_handle)

    assert handle in subscriber_base.get_handlers(config)


def test_unsubscribe_is_noop_when_config_has_no_handlers(subscriber_base: SubscriberBase[Any]):
    """
    Unsubscribe should not raise when no handlers exist for the config.

    Given: A SubscriberBase instance with no handlers registered
    When: unsubscribe is called for a config that has never been subscribed to
    Then: No exception should be raised
    """
    subscriber_base.unsubscribe(Handler(Mock(), object()))
