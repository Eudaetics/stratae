"""
Unit tests for the Publisher mixin.

This test suite verifies the following behaviors:

Publisher:
- Publisher cannot be instantiated directly (abstract).
- publish returns a BoundEvent bound to emit_publish.
- The BoundEvent stores the correct EventConfig, emitter, and config.
- Calling the BoundEvent constructs the event and calls emit_publish with the payload and itself.
- Calling the BoundEvent with keyword args calls emit_publish with the payload and itself.
- The return value from emit_publish is returned to the caller.
- Each call to publish returns a distinct BoundEvent instance.
"""

from dataclasses import dataclass
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stratae.events.bound import BoundEvent
from stratae.events.event import Payload, PubSub, event
from stratae.events.mixins.publish import Publisher


@pytest.fixture
def publisher() -> Publisher[str, None]:
    """Return a Publisher instance with abstract methods cleared for testing."""
    from unittest.mock import patch

    with patch.object(Publisher, "__abstractmethods__", frozenset[str]()):
        return Publisher[str, None]()  # pyright: ignore[reportAbstractUsage]


def test_publish_bus_is_abstract():
    """
    Publisher should raise TypeError when instantiated directly.

    Given: The abstract Publisher class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class Publisher"):
        Publisher()  # pyright: ignore[reportAbstractUsage]


def test_publish_returns_bound_event(publisher: Publisher[str, None]):
    """
    Publish should return a BoundEvent instance.

    Given: A Publisher instance with abstract methods cleared
    When: publish is used as a decorator factory with a config
    Then: A BoundEvent instance should be returned
    """

    @dataclass
    class _ItemShipped(Payload): ...

    bound = publisher.publish(event(PubSub)(_ItemShipped), config="orders")

    assert isinstance(bound, BoundEvent)


def test_publish_stores_event_emitter_and_config(publisher: Publisher[str, None]):
    """
    BoundEvent returned by publish should store the EventConfig, emit_publish, and config.

    Given: A Publisher instance with abstract methods cleared
    When: publish is used as a decorator factory with a config
    Then: The BoundEvent should store an EventConfig, emit_publish, and config
    """

    @dataclass
    class _ItemShipped(Payload):
        item_id: int
        quantity: int

    _item_shipped = event(PubSub)(_ItemShipped)
    bound = publisher.publish(_item_shipped, config="orders")

    assert bound.event is _item_shipped
    assert bound.emitter == publisher.emit_publish
    assert bound.config == "orders"


def test_publish_bound_event_calls_emit_publish_with_positional_args(
    publisher: Publisher[str, None], mocker: MockerFixture
):
    """
    BoundEvent called with positional args should construct the event and call emit_publish.

    Given: A BoundEvent returned by publish
    When: The BoundEvent is called with positional arguments
    Then: emit_publish should be called with the constructed payload and the BoundEvent itself
    """
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock())

    @dataclass
    class _ItemShipped(Payload):
        item_id: int
        quantity: int

    bound = publisher.publish(event(PubSub)(_ItemShipped), config="orders")
    bound(1, 10)

    mock_emit.assert_called_once_with(_ItemShipped(1, 10), bound)


def test_publish_bound_event_calls_emit_publish_with_keyword_args(
    publisher: Publisher[str, None], mocker: MockerFixture
):
    """
    BoundEvent called with keyword args should construct the event and call emit_publish.

    Given: A BoundEvent returned by publish
    When: The BoundEvent is called with keyword arguments
    Then: emit_publish should be called with the constructed payload and the BoundEvent itself
    """
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock())

    @dataclass
    class _ItemShipped(Payload):
        item_id: int
        quantity: int

    bound = publisher.publish(event(PubSub)(_ItemShipped), config="orders")
    bound(item_id=2, quantity=5)

    mock_emit.assert_called_once_with(_ItemShipped(item_id=2, quantity=5), bound)


def test_publish_bound_event_returns_emit_publish_result(
    publisher: Publisher[str, None], mocker: MockerFixture
):
    """
    The return value from emit_publish should propagate out of the BoundEvent call.

    Given: A BoundEvent created via the decorator factory
    When: The BoundEvent is called
    Then: The return value should match what emit_publish returned
    """
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock(return_value="dispatched"))

    @dataclass
    class _ItemShipped(Payload):
        item_id: int
        quantity: int

    bound = publisher.publish(event(PubSub)(_ItemShipped), config="orders")
    result = bound(1, 10)

    assert result == mock_emit.return_value


def test_publish_returns_distinct_bound_events(publisher: Publisher[str, None]):
    """
    Each call to publish should return a distinct BoundEvent instance.

    Given: A Publisher instance with abstract methods cleared
    When: publish is called twice with the same EventConfig
    Then: The two BoundEvents should be different objects
    """

    @event(PubSub)
    @dataclass
    class _ItemShipped(Payload):
        item_id: int
        quantity: int

    first = publisher.publish(_ItemShipped, config="orders")
    second = publisher.publish(_ItemShipped, config="orders")

    assert first is not second
