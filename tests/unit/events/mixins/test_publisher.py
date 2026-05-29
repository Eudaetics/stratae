"""
Unit tests for the Publisher mixin.

This test suite verifies the following behaviors:

Publisher:
- publish returns a BoundEvent bound to emit_publish.
- The BoundEvent stores the correct schema, emitter, and meta.
- Publisher cannot be instantiated directly (abstract).
- Calling the BoundEvent constructs the event and calls emit_publish with meta and event.
- Calling the BoundEvent with no meta calls emit_publish with None.
- The return value from emit_publish is returned to the caller.

"""

from typing import Any
from unittest.mock import Mock, patch

import pytest
from pytest_mock import MockerFixture

from stratae.events.channel import Channel
from stratae.events.event import BoundEvent, EventMeta, EventSchema
from stratae.events.mixins.publish import Publisher


class _ItemShipped(EventSchema):
    def __init__(self, item_id: int, quantity: int) -> None:
        self.item_id = item_id
        self.quantity = quantity

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _ItemShipped):
            return False
        return self.item_id == value.item_id and self.quantity == value.quantity


@pytest.fixture
def publisher() -> Publisher[EventMeta, None]:
    """Yield a Publisher instance with abstract methods cleared for testing."""
    with patch.object(Publisher, "__abstractmethods__", frozenset[str]()):
        return Publisher()  # pyright: ignore[reportAbstractUsage]


@pytest.fixture
def none_publisher() -> Publisher[None, None]:
    """Return a Publisher[None, None] instance for testing the no-meta path."""
    with patch.object(Publisher, "__abstractmethods__", frozenset[str]()):
        return Publisher()  # pyright: ignore[reportAbstractUsage]


@pytest.fixture
def meta() -> EventMeta:
    """Return an EventMeta instance for use in publish calls."""
    return EventMeta()


def test_publish_bus_is_abstract():
    """
    Publisher should raise TypeError when instantiated directly.

    Given: The abstract Publisher class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class Publisher"):
        Publisher()  # pyright: ignore[reportAbstractUsage]


def test_publish_returns_bound_event(publisher: Publisher[EventMeta, None], meta: EventMeta):
    """
    Publish should return a BoundEvent instance.

    Given: A Publisher instance with abstract methods cleared
    When: publish is called with a channel, schema, and meta
    Then: A BoundEvent instance should be returned
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = publisher.publish(channel, _ItemShipped, meta=meta)

    # Assert
    assert isinstance(bound, BoundEvent)


def test_publish_bound_event_stores_schema_emitter_and_meta(
    publisher: Publisher[EventMeta, None], meta: EventMeta
):
    """
    BoundEvent returned by publish should store the channel, schema, emit_publish, and meta.

    Given: A Publisher instance with abstract methods cleared
    When: publish is called with a channel, schema, and meta
    Then: The BoundEvent should store that channel, schema, emit_publish, and meta
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = publisher.publish(channel, _ItemShipped, meta=meta)

    # Assert
    assert bound.channel is channel
    assert bound.schema is _ItemShipped
    assert bound.emitter == publisher.emit_publish
    assert bound.meta is meta


def test_publish_bound_event_calls_emit_publish_with_positional_args(
    publisher: Publisher[EventMeta, None], meta: EventMeta, mocker: MockerFixture
):
    """
    BoundEvent called with positional args should construct the event and call emit_publish.

    Given: A BoundEvent returned by publish
    When: The BoundEvent is called with positional arguments
    Then: emit_publish should be called with the meta and the constructed event
    """
    # Arrange
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock())
    channel = Channel("test")
    bound = publisher.publish(channel, _ItemShipped, meta=meta)

    # Act
    bound(1, 10)

    # Assert
    mock_emit.assert_called_once_with(channel, meta, _ItemShipped(1, 10))


def test_publish_bound_event_calls_emit_publish_with_keyword_args(
    publisher: Publisher[EventMeta, None], meta: EventMeta, mocker: MockerFixture
):
    """
    BoundEvent called with keyword args should construct the event and call emit_publish.

    Given: A BoundEvent returned by publish
    When: The BoundEvent is called with keyword arguments
    Then: emit_publish should be called with the meta and the constructed event
    """
    # Arrange
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock())
    channel = Channel("test")
    bound = publisher.publish(channel, _ItemShipped, meta=meta)

    # Act
    bound(item_id=2, quantity=5)

    # Assert
    mock_emit.assert_called_once_with(channel, meta, _ItemShipped(2, 5))


def test_publish_bound_event_returns_emit_publish_result(
    publisher: Publisher[EventMeta, None], meta: EventMeta
):
    """
    Return value from emit_publish should be returned to the caller.

    Given: A BoundEvent returned by publish whose emit_publish returns a known value
    When: The BoundEvent is called
    Then: The return value should match what emit_publish returned
    """
    # Arrange
    mock_emit = Mock(return_value="dispatched")
    publisher.emit_publish = mock_emit
    channel = Channel("test")
    bound = publisher.publish(channel, _ItemShipped, meta=meta)

    # Act
    result = bound(1, 10)

    # Assert
    assert result == "dispatched"


def test_publish_without_meta_returns_bound_event(none_publisher: Publisher[None, None]):
    """
    Publish called without meta should return a BoundEvent with meta set to None.

    Given: A Publisher[None, None] instance with abstract methods cleared
    When: publish is called with only a channel and schema
    Then: A BoundEvent should be returned with meta set to None
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = none_publisher.publish(channel, _ItemShipped)

    # Assert
    assert isinstance(bound, BoundEvent)
    assert bound.meta is None


def test_publish_without_meta_calls_emit_publish_with_none(
    none_publisher: Publisher[None, None], mocker: MockerFixture
):
    """
    BoundEvent from a no-meta publish should call emit_publish with None as meta.

    Given: A BoundEvent returned by publish with no meta
    When: The BoundEvent is called
    Then: emit_publish should be called with None as meta
    """
    # Arrange
    mock_emit = mocker.patch.object(none_publisher, "emit_publish", new=Mock())
    channel = Channel("test")
    bound = none_publisher.publish(channel, _ItemShipped)

    # Act
    bound(1, 10)

    # Assert
    mock_emit.assert_called_once_with(channel, None, _ItemShipped(1, 10))
