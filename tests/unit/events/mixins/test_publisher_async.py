"""
Unit tests for the AsyncPublisher mixin.

This test suite verifies the following behaviors:

AsyncPublisher:
- publish returns an AsyncBoundEvent bound to emit_publish.
- The AsyncBoundEvent stores the correct schema, emitter, and meta.
- AsyncPublisher cannot be instantiated directly (abstract).
- Awaiting the result of calling the AsyncBoundEvent calls emit_publish with meta and event.
- Awaiting the result of calling the AsyncBoundEvent with no meta calls emit_publish with None.
- The resolved return value from emit_publish is returned to the caller.
"""

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from pytest_mock import MockerFixture

from stratae.events.channel import Channel
from stratae.events.event import AsyncBoundEvent, EventMeta, EventSchema
from stratae.events.mixins.publish import AsyncPublisher


class _ItemShipped(EventSchema):
    def __init__(self, item_id: int, quantity: int) -> None:
        self.item_id = item_id
        self.quantity = quantity

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _ItemShipped):
            return False
        return self.item_id == value.item_id and self.quantity == value.quantity


@pytest.fixture
def async_publisher() -> AsyncPublisher[EventMeta, None]:
    """Yield an AsyncPublisher instance with abstract methods cleared for testing."""
    with patch.object(AsyncPublisher, "__abstractmethods__", frozenset[str]()):
        return AsyncPublisher()  # pyright: ignore[reportAbstractUsage]


@pytest.fixture
def none_async_publisher() -> AsyncPublisher[None, None]:
    """Return an AsyncPublisher[None, None] instance for testing the no-meta path."""
    with patch.object(AsyncPublisher, "__abstractmethods__", frozenset[str]()):
        return AsyncPublisher()  # pyright: ignore[reportAbstractUsage]


@pytest.fixture
def meta() -> EventMeta:
    """Return an EventMeta instance for use in publish calls."""
    return EventMeta()


def test_async_publish_bus_is_abstract():
    """
    AsyncPublisher should raise TypeError when instantiated directly.

    Given: The abstract AsyncPublisher class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncPublisher"):
        AsyncPublisher()  # pyright: ignore[reportAbstractUsage]


def test_async_publish_returns_async_bound_event(
    async_publisher: AsyncPublisher[EventMeta, None], meta: EventMeta
):
    """
    Publish should return an AsyncBoundEvent instance.

    Given: An AsyncPublisher instance with abstract methods cleared
    When: publish is called with a channel, schema, and meta
    Then: An AsyncBoundEvent instance should be returned
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = async_publisher.publish(channel, _ItemShipped, meta=meta)

    # Assert
    assert isinstance(bound, AsyncBoundEvent)


def test_async_publish_bound_event_stores_schema_emitter_and_meta(
    async_publisher: AsyncPublisher[EventMeta, None], meta: EventMeta
):
    """
    AsyncBoundEvent returned by publish should store the channel, schema, emit_publish, and meta.

    Given: An AsyncPublisher instance with abstract methods cleared
    When: publish is called with a channel, schema, and meta
    Then: The AsyncBoundEvent should store that channel, schema, emit_publish, and meta
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = async_publisher.publish(channel, _ItemShipped, meta=meta)

    # Assert
    assert bound.channel is channel
    assert bound.schema is _ItemShipped
    assert bound.emitter == async_publisher.emit_publish
    assert bound.meta is meta


async def test_async_publish_bound_event_calls_emit_publish_with_positional_args(
    async_publisher: AsyncPublisher[EventMeta, None], meta: EventMeta, mocker: MockerFixture
):
    """
    AsyncBoundEvent called with positional args should construct the event and call emit_publish.

    Given: An AsyncBoundEvent returned by an AsyncPublisher's publish
    When: The AsyncBoundEvent is called with positional arguments and the result is awaited
    Then: emit_publish should be called with the meta and the constructed event
    """
    # Arrange
    mock_emit = mocker.patch.object(async_publisher, "emit_publish", new=AsyncMock())
    channel = Channel("test")
    bound = async_publisher.publish(channel, _ItemShipped, meta=meta)

    # Act
    await bound(1, 10)

    # Assert
    mock_emit.assert_called_once_with(channel, meta, _ItemShipped(1, 10))


async def test_async_publish_bound_event_calls_emit_publish_with_keyword_args(
    async_publisher: AsyncPublisher[EventMeta, None], meta: EventMeta, mocker: MockerFixture
):
    """
    AsyncBoundEvent called with keyword args should construct the event and call emit_publish.

    Given: An AsyncBoundEvent returned by an AsyncPublisher's publish
    When: The AsyncBoundEvent is called with keyword arguments and the result is awaited
    Then: emit_publish should be called with the meta and the constructed event
    """
    # Arrange
    mock_emit = mocker.patch.object(async_publisher, "emit_publish", new=AsyncMock())
    channel = Channel("test")
    bound = async_publisher.publish(channel, _ItemShipped, meta=meta)

    # Act
    await bound(item_id=2, quantity=5)

    # Assert
    mock_emit.assert_called_once_with(channel, meta, _ItemShipped(2, 5))


async def test_async_publish_bound_event_returns_emit_publish_result(
    async_publisher: AsyncPublisher[EventMeta, None], meta: EventMeta
):
    """
    Resolved return value from emit_publish should be returned to the caller.

    Given: An AsyncBoundEvent returned by an AsyncPublisher whose emit_publish resolves to a
    known value
    When: The AsyncBoundEvent is called and the result is awaited
    Then: The return value should match what emit_publish resolved to
    """
    # Arrange
    mock_emit = AsyncMock(return_value="dispatched")
    async_publisher.emit_publish = mock_emit
    channel = Channel("test")
    bound = async_publisher.publish(channel, _ItemShipped, meta=meta)

    # Act
    result = await bound(1, 10)

    # Assert
    assert result == "dispatched"


def test_async_publish_without_meta_returns_async_bound_event(
    none_async_publisher: AsyncPublisher[None, None],
):
    """
    Publish called without meta should return an AsyncBoundEvent with meta set to None.

    Given: An AsyncPublisher[None, None] instance with abstract methods cleared
    When: publish is called with only a channel and schema
    Then: An AsyncBoundEvent should be returned with meta set to None
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = none_async_publisher.publish(channel, _ItemShipped)

    # Assert
    assert isinstance(bound, AsyncBoundEvent)
    assert bound.meta is None


async def test_async_publish_without_meta_calls_emit_publish_with_none(
    none_async_publisher: AsyncPublisher[None, None], mocker: MockerFixture
):
    """
    AsyncBoundEvent from a no-meta publish should call emit_publish with None as meta.

    Given: An AsyncBoundEvent returned by publish with no meta
    When: The AsyncBoundEvent is called and awaited
    Then: emit_publish should be called with None as meta
    """
    # Arrange
    mock_emit = mocker.patch.object(none_async_publisher, "emit_publish", new=AsyncMock())
    channel = Channel("test")
    bound = none_async_publisher.publish(channel, _ItemShipped)

    # Act
    await bound(1, 10)

    # Assert
    mock_emit.assert_called_once_with(channel, None, _ItemShipped(1, 10))
