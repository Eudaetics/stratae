"""
Unit tests for the Publisher mixin.

This test suite verifies the following behaviors:

AsyncPublisher:
- publish returns a BoundEvent bound to emit_publish (inherited).
- The BoundEvent stores the correct event class and emitter.
- AsyncPublisher cannot be instantiated directly (abstract).
- Awaiting the result of calling the BoundEvent calls emit_publish with the constructed event.
- The resolved return value from emit_publish is returned to the caller.
"""

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from pytest_mock import MockerFixture

from stratae.events.event import BoundEvent, EventSchema
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
def async_publisher() -> AsyncPublisher[None]:
    """Yield an AsyncPublisher instance with abstract methods cleared for testing."""
    with patch.object(AsyncPublisher, "__abstractmethods__", frozenset[str]()):
        return AsyncPublisher()  # pyright: ignore[reportAbstractUsage]


def test_async_publish_bus_is_abstract():
    """
    AsyncPublisher should raise TypeError when instantiated directly.

    Given: The abstract AsyncPublisher class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncPublisher"):
        AsyncPublisher()  # pyright: ignore[reportAbstractUsage]


def test_async_publish_returns_bound_event(async_publisher: AsyncPublisher[None]):
    """
    Publish should return a BoundEvent instance.

    Given: An AsyncPublisher instance with abstract methods cleared
    When: publish is called with an event class
    Then: A BoundEvent instance should be returned
    """
    # Act
    bound = async_publisher.publish(_ItemShipped)

    # Assert
    assert isinstance(bound, BoundEvent)


def test_async_publish_bound_event_stores_event_and_emitter(async_publisher: AsyncPublisher[None]):
    """
    BoundEvent returned by publish should store the event class and emit_publish.

    Given: An AsyncPublisher instance with abstract methods cleared
    When: publish is called with an event class
    Then: The BoundEvent should store that event class and emit_publish as the emitter
    """
    # Act
    bound = async_publisher.publish(_ItemShipped)

    # Assert
    assert bound.event is _ItemShipped
    assert bound.emitter == async_publisher.emit_publish


async def test_async_publish_bound_event_calls_emit_publish_with_positional_args(
    async_publisher: AsyncPublisher[None], mocker: MockerFixture
):
    """
    BoundEvent called with positional args should construct the event and call emit_publish.

    Given: A BoundEvent returned by an AsyncPublisher's publish
    When: The BoundEvent is called with positional arguments and the result is awaited
    Then: emit_publish should be called with the constructed event
    """
    # Arrange
    mock_emit = mocker.patch.object(async_publisher, "emit_publish", new=AsyncMock())
    bound = async_publisher.publish(_ItemShipped)

    # Act
    await bound(1, 10)

    # Assert
    mock_emit.assert_called_once_with(_ItemShipped(1, 10))


async def test_async_publish_bound_event_calls_emit_publish_with_keyword_args(
    async_publisher: AsyncPublisher[None], mocker: MockerFixture
):
    """
    BoundEvent called with keyword args should construct the event and call emit_publish.

    Given: A BoundEvent returned by an AsyncPublisher's publish
    When: The BoundEvent is called with keyword arguments and the result is awaited
    Then: emit_publish should be called with the constructed event
    """
    # Arrange
    mock_emit = mocker.patch.object(async_publisher, "emit_publish", new=AsyncMock())
    bound = async_publisher.publish(_ItemShipped)

    # Act
    await bound(item_id=2, quantity=5)

    # Assert
    mock_emit.assert_called_once_with(_ItemShipped(2, 5))


async def test_async_publish_bound_event_returns_emit_publish_result(
    async_publisher: AsyncPublisher[None],
):
    """
    Resolved return value from emit_publish should be returned to the caller.

    Given: A BoundEvent returned by an AsyncPublisher whose emit_publish resolves to a known value
    When: The BoundEvent is called and the result is awaited
    Then: The return value should match what emit_publish resolved to
    """
    # Arrange
    mock_emit = AsyncMock(return_value="dispatched")
    async_publisher.emit_publish = mock_emit
    bound = async_publisher.publish(_ItemShipped)

    # Act
    result = await bound(1, 10)

    # Assert
    assert result == "dispatched"
