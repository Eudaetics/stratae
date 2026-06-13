"""
Unit tests for the AsyncPublisher mixin.

This test suite verifies the following behaviors:

AsyncPublisher:
- AsyncPublisher cannot be instantiated directly (abstract).
- publish returns an AsyncBoundEvent bound to emit_publish.
- The AsyncBoundEvent stores the correct schema, emitter, and config.
- Awaiting the AsyncBoundEvent calls emit_publish with the payload and itself.
- Awaiting the AsyncBoundEvent with keyword args calls emit_publish with the payload and itself.
- The resolved return value from emit_publish is returned to the caller.
- Each call to publish returns a distinct AsyncBoundEvent instance.
"""

from unittest.mock import AsyncMock

import pytest
from pytest_mock import MockerFixture

from stratae.events.bound import AsyncBoundEvent
from stratae.events.event import Payload
from stratae.events.mixins.publish import AsyncPublisher


@pytest.fixture
def async_publisher() -> AsyncPublisher[str, None]:
    """Return an AsyncPublisher instance with abstract methods cleared for testing."""
    from unittest.mock import patch

    with patch.object(AsyncPublisher, "__abstractmethods__", frozenset[str]()):
        return AsyncPublisher[str, None]()  # pyright: ignore[reportAbstractUsage]


def test_async_publish_bus_is_abstract():
    """
    AsyncPublisher should raise TypeError when instantiated directly.

    Given: The abstract AsyncPublisher class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncPublisher"):
        AsyncPublisher()  # pyright: ignore[reportAbstractUsage]


def test_async_publish_returns_async_bound_event(async_publisher: AsyncPublisher[str, None]):
    """
    Publish should return an AsyncBoundEvent instance.

    Given: An AsyncPublisher instance with abstract methods cleared
    When: publish is used as a decorator factory with a config
    Then: An AsyncBoundEvent instance should be returned
    """

    @async_publisher.publish(config="orders")
    class _ItemShipped(Payload): ...

    assert isinstance(_ItemShipped, AsyncBoundEvent)


def test_async_publish_stores_schema_emitter_and_config(
    async_publisher: AsyncPublisher[str, None],
):
    """
    AsyncBoundEvent returned by publish should store the schema, emit_publish, and config.

    Given: An AsyncPublisher instance with abstract methods cleared
    When: publish is used as a decorator factory with a config
    Then: The AsyncBoundEvent should store a Payload subclass, emit_publish, and config
    """

    @async_publisher.publish(config="orders")
    class _ItemShipped(Payload):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

    assert isinstance(_ItemShipped.factory, type)
    assert _ItemShipped.emitter == async_publisher.emit_publish
    assert _ItemShipped.config == "orders"


async def test_async_publish_bound_event_calls_emit_publish_with_positional_args(
    async_publisher: AsyncPublisher[str, None], mocker: MockerFixture
):
    """
    AsyncBoundEvent called with positional args should construct the event and call emit_publish.

    Given: An AsyncBoundEvent returned by publish
    When: The AsyncBoundEvent is called and awaited with positional arguments
    Then: emit_publish should be called with the constructed payload and the AsyncBoundEvent itself
    """
    mock_emit = mocker.patch.object(async_publisher, "emit_publish", new=AsyncMock())

    @async_publisher.publish(config="orders")
    class _ItemShipped(Payload):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, type(self)):
                return False
            return self.item_id == value.item_id and self.quantity == value.quantity

    await _ItemShipped(1, 10)

    mock_emit.assert_called_once_with(_ItemShipped.factory(1, 10), _ItemShipped)


async def test_async_publish_bound_event_calls_emit_publish_with_keyword_args(
    async_publisher: AsyncPublisher[str, None], mocker: MockerFixture
):
    """
    AsyncBoundEvent called with keyword args should construct the event and call emit_publish.

    Given: An AsyncBoundEvent returned by publish
    When: The AsyncBoundEvent is called and awaited with keyword arguments
    Then: emit_publish should be called with the constructed payload and the AsyncBoundEvent itself
    """
    mock_emit = mocker.patch.object(async_publisher, "emit_publish", new=AsyncMock())

    @async_publisher.publish(config="orders")
    class _ItemShipped(Payload):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, type(self)):
                return False
            return self.item_id == value.item_id and self.quantity == value.quantity

    await _ItemShipped(item_id=2, quantity=5)

    mock_emit.assert_called_once_with(_ItemShipped.factory(item_id=2, quantity=5), _ItemShipped)


async def test_async_publish_bound_event_returns_emit_publish_result(
    async_publisher: AsyncPublisher[str, None], mocker: MockerFixture
):
    """
    The return value from emit_publish should propagate out of the AsyncBoundEvent call.

    Given: An AsyncBoundEvent created via the decorator factory
    When: The AsyncBoundEvent is called and awaited
    Then: The return value should match what emit_publish resolved to
    """
    mock_emit = mocker.patch.object(
        async_publisher, "emit_publish", new=AsyncMock(return_value="dispatched")
    )

    @async_publisher.publish(config="orders")
    class _ItemShipped(Payload):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

    result = await _ItemShipped(1, 10)

    assert result == mock_emit.return_value


def test_async_publish_returns_distinct_bound_events(async_publisher: AsyncPublisher[str, None]):
    """
    Each call to publish should return a distinct AsyncBoundEvent instance.

    Given: An AsyncPublisher instance with abstract methods cleared
    When: publish is called twice with the same schema
    Then: The two AsyncBoundEvents should be different objects
    """

    class _ItemShipped(Payload):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

    first = async_publisher.publish(_ItemShipped, config="orders")
    second = async_publisher.publish(_ItemShipped, config="orders")

    assert first is not second
