"""
Unit tests for the AsyncBasicPublisher mixin.

This test suite verifies the following behaviors:

AsyncBasicPublisher:
- AsyncBasicPublisher cannot be instantiated directly (abstract).
- publish returns an AsyncBoundEvent bound to emit_publish.
- The AsyncBoundEvent stores the correct schema, emitter, and None config.
- Awaiting the AsyncBoundEvent calls emit_publish with the payload and itself.
- Awaiting the AsyncBoundEvent with keyword args calls emit_publish with the payload and itself.
- The resolved return value from emit_publish is returned to the caller.
- publish stores None on the AsyncBoundEvent config.
- Each call to publish returns a distinct AsyncBoundEvent instance.
"""

from unittest.mock import AsyncMock

import pytest
from pytest_mock import MockerFixture

from stratae.events.event import AsyncBoundEvent, EventSchema
from stratae.events.mixins.publish import AsyncBasicPublisher


@pytest.fixture
def async_publisher() -> AsyncBasicPublisher[None]:
    """Return an AsyncBasicPublisher instance with abstract methods cleared for testing."""
    from unittest.mock import patch

    with patch.object(AsyncBasicPublisher, "__abstractmethods__", frozenset[str]()):
        return AsyncBasicPublisher()  # pyright: ignore[reportAbstractUsage]


def test_async_basic_publish_bus_is_abstract():
    """
    AsyncBasicPublisher should raise TypeError when instantiated directly.

    Given: The abstract AsyncBasicPublisher class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncBasicPublisher"):
        AsyncBasicPublisher()  # pyright: ignore[reportAbstractUsage]


def test_async_basic_publish_returns_async_bound_event(
    async_publisher: AsyncBasicPublisher[None],
):
    """
    Publish should return an AsyncBoundEvent instance.

    Given: An AsyncBasicPublisher instance with abstract methods cleared
    When: publish is used as a decorator
    Then: An AsyncBoundEvent instance should be returned
    """

    @async_publisher.publish
    class _ItemShipped(EventSchema): ...

    assert isinstance(_ItemShipped, AsyncBoundEvent)


def test_async_basic_publish_stores_schema_emitter_and_none_config(
    async_publisher: AsyncBasicPublisher[None],
):
    """
    AsyncBoundEvent returned by publish should store the schema, emit_publish, and None config.

    Given: An AsyncBasicPublisher instance with abstract methods cleared
    When: publish is used as a decorator
    Then: The AsyncBoundEvent should store an EventSchema subclass, emit_publish, and None as config
    """

    @async_publisher.publish
    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

    assert isinstance(_ItemShipped.schema, type)
    assert _ItemShipped.emitter == async_publisher.emit_publish
    assert _ItemShipped.config is None


async def test_async_basic_publish_bound_event_calls_emit_publish_with_positional_args(
    async_publisher: AsyncBasicPublisher[None], mocker: MockerFixture
):
    """
    AsyncBoundEvent called with positional args should construct the event and call emit_publish.

    Given: An AsyncBoundEvent returned by publish
    When: The AsyncBoundEvent is called and awaited with positional arguments
    Then: emit_publish should be called with the constructed payload and the AsyncBoundEvent itself
    """
    mock_emit = mocker.patch.object(async_publisher, "emit_publish", new=AsyncMock())

    @async_publisher.publish
    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, type(self)):
                return False
            return self.item_id == value.item_id and self.quantity == value.quantity

    await _ItemShipped(1, 10)

    mock_emit.assert_called_once_with(_ItemShipped.schema(1, 10), _ItemShipped)


async def test_async_basic_publish_bound_event_calls_emit_publish_with_keyword_args(
    async_publisher: AsyncBasicPublisher[None], mocker: MockerFixture
):
    """
    AsyncBoundEvent called with keyword args should construct the event and call emit_publish.

    Given: An AsyncBoundEvent returned by publish
    When: The AsyncBoundEvent is called and awaited with keyword arguments
    Then: emit_publish should be called with the constructed payload and the AsyncBoundEvent itself
    """
    mock_emit = mocker.patch.object(async_publisher, "emit_publish", new=AsyncMock())

    @async_publisher.publish
    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, type(self)):
                return False
            return self.item_id == value.item_id and self.quantity == value.quantity

    await _ItemShipped(item_id=2, quantity=5)

    mock_emit.assert_called_once_with(_ItemShipped.schema(item_id=2, quantity=5), _ItemShipped)


async def test_async_basic_publish_bound_event_returns_emit_publish_result(
    async_publisher: AsyncBasicPublisher[None], mocker: MockerFixture
):
    """
    The return value from emit_publish should propagate out of the AsyncBoundEvent call.

    Given: An AsyncBoundEvent created via the decorator
    When: The AsyncBoundEvent is called and awaited
    Then: The return value should match what emit_publish resolved to
    """
    mock_emit = mocker.patch.object(
        async_publisher, "emit_publish", new=AsyncMock(return_value="dispatched")
    )

    @async_publisher.publish
    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

    result = await _ItemShipped(1, 10)

    assert result == mock_emit.return_value


def test_async_basic_publish_returns_distinct_bound_events(
    async_publisher: AsyncBasicPublisher[None],
):
    """
    Each call to publish should return a distinct AsyncBoundEvent instance.

    Given: An AsyncBasicPublisher instance with abstract methods cleared
    When: publish is called twice with the same schema
    Then: The two AsyncBoundEvents should be different objects
    """

    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

    first = async_publisher.publish(_ItemShipped)
    second = async_publisher.publish(_ItemShipped)

    assert first is not second
    assert first.schema is second.schema
