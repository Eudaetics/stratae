"""
Unit tests for the AsyncRequester mixin.

This test suite verifies the following behaviors:

AsyncRequester:
- request returns an AsyncBoundEvent bound to emit_request.
- The AsyncBoundEvent stores the correct channel, schema, emitter, and meta.
- AsyncRequester cannot be instantiated directly (abstract).
- Awaiting the result of calling the AsyncBoundEvent calls emit_request with meta and event.
- Awaiting the result of calling the AsyncBoundEvent with no meta calls emit_request with None.
- The resolved return value from emit_request is returned to the caller.
"""

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from pytest_mock import MockerFixture

from stratae.events.channel import Channel
from stratae.events.event import AsyncBoundEvent, EventMeta, EventSchema
from stratae.events.mixins.request import AsyncRequester


class _GetPrice(EventSchema):
    def __init__(self, item_id: int, currency: str) -> None:
        self.item_id = item_id
        self.currency = currency

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _GetPrice):
            return False
        return self.item_id == value.item_id and self.currency == value.currency


@pytest.fixture
def async_requester() -> AsyncRequester[EventMeta, Any]:
    """Return an AsyncRequester instance with abstract methods cleared for testing."""
    with patch.object(AsyncRequester, "__abstractmethods__", frozenset[str]()):
        return AsyncRequester()  # pyright: ignore[reportAbstractUsage]


@pytest.fixture
def none_async_requester() -> AsyncRequester[None, Any]:
    """Return an AsyncRequester[None, Any] instance for testing the no-meta path."""
    with patch.object(AsyncRequester, "__abstractmethods__", frozenset[str]()):
        return AsyncRequester()  # pyright: ignore[reportAbstractUsage]


@pytest.fixture
def meta() -> EventMeta:
    """Return an EventMeta instance for use in request calls."""
    return EventMeta()


def test_async_request_is_abstract():
    """
    AsyncRequester should raise TypeError when instantiated directly.

    Given: The abstract AsyncRequester class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncRequester"):
        AsyncRequester()  # pyright: ignore[reportAbstractUsage]


def test_async_request_returns_async_bound_event(
    async_requester: AsyncRequester[EventMeta, Any], meta: EventMeta
):
    """
    Request should return an AsyncBoundEvent instance.

    Given: An AsyncRequester instance with abstract methods cleared
    When: request is called with a channel, schema, and meta
    Then: An AsyncBoundEvent instance should be returned
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = async_requester.request(channel, _GetPrice, meta=meta)

    # Assert
    assert isinstance(bound, AsyncBoundEvent)


def test_async_request_bound_event_stores_channel_schema_emitter_and_meta(
    async_requester: AsyncRequester[EventMeta, Any], meta: EventMeta
):
    """
    AsyncBoundEvent returned by request should store the channel, schema, emit_request, and meta.

    Given: An AsyncRequester instance with abstract methods cleared
    When: request is called with a channel, schema, and meta
    Then: The AsyncBoundEvent should store that channel, schema, emit_request, and meta
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = async_requester.request(channel, _GetPrice, meta=meta)

    # Assert
    assert bound.channel is channel
    assert bound.schema is _GetPrice
    assert bound.emitter == async_requester.emit_request
    assert bound.meta is meta


async def test_async_request_bound_event_calls_emit_request_with_positional_args(
    async_requester: AsyncRequester[EventMeta, Any], meta: EventMeta, mocker: MockerFixture
):
    """
    AsyncBoundEvent called with positional args should construct the event and call emit_request.

    Given: An AsyncBoundEvent returned by an AsyncRequester's request
    When: The AsyncBoundEvent is called with positional arguments and the result is awaited
    Then: emit_request should be called with the channel, constructed event, and meta
    """
    # Arrange
    mock_emit = mocker.patch.object(async_requester, "emit_request", new=AsyncMock())
    channel = Channel("test")
    bound = async_requester.request(channel, _GetPrice, meta=meta)

    # Act
    await bound(1, "USD")

    # Assert
    mock_emit.assert_called_once_with(channel, _GetPrice(1, "USD"), meta=meta)


async def test_async_request_bound_event_calls_emit_request_with_keyword_args(
    async_requester: AsyncRequester[EventMeta, Any], meta: EventMeta, mocker: MockerFixture
):
    """
    AsyncBoundEvent called with keyword args should construct the event and call emit_request.

    Given: An AsyncBoundEvent returned by an AsyncRequester's request
    When: The AsyncBoundEvent is called with keyword arguments and the result is awaited
    Then: emit_request should be called with the channel, constructed event, and meta
    """
    # Arrange
    mock_emit = mocker.patch.object(async_requester, "emit_request", new=AsyncMock())
    channel = Channel("test")
    bound = async_requester.request(channel, _GetPrice, meta=meta)

    # Act
    await bound(item_id=2, currency="EUR")

    # Assert
    mock_emit.assert_called_once_with(channel, _GetPrice(2, "EUR"), meta=meta)


async def test_async_request_bound_event_returns_emit_request_result(
    async_requester: AsyncRequester[EventMeta, Any], meta: EventMeta
):
    """
    Resolved return value from emit_request should be returned to the caller.

    Given: An AsyncBoundEvent returned by request whose emit_request resolves to a known value
    When: The AsyncBoundEvent is called and the result is awaited
    Then: The return value should match what emit_request resolved to
    """
    # Arrange
    mock_emit = AsyncMock(return_value="9.99")
    async_requester.emit_request = mock_emit
    channel = Channel("test")
    bound = async_requester.request(channel, _GetPrice, meta=meta)

    # Act
    result = await bound(1, "USD")

    # Assert
    assert result == "9.99"


def test_async_request_without_meta_returns_async_bound_event(
    none_async_requester: AsyncRequester[None, Any],
):
    """
    Request called without meta should return an AsyncBoundEvent with meta set to None.

    Given: An AsyncRequester[None, Any] instance with abstract methods cleared
    When: request is called with only a channel and schema
    Then: An AsyncBoundEvent should be returned with meta set to None
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = none_async_requester.request(channel, _GetPrice)

    # Assert
    assert isinstance(bound, AsyncBoundEvent)
    assert bound.meta is None


async def test_async_request_without_meta_calls_emit_request_with_none(
    none_async_requester: AsyncRequester[None, Any], mocker: MockerFixture
):
    """
    AsyncBoundEvent from a no-meta request should call emit_request with None as meta.

    Given: An AsyncBoundEvent returned by request with no meta
    When: The AsyncBoundEvent is called and awaited
    Then: emit_request should be called with None as meta
    """
    # Arrange
    mock_emit = mocker.patch.object(none_async_requester, "emit_request", new=AsyncMock())
    channel = Channel("test")
    bound = none_async_requester.request(channel, _GetPrice)

    # Act
    await bound(1, "USD")

    # Assert
    mock_emit.assert_called_once_with(channel, _GetPrice(1, "USD"), meta=None)
