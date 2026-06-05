"""
Unit tests for the AsyncRequester mixin.

This test suite verifies the following behaviors:

AsyncRequester:
- AsyncRequester cannot be instantiated directly (abstract).
- request returns an AsyncBoundEvent bound to emit_request.
- The AsyncBoundEvent stores the correct schema, emitter, and config.
- Awaiting the AsyncBoundEvent calls emit_request with the payload and itself.
- Awaiting the AsyncBoundEvent with keyword args calls emit_request with the payload and itself.
- The resolved return value from emit_request is returned to the caller.
- request without config stores None on the AsyncBoundEvent.
"""

from typing import Any
from unittest.mock import AsyncMock

import pytest
from pytest_mock import MockerFixture

from stratae.events.event import AsyncBoundEvent, EventSchema
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
def async_requester() -> AsyncRequester[Any, Any]:
    """Return an AsyncRequester instance with abstract methods cleared for testing."""
    from unittest.mock import patch

    with patch.object(AsyncRequester, "__abstractmethods__", frozenset[str]()):
        return AsyncRequester()  # pyright: ignore[reportAbstractUsage]


def test_async_request_is_abstract():
    """
    AsyncRequester should raise TypeError when instantiated directly.

    Given: The abstract AsyncRequester class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncRequester"):
        AsyncRequester()  # pyright: ignore[reportAbstractUsage]


def test_async_request_returns_async_bound_event(async_requester: AsyncRequester[Any, Any]):
    """
    Request should return an AsyncBoundEvent instance.

    Given: An AsyncRequester instance with abstract methods cleared
    When: request is called with a schema and config
    Then: An AsyncBoundEvent instance should be returned
    """
    assert isinstance(async_requester.request(_GetPrice, config=object()), AsyncBoundEvent)


def test_async_request_stores_schema_emitter_and_config(
    async_requester: AsyncRequester[Any, Any],
):
    """
    AsyncBoundEvent returned by request should store the schema, emit_request, and config.

    Given: An AsyncRequester instance with abstract methods cleared
    When: request is called with a schema and config
    Then: The AsyncBoundEvent should store that schema, emit_request, and config
    """
    config = object()

    bound = async_requester.request(_GetPrice, config=config)

    assert bound.schema is _GetPrice
    assert bound.emitter == async_requester.emit_request
    assert bound.config is config


async def test_async_request_bound_event_calls_emit_request_with_positional_args(
    async_requester: AsyncRequester[Any, Any], mocker: MockerFixture
):
    """
    AsyncBoundEvent called with positional args should construct the event and call emit_request.

    Given: An AsyncBoundEvent returned by request
    When: The AsyncBoundEvent is called and awaited with positional arguments
    Then: emit_request should be called with the constructed payload and the AsyncBoundEvent itself
    """
    mock_emit = mocker.patch.object(async_requester, "emit_request", new=AsyncMock())
    bound = async_requester.request(_GetPrice)

    await bound(1, "USD")

    mock_emit.assert_called_once_with(_GetPrice(1, "USD"), bound)


async def test_async_request_bound_event_calls_emit_request_with_keyword_args(
    async_requester: AsyncRequester[Any, Any], mocker: MockerFixture
):
    """
    AsyncBoundEvent called with keyword args should construct the event and call emit_request.

    Given: An AsyncBoundEvent returned by request
    When: The AsyncBoundEvent is called and awaited with keyword arguments
    Then: emit_request should be called with the constructed payload and the AsyncBoundEvent itself
    """
    mock_emit = mocker.patch.object(async_requester, "emit_request", new=AsyncMock())
    bound = async_requester.request(_GetPrice)

    await bound(item_id=2, currency="EUR")

    mock_emit.assert_called_once_with(_GetPrice(2, "EUR"), bound)


async def test_async_request_bound_event_returns_emit_request_result(
    async_requester: AsyncRequester[Any, Any],
):
    """
    Resolved return value from emit_request should be returned to the caller.

    Given: An AsyncBoundEvent returned by request whose emit_request resolves to a known value
    When: The AsyncBoundEvent is called and awaited
    Then: The return value should match what emit_request resolved to
    """
    mock_emit = AsyncMock(return_value="9.99")
    async_requester.emit_request = mock_emit  # pyright: ignore[reportAttributeAccessIssue]
    bound = async_requester.request(_GetPrice)

    result = await bound(1, "USD")

    assert result == "9.99"


def test_async_request_without_config_stores_none(async_requester: AsyncRequester[Any, Any]):
    """
    Request called without config should return an AsyncBoundEvent with config set to None.

    Given: An AsyncRequester instance with abstract methods cleared
    When: request is called with only a schema
    Then: An AsyncBoundEvent should be returned with config set to None
    """
    bound = async_requester.request(_GetPrice)

    assert isinstance(bound, AsyncBoundEvent)
    assert bound.config is None
