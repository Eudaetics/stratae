"""
Unit tests for the Requester mixin.

This test suite verifies the following behaviors:

Requester:
- request returns a BoundEvent bound to emit_request.
- The BoundEvent stores the correct channel, schema, emitter, and meta.
- Requester cannot be instantiated directly (abstract).
- Calling the BoundEvent constructs the event and calls emit_request with meta and event.
- Calling the BoundEvent with no meta calls emit_request with None.
- The return value from emit_request is returned to the caller.
"""

from typing import Any
from unittest.mock import Mock, patch

import pytest
from pytest_mock import MockerFixture

from stratae.events.channel import Channel
from stratae.events.event import BoundEvent, EventMeta, EventSchema
from stratae.events.mixins.request import Requester


class _GetPrice(EventSchema):
    def __init__(self, item_id: int, currency: str) -> None:
        self.item_id = item_id
        self.currency = currency

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _GetPrice):
            return False
        return self.item_id == value.item_id and self.currency == value.currency


@pytest.fixture
def requester() -> Requester[EventMeta, Any]:
    """Return a Requester instance with abstract methods cleared for testing."""
    with patch.object(Requester, "__abstractmethods__", frozenset[str]()):
        return Requester()  # pyright: ignore[reportAbstractUsage]


@pytest.fixture
def none_requester() -> Requester[None, Any]:
    """Return a Requester[None, Any] instance for testing the no-meta path."""
    with patch.object(Requester, "__abstractmethods__", frozenset[str]()):
        return Requester()  # pyright: ignore[reportAbstractUsage]


@pytest.fixture
def meta() -> EventMeta:
    """Return an EventMeta instance for use in request calls."""
    return EventMeta()


def test_request_is_abstract():
    """
    Requester should raise TypeError when instantiated directly.

    Given: The abstract Requester class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class Requester"):
        Requester()  # pyright: ignore[reportAbstractUsage]


def test_request_returns_bound_event(requester: Requester[EventMeta, Any], meta: EventMeta):
    """
    Request should return a BoundEvent instance.

    Given: A Requester instance with abstract methods cleared
    When: request is called with a channel, schema, and meta
    Then: A BoundEvent instance should be returned
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = requester.request(channel, _GetPrice, meta=meta)

    # Assert
    assert isinstance(bound, BoundEvent)


def test_request_bound_event_stores_channel_schema_emitter_and_meta(
    requester: Requester[EventMeta, Any], meta: EventMeta
):
    """
    BoundEvent returned by request should store the channel, schema, emit_request, and meta.

    Given: A Requester instance with abstract methods cleared
    When: request is called with a channel, schema, and meta
    Then: The BoundEvent should store that channel, schema, emit_request, and meta
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = requester.request(channel, _GetPrice, meta=meta)

    # Assert
    assert bound.channel is channel
    assert bound.schema is _GetPrice
    assert bound.emitter == requester.emit_request
    assert bound.meta is meta


def test_request_bound_event_calls_emit_request_with_positional_args(
    requester: Requester[EventMeta, Any], meta: EventMeta, mocker: MockerFixture
):
    """
    BoundEvent called with positional args should construct the event and call emit_request.

    Given: A BoundEvent returned by request
    When: The BoundEvent is called with positional arguments
    Then: emit_request should be called with the channel, constructed event, and meta
    """
    # Arrange
    mock_emit = mocker.patch.object(requester, "emit_request", new=Mock())
    channel = Channel("test")
    bound = requester.request(channel, _GetPrice, meta=meta)

    # Act
    bound(1, "USD")

    # Assert
    mock_emit.assert_called_once_with(channel, _GetPrice(1, "USD"), meta=meta)


def test_request_bound_event_calls_emit_request_with_keyword_args(
    requester: Requester[EventMeta, Any], meta: EventMeta, mocker: MockerFixture
):
    """
    BoundEvent called with keyword args should construct the event and call emit_request.

    Given: A BoundEvent returned by request
    When: The BoundEvent is called with keyword arguments
    Then: emit_request should be called with the channel, constructed event, and meta
    """
    # Arrange
    mock_emit = mocker.patch.object(requester, "emit_request", new=Mock())
    channel = Channel("test")
    bound = requester.request(channel, _GetPrice, meta=meta)

    # Act
    bound(item_id=2, currency="EUR")

    # Assert
    mock_emit.assert_called_once_with(channel, _GetPrice(2, "EUR"), meta=meta)


def test_request_bound_event_returns_emit_request_result(
    requester: Requester[EventMeta, Any], meta: EventMeta
):
    """
    Return value from emit_request should be returned to the caller.

    Given: A BoundEvent returned by request whose emit_request returns a known value
    When: The BoundEvent is called
    Then: The return value should match what emit_request returned
    """
    # Arrange
    mock_emit = Mock(return_value="9.99")
    requester.emit_request = mock_emit
    channel = Channel("test")
    bound = requester.request(channel, _GetPrice, meta=meta)

    # Act
    result = bound(1, "USD")

    # Assert
    assert result == "9.99"


def test_request_without_meta_returns_bound_event(none_requester: Requester[None, Any]):
    """
    Request called without meta should return a BoundEvent with meta set to None.

    Given: A Requester[None, Any] instance with abstract methods cleared
    When: request is called with only a channel and schema
    Then: A BoundEvent should be returned with meta set to None
    """
    # Arrange
    channel = Channel("test")

    # Act
    bound = none_requester.request(channel, _GetPrice)

    # Assert
    assert isinstance(bound, BoundEvent)
    assert bound.meta is None


def test_request_without_meta_calls_emit_request_with_none(
    none_requester: Requester[None, Any], mocker: MockerFixture
):
    """
    BoundEvent from a no-meta request should call emit_request with None as meta.

    Given: A BoundEvent returned by request with no meta
    When: The BoundEvent is called
    Then: emit_request should be called with None as meta
    """
    # Arrange
    mock_emit = mocker.patch.object(none_requester, "emit_request", new=Mock())
    channel = Channel("test")
    bound = none_requester.request(channel, _GetPrice)

    # Act
    bound(1, "USD")

    # Assert
    mock_emit.assert_called_once_with(channel, _GetPrice(1, "USD"), meta=None)
