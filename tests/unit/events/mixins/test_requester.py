"""
Unit tests for the Requester mixin.

This test suite verifies the following behaviors:

Requester:
- Requester cannot be instantiated directly (abstract).
- request returns a BoundEvent bound to emit_request.
- The BoundEvent stores the correct schema, emitter, and config.
- Calling the BoundEvent constructs the event and calls emit_request with the payload and itself.
- Calling the BoundEvent with keyword args calls emit_request with the payload and itself.
- The return value from emit_request is returned to the caller.
- request without config stores None on the BoundEvent.
"""

from typing import Any
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stratae.events.event import BoundEvent, EventSchema
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
def requester() -> Requester[Any, Any]:
    """Return a Requester instance with abstract methods cleared for testing."""
    from unittest.mock import patch

    with patch.object(Requester, "__abstractmethods__", frozenset[str]()):
        return Requester()  # pyright: ignore[reportAbstractUsage]


def test_request_is_abstract():
    """
    Requester should raise TypeError when instantiated directly.

    Given: The abstract Requester class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class Requester"):
        Requester()  # pyright: ignore[reportAbstractUsage]


def test_request_returns_bound_event(requester: Requester[Any, Any]):
    """
    Request should return a BoundEvent instance.

    Given: A Requester instance with abstract methods cleared
    When: request is called with a schema and config
    Then: A BoundEvent instance should be returned
    """
    assert isinstance(requester.request(_GetPrice, config=object()), BoundEvent)


def test_request_stores_schema_emitter_and_config(requester: Requester[Any, Any]):
    """
    BoundEvent returned by request should store the schema, emit_request, and config.

    Given: A Requester instance with abstract methods cleared
    When: request is called with a schema and config
    Then: The BoundEvent should store that schema, emit_request, and config
    """
    config = object()

    bound = requester.request(_GetPrice, config=config)

    assert bound.schema is _GetPrice
    assert bound.emitter == requester.emit_request
    assert bound.config is config


def test_request_bound_event_calls_emit_request_with_positional_args(
    requester: Requester[Any, Any], mocker: MockerFixture
):
    """
    BoundEvent called with positional args should construct the event and call emit_request.

    Given: A BoundEvent returned by request
    When: The BoundEvent is called with positional arguments
    Then: emit_request should be called with the constructed payload and the BoundEvent itself
    """
    mock_emit = mocker.patch.object(requester, "emit_request", new=Mock())
    bound = requester.request(_GetPrice)

    bound(1, "USD")

    mock_emit.assert_called_once_with(_GetPrice(1, "USD"), bound)


def test_request_bound_event_calls_emit_request_with_keyword_args(
    requester: Requester[Any, Any], mocker: MockerFixture
):
    """
    BoundEvent called with keyword args should construct the event and call emit_request.

    Given: A BoundEvent returned by request
    When: The BoundEvent is called with keyword arguments
    Then: emit_request should be called with the constructed payload and the BoundEvent itself
    """
    mock_emit = mocker.patch.object(requester, "emit_request", new=Mock())
    bound = requester.request(_GetPrice)

    bound(item_id=2, currency="EUR")

    mock_emit.assert_called_once_with(_GetPrice(2, "EUR"), bound)


def test_request_bound_event_returns_emit_request_result(requester: Requester[Any, Any]):
    """
    Return value from emit_request should be returned to the caller.

    Given: A BoundEvent returned by request whose emit_request returns a known value
    When: The BoundEvent is called
    Then: The return value should match what emit_request returned
    """
    mock_emit = Mock(return_value="9.99")
    requester.emit_request = mock_emit  # pyright: ignore[reportAttributeAccessIssue]
    bound = requester.request(_GetPrice)

    result = bound(1, "USD")

    assert result == "9.99"


def test_request_without_config_stores_none(requester: Requester[Any, Any]):
    """
    Request called without config should return a BoundEvent with config set to None.

    Given: A Requester instance with abstract methods cleared
    When: request is called with only a schema
    Then: A BoundEvent should be returned with config set to None
    """
    bound = requester.request(_GetPrice)

    assert isinstance(bound, BoundEvent)
    assert bound.config is None
