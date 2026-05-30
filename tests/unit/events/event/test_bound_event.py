"""
Unit tests for the BoundEvent class.

This test suite verifies the following behaviors:
- The schema, emitter, and meta are stored on initialization.
- Calling the bound event constructs the event with positional arguments.
- Calling the bound event constructs the event with keyword arguments.
- The return value from the emitter is returned to the caller.
"""

from typing import Any
from unittest.mock import create_autospec

import pytest
from pytest_mock import MockerFixture

from stratae.events import EventMeta
from stratae.events.channel import Channel
from stratae.events.event import BoundEvent, EventSchema


def _emitter_spec(channel: Channel, payload: EventSchema, *, meta: EventMeta | None) -> Any: ...


class _OrderCreated(EventSchema):
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _OrderCreated):
            return False
        return self.order_id == value.order_id and self.status == value.status


@pytest.fixture
def meta() -> EventMeta:
    """Return an EventMeta instance for use in BoundEvent construction."""
    return EventMeta()


def test_init_stores_schema_emitter_and_meta(meta: EventMeta):
    """
    Test that the schema, emitter, and meta are stored during initialization.

    Given: A schema, an emitter callable, and an EventMeta
    When: A BoundEvent is created
    Then: The schema, emitter, and meta attributes should reference the supplied objects
    """
    # Arrange
    emitter = create_autospec(_emitter_spec)
    channel = Channel("test")

    # Act
    bound: BoundEvent[[int, str], EventMeta, Any] = BoundEvent(
        channel, _OrderCreated, emitter, meta=meta
    )

    # Assert
    assert bound.schema is _OrderCreated
    assert bound.emitter is emitter
    assert bound.meta is meta


def test_call_passes_positional_args_to_schema(meta: EventMeta, mocker: MockerFixture):
    """
    Test that positional arguments are forwarded to the schema constructor.

    Given: A BoundEvent wrapping a schema that accepts positional arguments
    When: The BoundEvent is called with positional arguments
    Then: The schema constructor should be called with those values as positional args
    """
    # Arrange
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = create_autospec(_emitter_spec)
    channel = Channel("test")
    bound: BoundEvent[[int, str], EventMeta, Any] = BoundEvent(
        channel, _OrderCreated, emitter, meta=meta
    )

    # Act
    bound(1, "pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(channel, _OrderCreated(1, "pending"), meta=meta)


def test_call_passes_keyword_args_to_schema(meta: EventMeta, mocker: MockerFixture):
    """
    Test that keyword arguments are forwarded to the schema constructor.

    Given: A BoundEvent wrapping a schema that accepts keyword arguments
    When: The BoundEvent is called with keyword arguments
    Then: The schema constructor should be called with those values as keyword args
    """
    # Arrange
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = create_autospec(_emitter_spec)
    channel = Channel("test")
    bound = BoundEvent(channel, _OrderCreated, emitter, meta=meta)

    # Act
    bound(order_id=2, status="complete")

    # Assert
    spy.assert_called_once_with(mocker.ANY, order_id=2, status="complete")
    emitter.assert_called_once_with(channel, _OrderCreated(2, "complete"), meta=meta)


def test_call_passes_mixed_args_to_schema(meta: EventMeta, mocker: MockerFixture):
    """
    Test that a mix of positional and keyword arguments are forwarded to the schema constructor.

    Given: A BoundEvent wrapping a schema that accepts positional and keyword arguments
    When: The BoundEvent is called with one positional and one keyword argument
    Then: The schema constructor should be called with args in the same positional and keyword form
    """
    # Arrange
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = create_autospec(_emitter_spec)
    channel = Channel("test")
    bound = BoundEvent(channel, _OrderCreated, emitter, meta=meta)

    # Act
    bound(1, status="pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, status="pending")
    emitter.assert_called_once_with(channel, _OrderCreated(1, "pending"), meta=meta)


def test_call_returns_emitter_result(meta: EventMeta):
    """
    Test that the return value from the emitter is returned to the caller.

    Given: A BoundEvent whose emitter returns a known value
    When: The BoundEvent is called
    Then: The return value should match the emitter's return value
    """
    # Arrange
    emitter = create_autospec(_emitter_spec, return_value="dispatched")
    channel = Channel("test")
    bound: BoundEvent[[int, str], EventMeta, str] = BoundEvent(
        channel, _OrderCreated, emitter, meta=meta
    )

    # Act
    result = bound(1, "pending")

    # Assert
    assert result == "dispatched"
