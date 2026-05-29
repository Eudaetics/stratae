"""
Unit tests for the AsyncBoundEvent class.

This test suite verifies the following behaviors:
- The schema, emitter, and meta are stored on initialization.
- Calling the bound event constructs the event with positional arguments.
- Calling the bound event constructs the event with keyword arguments.
- The return value from the async emitter is returned to the caller.
"""

from typing import Any
from unittest.mock import create_autospec

import pytest
from pytest_mock import MockerFixture

from stratae.events import EventMeta
from stratae.events.channel import Channel
from stratae.events.event import AsyncBoundEvent, EventSchema


async def _emitter_spec(
    channel: Channel, payload: EventSchema, *, meta: EventMeta | None
) -> Any: ...


class _PaymentReceived(EventSchema):
    def __init__(self, payment_id: int, amount: int) -> None:
        self.payment_id = payment_id
        self.amount = amount

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _PaymentReceived):
            return False
        return self.payment_id == value.payment_id and self.amount == value.amount


@pytest.fixture
def meta() -> EventMeta:
    """Return an EventMeta instance for use in AsyncBoundEvent construction."""
    return EventMeta()


def test_init_stores_schema_emitter_and_meta(meta: EventMeta):
    """
    Test that the schema, emitter, and meta are stored during initialization.

    Given: A schema, an async emitter callable, and an EventMeta
    When: An AsyncBoundEvent is created
    Then: The schema, emitter, and meta attributes should reference the supplied objects
    """
    # Arrange
    emitter = create_autospec(_emitter_spec)
    channel = Channel("test")

    # Act
    bound: AsyncBoundEvent[[int, int], EventMeta, Any] = AsyncBoundEvent(
        channel, _PaymentReceived, emitter, meta
    )

    # Assert
    assert bound.schema is _PaymentReceived
    assert bound.emitter is emitter
    assert bound.meta is meta


async def test_call_passes_positional_args_to_schema(meta: EventMeta, mocker: MockerFixture):
    """
    Test that positional arguments are forwarded to the schema constructor.

    Given: An AsyncBoundEvent wrapping a schema that accepts positional arguments
    When: The AsyncBoundEvent is called and awaited with positional arguments
    Then: The schema constructor should be called with those values as positional args
    """
    # Arrange
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = create_autospec(_emitter_spec)
    channel = Channel("test")
    bound: AsyncBoundEvent[[int, int], EventMeta, Any] = AsyncBoundEvent(
        channel, _PaymentReceived, emitter, meta
    )

    # Act
    await bound(42, 100)

    # Assert
    spy.assert_called_once_with(mocker.ANY, 42, 100)
    emitter.assert_called_once_with(channel, _PaymentReceived(42, 100), meta=meta)


async def test_call_passes_keyword_args_to_schema(meta: EventMeta, mocker: MockerFixture):
    """
    Test that keyword arguments are forwarded to the schema constructor.

    Given: An AsyncBoundEvent wrapping a schema that accepts keyword arguments
    When: The AsyncBoundEvent is called and awaited with keyword arguments
    Then: The schema constructor should be called with those values as keyword args
    """
    # Arrange
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = create_autospec(_emitter_spec)
    channel = Channel("test")
    bound = AsyncBoundEvent(channel, _PaymentReceived, emitter, meta)

    # Act
    await bound(payment_id=7, amount=50)

    # Assert
    spy.assert_called_once_with(mocker.ANY, payment_id=7, amount=50)
    emitter.assert_called_once_with(channel, _PaymentReceived(7, 50), meta=meta)


async def test_call_passes_mixed_args_to_schema(meta: EventMeta, mocker: MockerFixture):
    """
    Test that a mix of positional and keyword arguments are forwarded to the schema constructor.

    Given: An AsyncBoundEvent wrapping a schema that accepts positional and keyword arguments
    When: The AsyncBoundEvent is called and awaited with one positional and one keyword argument
    Then: The schema constructor should be called with args in the same positional and keyword form
    """
    # Arrange
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = create_autospec(_emitter_spec)
    channel = Channel("test")
    bound = AsyncBoundEvent(channel, _PaymentReceived, emitter, meta)

    # Act
    await bound(42, amount=100)

    # Assert
    spy.assert_called_once_with(mocker.ANY, 42, amount=100)
    emitter.assert_called_once_with(channel, _PaymentReceived(42, 100), meta=meta)


async def test_call_returns_emitter_result(meta: EventMeta):
    """
    Test that the resolved value from the async emitter is returned to the caller.

    Given: An AsyncBoundEvent whose emitter resolves to a known value
    When: The AsyncBoundEvent is called and awaited
    Then: The return value should match the emitter's resolved value
    """
    # Arrange
    emitter = create_autospec(_emitter_spec, return_value="dispatched")
    channel = Channel("test")
    bound: AsyncBoundEvent[[int, int], EventMeta, str] = AsyncBoundEvent(
        channel, _PaymentReceived, emitter, meta
    )

    # Act
    result = await bound(42, 100)

    # Assert
    assert result == "dispatched"
