"""
Unit tests for the AsyncBoundEvent class.

This test suite verifies the following behaviors:
- The schema, emitter, and meta are stored on initialization.
- Calling the bound event constructs the event with positional arguments.
- Calling the bound event constructs the event with keyword arguments.
- The return value from the async emitter is returned to the caller.
"""

from typing import Any
from unittest.mock import AsyncMock

import pytest
from pytest_mock import MockerFixture

from stratae.events import EventMeta
from stratae.events.event import AsyncBoundEvent, EventSchema


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
    emitter = AsyncMock()

    # Act
    bound: AsyncBoundEvent[[int, int], EventMeta, Any] = AsyncBoundEvent(
        _PaymentReceived, emitter, meta
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
    emitter = AsyncMock()
    bound: AsyncBoundEvent[[int, int], EventMeta, Any] = AsyncBoundEvent(
        _PaymentReceived, emitter, meta
    )

    # Act
    await bound(42, 100)

    # Assert
    spy.assert_called_once_with(mocker.ANY, 42, 100)
    emitter.assert_called_once_with(meta, _PaymentReceived(42, 100))


async def test_call_passes_keyword_args_to_schema(meta: EventMeta, mocker: MockerFixture):
    """
    Test that keyword arguments are forwarded to the schema constructor.

    Given: An AsyncBoundEvent wrapping a schema that accepts keyword arguments
    When: The AsyncBoundEvent is called and awaited with keyword arguments
    Then: The schema constructor should be called with those values as keyword args
    """
    # Arrange
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = AsyncMock()
    bound = AsyncBoundEvent(_PaymentReceived, emitter, meta)

    # Act
    await bound(payment_id=7, amount=50)

    # Assert
    spy.assert_called_once_with(mocker.ANY, payment_id=7, amount=50)
    emitter.assert_called_once_with(meta, _PaymentReceived(7, 50))


async def test_call_passes_mixed_args_to_schema(meta: EventMeta, mocker: MockerFixture):
    """
    Test that a mix of positional and keyword arguments are forwarded to the schema constructor.

    Given: An AsyncBoundEvent wrapping a schema that accepts positional and keyword arguments
    When: The AsyncBoundEvent is called and awaited with one positional and one keyword argument
    Then: The schema constructor should be called with args in the same positional and keyword form
    """
    # Arrange
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = AsyncMock()
    bound = AsyncBoundEvent(_PaymentReceived, emitter, meta)

    # Act
    await bound(42, amount=100)

    # Assert
    spy.assert_called_once_with(mocker.ANY, 42, amount=100)
    emitter.assert_called_once_with(meta, _PaymentReceived(42, 100))


async def test_call_returns_emitter_result(meta: EventMeta):
    """
    Test that the resolved value from the async emitter is returned to the caller.

    Given: An AsyncBoundEvent whose emitter resolves to a known value
    When: The AsyncBoundEvent is called and awaited
    Then: The return value should match the emitter's resolved value
    """
    # Arrange
    emitter = AsyncMock(return_value="dispatched")
    bound: AsyncBoundEvent[[int, int], EventMeta, str] = AsyncBoundEvent(
        _PaymentReceived, emitter, meta
    )

    # Act
    result = await bound(42, 100)

    # Assert
    assert result == "dispatched"
