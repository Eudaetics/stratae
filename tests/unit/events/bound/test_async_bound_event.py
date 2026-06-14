"""
Unit tests for the AsyncBoundEvent class.

This test suite verifies the following behaviors:
- The schema, emitter, and config are stored on initialization.
- Awaiting the bound event constructs the event with positional arguments.
- Awaiting the bound event constructs the event with keyword arguments.
- Awaiting the bound event with mixed positional and keyword arguments forwards them correctly.
- The resolved value from the async emitter is returned to the caller.
- An async factory is awaited before its result is forwarded to the emitter.
"""

import asyncio
from unittest.mock import AsyncMock

from pytest_mock import MockerFixture

from stratae.events.bound import AsyncBoundEvent
from stratae.events.event import Payload


class _PaymentReceived(Payload):
    def __init__(self, payment_id: int, amount: int) -> None:
        self.payment_id = payment_id
        self.amount = amount

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, _PaymentReceived):
            return False
        return self.payment_id == value.payment_id and self.amount == value.amount


def test_init_stores_schema_emitter_and_config():
    """
    Test that the schema, emitter, and config are stored during initialization.

    Given: A schema, an async emitter callable, and a config object
    When: An AsyncBoundEvent is created
    Then: The schema, emitter, and config attributes should reference the supplied objects
    """
    emitter = AsyncMock()
    config = object()

    bound = AsyncBoundEvent(emitter, _PaymentReceived, config=config)

    assert bound.factory is _PaymentReceived
    assert bound.emitter is emitter
    assert bound.config is config


async def test_call_passes_positional_args_to_schema(mocker: MockerFixture):
    """
    Test that positional arguments are forwarded to the schema constructor.

    Given: An AsyncBoundEvent wrapping a schema that accepts positional arguments
    When: The AsyncBoundEvent is called and awaited with positional arguments
    Then: The schema constructor should be called with those values and the emitter
          should receive the constructed payload and the AsyncBoundEvent itself
    """
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = AsyncMock()
    bound = AsyncBoundEvent(emitter, _PaymentReceived, config=None)

    await bound(42, 100)

    spy.assert_called_once_with(mocker.ANY, 42, 100)
    emitter.assert_called_once_with(_PaymentReceived(42, 100), bound)


async def test_call_passes_keyword_args_to_schema(mocker: MockerFixture):
    """
    Test that keyword arguments are forwarded to the schema constructor.

    Given: An AsyncBoundEvent wrapping a schema that accepts keyword arguments
    When: The AsyncBoundEvent is called and awaited with keyword arguments
    Then: The schema constructor should be called with those values and the emitter
          should receive the constructed payload and the AsyncBoundEvent itself
    """
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = AsyncMock()
    bound = AsyncBoundEvent(emitter, _PaymentReceived, config=None)

    await bound(payment_id=7, amount=50)

    spy.assert_called_once_with(mocker.ANY, payment_id=7, amount=50)
    emitter.assert_called_once_with(_PaymentReceived(7, 50), bound)


async def test_call_passes_mixed_args_to_schema(mocker: MockerFixture):
    """
    Test that a mix of positional and keyword arguments are forwarded to the schema constructor.

    Given: An AsyncBoundEvent wrapping a schema that accepts positional and keyword arguments
    When: The AsyncBoundEvent is called and awaited with one positional and one keyword argument
    Then: The schema constructor should be called with args in the same form and the emitter
          should receive the constructed payload and the AsyncBoundEvent itself
    """
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = AsyncMock()
    bound = AsyncBoundEvent(emitter, _PaymentReceived, config=None)

    await bound(42, amount=100)

    spy.assert_called_once_with(mocker.ANY, 42, amount=100)
    emitter.assert_called_once_with(_PaymentReceived(42, 100), bound)


async def test_call_returns_emitter_result():
    """
    Test that the resolved value from the async emitter is returned to the caller.

    Given: An AsyncBoundEvent whose emitter resolves to a known value
    When: The AsyncBoundEvent is called and awaited
    Then: The return value should match the emitter's resolved value
    """
    emitter = AsyncMock(return_value="dispatched")
    bound = AsyncBoundEvent(emitter, _PaymentReceived, config=None)

    result = await bound(42, 100)

    assert result == "dispatched"


async def test_call_awaits_async_factory_before_passing_to_emitter() -> None:
    """
    Test that an async factory is awaited and its resolved value forwarded to the emitter.

    Given: An AsyncBoundEvent whose factory is a coroutine function
    When: The AsyncBoundEvent is called and awaited
    Then: The emitter should receive the resolved payload, not the coroutine
    """

    # Arrange
    async def _async_factory(payment_id: int, amount: int) -> _PaymentReceived:
        await asyncio.sleep(0)
        return _PaymentReceived(payment_id, amount)

    emitter = AsyncMock()
    bound = AsyncBoundEvent(emitter, _async_factory, config=None)

    # Act
    await bound(42, 100)

    # Assert
    emitter.assert_awaited_once_with(_PaymentReceived(42, 100), bound)
