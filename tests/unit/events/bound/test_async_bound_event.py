"""
Unit tests for the AsyncBoundEvent class.

This test suite verifies the following behaviors:
- The EventConfig, emitter, and config are stored on initialization.
- Awaiting the bound event constructs the payload with positional arguments.
- Awaiting the bound event constructs the payload with keyword arguments.
- Awaiting the bound event with mixed positional and keyword arguments forwards them correctly.
- The resolved value from the async emitter is returned to the caller.
- An async factory is awaited before its result is forwarded to the emitter.
"""

import asyncio
from typing import Any
from unittest.mock import Mock, create_autospec

import pytest
from pytest_mock import MockerFixture

from stratae.events import AsyncBoundEvent, EventConfig, PubSub


async def _async_emit(
    payload: Any, event: EventConfig[..., Any, Any], config: Any, serializer: Any = None
): ...


class _PaymentReceived:
    def __init__(self, payment_id: int, amount: int) -> None:
        self.payment_id = payment_id
        self.amount = amount

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, _PaymentReceived):
            return False
        return self.payment_id == value.payment_id and self.amount == value.amount


@pytest.fixture
def payment_received() -> EventConfig[..., _PaymentReceived, PubSub]:
    """Return a fresh EventConfig for ``_PaymentReceived``."""
    return EventConfig(_PaymentReceived, PubSub)


def test_init_stores_event_emitter_and_config(
    payment_received: EventConfig[..., _PaymentReceived, PubSub],
):
    """
    Test that the EventConfig, emitter, and config are stored during initialization.

    Given: An EventConfig, an async emitter callable, and a config object
    When: An AsyncBoundEvent is created
    Then: The event, emitter, and config attributes should reference the supplied objects
    """
    emitter = create_autospec(_async_emit)
    config = object()

    bound = AsyncBoundEvent(emitter, payment_received, config=config)

    assert bound.event is payment_received
    assert bound.emitter is emitter
    assert bound.config is config


def test_init_defaults_serializer_to_none(
    payment_received: EventConfig[..., _PaymentReceived, PubSub],
):
    """
    Test that serializer defaults to None when not supplied.

    Given: No serializer argument
    When: An AsyncBoundEvent is created
    Then: The serializer attribute should be None
    """
    emitter = create_autospec(_async_emit)

    bound = AsyncBoundEvent(emitter, payment_received, config=None)

    assert bound.serializer is None


def test_init_stores_serializer(
    payment_received: EventConfig[..., _PaymentReceived, PubSub],
):
    """
    Test that a supplied serializer is stored during initialization.

    Given: A serializer callable
    When: An AsyncBoundEvent is created with that serializer
    Then: The serializer attribute should reference the supplied callable
    """
    emitter = create_autospec(_async_emit)
    serializer = Mock()

    bound = AsyncBoundEvent(emitter, payment_received, config=None, serializer=serializer)

    assert bound.serializer is serializer


async def test_call_passes_positional_args_to_factory(
    payment_received: EventConfig[..., _PaymentReceived, PubSub],
    mocker: MockerFixture,
):
    """
    Test that positional arguments are forwarded to the factory.

    Given: An AsyncBoundEvent wrapping an EventConfig whose factory accepts positional arguments
    When: The AsyncBoundEvent is called and awaited with positional arguments
    Then: The factory should be called with those values and the emitter
          should receive the constructed payload, the EventConfig, and the config
    """
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = create_autospec(_async_emit)
    bound = AsyncBoundEvent(emitter, payment_received, config=None)

    await bound(42, 100)

    spy.assert_called_once_with(mocker.ANY, 42, 100)
    emitter.assert_called_once_with(
        _PaymentReceived(42, 100), payment_received, None, serializer=None
    )


async def test_call_passes_keyword_args_to_factory(
    payment_received: EventConfig[..., _PaymentReceived, PubSub],
    mocker: MockerFixture,
):
    """
    Test that keyword arguments are forwarded to the factory.

    Given: An AsyncBoundEvent wrapping an EventConfig whose factory accepts keyword arguments
    When: The AsyncBoundEvent is called and awaited with keyword arguments
    Then: The factory should be called with those values and the emitter
          should receive the constructed payload, the EventConfig, and the config
    """
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = create_autospec(_async_emit)
    bound = AsyncBoundEvent(emitter, payment_received, config=None)

    await bound(payment_id=7, amount=50)

    spy.assert_called_once_with(mocker.ANY, payment_id=7, amount=50)
    emitter.assert_called_once_with(
        _PaymentReceived(7, 50), payment_received, None, serializer=None
    )


async def test_call_passes_mixed_args_to_factory(
    payment_received: EventConfig[..., _PaymentReceived, PubSub],
    mocker: MockerFixture,
):
    """
    Test that a mix of positional and keyword arguments are forwarded to the factory.

    Given: An AsyncBoundEvent wrapping an EventConfig that accepts positional and keyword args
    When: The AsyncBoundEvent is called and awaited with one positional and one keyword argument
    Then: The factory should be called with args in the same form and the emitter
          should receive the constructed payload, the EventConfig, and the config
    """
    spy = mocker.spy(_PaymentReceived, "__init__")
    emitter = create_autospec(_async_emit)
    bound = AsyncBoundEvent(emitter, payment_received, config=None)

    await bound(42, amount=100)

    spy.assert_called_once_with(mocker.ANY, 42, amount=100)
    emitter.assert_called_once_with(
        _PaymentReceived(42, 100), payment_received, None, serializer=None
    )


async def test_call_returns_emitter_result(
    payment_received: EventConfig[..., _PaymentReceived, PubSub],
):
    """
    Test that the resolved value from the async emitter is returned to the caller.

    Given: An AsyncBoundEvent whose emitter resolves to a known value
    When: The AsyncBoundEvent is called and awaited
    Then: The return value should match the emitter's resolved value
    """
    emitter = create_autospec(_async_emit)

    def _return(
        payload: object, event: object, config: object, serializer: object = None
    ) -> object:
        return "dispatched"

    emitter.side_effect = _return
    bound = AsyncBoundEvent(emitter, payment_received, config=None)

    result = await bound(42, 100)

    assert result == "dispatched"


async def test_call_forwards_serializer_to_emitter(
    payment_received: EventConfig[..., _PaymentReceived, PubSub],
) -> None:
    """
    Test that the bound serializer is forwarded to the emitter when awaited.

    Given: An AsyncBoundEvent constructed with a serializer
    When: The AsyncBoundEvent is called and awaited
    Then: The emitter should receive that same serializer
    """
    emitter = create_autospec(_async_emit)
    serializer = Mock()
    bound = AsyncBoundEvent(emitter, payment_received, config=None, serializer=serializer)

    await bound(42, 100)

    emitter.assert_awaited_once_with(
        _PaymentReceived(42, 100), payment_received, None, serializer=serializer
    )


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

    event = EventConfig(_async_factory, PubSub, payload_type=_PaymentReceived)
    emitter = create_autospec(_async_emit)
    bound = AsyncBoundEvent(emitter, event, config=None)

    # Act
    await bound(42, 100)

    # Assert
    emitter.assert_awaited_once_with(_PaymentReceived(42, 100), event, None, serializer=None)
