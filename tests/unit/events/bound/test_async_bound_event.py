"""
Unit tests for the AsyncBoundEvent class (the no-factory passthrough binding).

This test suite verifies the following behaviors:
- The Event, emitter, and config are stored on initialization.
- Awaiting the bound event forwards an already-built payload to the emitter.
- The resolved value from the async emitter is returned to the caller.
"""

from typing import Any
from unittest.mock import Mock, create_autospec

import pytest

from stratae.events import AsyncBoundEvent, Event, PubSub


async def _async_emit(
    payload: Any, event: Event[Any, Any], config: Any, serializer: Any = None
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
def payment_received() -> Event[_PaymentReceived, PubSub]:
    """Return a fresh Event for ``_PaymentReceived``."""
    return Event(_PaymentReceived, PubSub)


def test_init_stores_event_emitter_and_config(
    payment_received: Event[_PaymentReceived, PubSub],
):
    """
    Test that the Event, emitter, and config are stored during initialization.

    Given: An Event, an async emitter callable, and a config object
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
    payment_received: Event[_PaymentReceived, PubSub],
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
    payment_received: Event[_PaymentReceived, PubSub],
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


async def test_call_forwards_payload_to_emitter(
    payment_received: Event[_PaymentReceived, PubSub],
):
    """
    Test that awaiting the bound event forwards the payload unchanged.

    Given: An AsyncBoundEvent with no factory
    When: The AsyncBoundEvent is called and awaited with an already-built payload
    Then: The emitter should receive that exact payload, the Event, and the config
    """
    emitter = create_autospec(_async_emit)
    bound = AsyncBoundEvent(emitter, payment_received, config=None)
    payload = _PaymentReceived(42, 100)

    await bound(payload)

    emitter.assert_awaited_once_with(payload, payment_received, None, serializer=None)


async def test_call_returns_emitter_result(
    payment_received: Event[_PaymentReceived, PubSub],
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

    result = await bound(_PaymentReceived(42, 100))

    assert result == "dispatched"


async def test_call_forwards_serializer_to_emitter(
    payment_received: Event[_PaymentReceived, PubSub],
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
    payload = _PaymentReceived(42, 100)

    await bound(payload)

    emitter.assert_awaited_once_with(payload, payment_received, None, serializer=serializer)
