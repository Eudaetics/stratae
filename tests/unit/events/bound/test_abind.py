"""
Unit tests for abind.

This test suite verifies the following behaviors:

abind — with a factory:
- Returns an AsyncFactoryBoundEvent.
- The AsyncFactoryBoundEvent stores the provided event, factory, and config.
- Calling the AsyncFactoryBoundEvent constructs the schema and awaits the emitter.
- Calling the AsyncFactoryBoundEvent returns the emitter's result.
- An async factory is awaited before its result is forwarded to the emitter.

abind — without a factory:
- Returns an AsyncBoundEvent.
- The AsyncBoundEvent stores the provided event and config.
- Calling the AsyncBoundEvent forwards an already-built payload to the emitter.
- Calling the AsyncBoundEvent returns the emitter's result.
"""

import asyncio
from typing import Any
from unittest.mock import Mock, create_autospec

from stratae.events import AsyncBoundEvent, AsyncFactoryBoundEvent, Event, PubSub, abind


async def _async_emit(
    payload: Any, event: Event[Any, Any], config: Any, serializer: Any = None
): ...


class _OrderCreated:
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderCreated):
            return False
        return self.order_id == other.order_id and self.status == other.status


# region: Abind With Factory


def test_abind_with_factory_returns_async_factory_bound_event() -> None:
    """
    Abind with a factory returns an AsyncFactoryBoundEvent.

    Given: An async emitter, an Event, a factory, and a config
    When: abind is called with all four
    Then: The result should be an AsyncFactoryBoundEvent instance
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)

    # Act
    result = abind(emitter, ev, factory=_OrderCreated, config=None)

    # Assert
    assert isinstance(result, AsyncFactoryBoundEvent)


def test_abind_with_factory_stores_config() -> None:
    """
    Abind stores the provided config on the returned AsyncFactoryBoundEvent.

    Given: A distinct config object
    When: abind is called with a factory and that config
    Then: The AsyncFactoryBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)
    config = object()

    # Act
    result = abind(emitter, ev, factory=_OrderCreated, config=config)

    # Assert
    assert result.config is config


async def test_abind_with_factory_calling_constructs_schema_and_awaits_emitter() -> None:
    """
    Calling an AsyncFactoryBoundEvent produced by abind constructs the schema and awaits emitter.

    Given: An AsyncFactoryBoundEvent produced by abind
    When: The AsyncFactoryBoundEvent is called with arguments
    Then: The emitter should receive the constructed payload, the Event, and the config
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)
    bound = abind(emitter, ev, factory=_OrderCreated, config=None)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(_OrderCreated(1, "pending"), ev, None, serializer=None)


async def test_abind_with_factory_returns_emitter_result() -> None:
    """
    Calling an AsyncFactoryBoundEvent produced by abind returns the emitter's result.

    Given: An AsyncFactoryBoundEvent whose emitter returns the constructed payload
    When: The AsyncFactoryBoundEvent is called
    Then: The return value should match the constructed payload
    """
    # Arrange
    emitter = create_autospec(_async_emit)

    def _return(
        payload: object, event: object, config: object, serializer: object = None
    ) -> object:
        return payload

    emitter.side_effect = _return
    ev = Event(_OrderCreated, PubSub)
    bound = abind(emitter, ev, factory=_OrderCreated, config=None)

    # Act
    result = await bound(1, "pending")

    # Assert
    assert result == _OrderCreated(1, "pending")


def test_abind_with_factory_stores_serializer() -> None:
    """
    Abind stores the provided serializer on the returned AsyncFactoryBoundEvent.

    Given: A serializer callable
    When: abind is called with a factory and that serializer
    Then: The AsyncFactoryBoundEvent's serializer should reference the same callable
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)
    serializer = Mock()

    # Act
    result = abind(emitter, ev, factory=_OrderCreated, config=None, serializer=serializer)

    # Assert
    assert result.serializer is serializer


async def test_abind_with_factory_forwards_serializer_to_emitter() -> None:
    """
    Calling an AsyncFactoryBoundEvent produced by abind forwards the serializer to the emitter.

    Given: An AsyncFactoryBoundEvent produced by abind with a serializer
    When: The AsyncFactoryBoundEvent is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)
    serializer = Mock()
    bound = abind(emitter, ev, factory=_OrderCreated, config=None, serializer=serializer)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(_OrderCreated(1, "pending"), ev, None, serializer=serializer)


async def test_abind_with_factory_awaits_async_factory_then_awaits_emitter() -> None:
    """
    Abind with an async factory awaits the factory before forwarding to the emitter.

    Given: An async factory
    When: The AsyncFactoryBoundEvent produced by abind is called
    Then: The emitter should receive the resolved payload, not the coroutine
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)

    async def _async_factory(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    bound = abind(emitter, ev, factory=_async_factory, config=None)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(_OrderCreated(1, "pending"), ev, None, serializer=None)


# endregion

# region: Abind Without Factory


def test_abind_without_factory_returns_async_bound_event() -> None:
    """
    Abind with no factory returns an AsyncBoundEvent.

    Given: An async emitter, an Event, and a config, but no factory
    When: abind is called with just those three
    Then: The result should be an AsyncBoundEvent instance
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)

    # Act
    result = abind(emitter, ev, config=None)

    # Assert
    assert isinstance(result, AsyncBoundEvent)


def test_abind_without_factory_stores_config() -> None:
    """
    Abind stores the provided config on the returned AsyncBoundEvent.

    Given: A distinct config object
    When: abind is called with no factory and that config
    Then: The AsyncBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)
    config = object()

    # Act
    result = abind(emitter, ev, config=config)

    # Assert
    assert result.config is config


async def test_abind_without_factory_calling_forwards_payload_to_emitter() -> None:
    """
    Calling an AsyncBoundEvent produced by abind forwards an already-built payload.

    Given: An AsyncBoundEvent produced by abind with no factory
    When: The AsyncBoundEvent is called with a ready-made payload
    Then: The emitter should receive that exact payload, the Event, and the config
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)
    bound = abind(emitter, ev, config=None)
    payload = _OrderCreated(1, "pending")

    # Act
    await bound(payload)

    # Assert
    emitter.assert_awaited_once_with(payload, ev, None, serializer=None)


async def test_abind_without_factory_returns_emitter_result() -> None:
    """
    Calling an AsyncBoundEvent produced by abind returns the emitter's result.

    Given: An AsyncBoundEvent whose emitter echoes the payload
    When: The AsyncBoundEvent is called
    Then: The return value should match the payload
    """
    # Arrange
    emitter = create_autospec(_async_emit)

    def _return(
        payload: object, event: object, config: object, serializer: object = None
    ) -> object:
        return payload

    emitter.side_effect = _return
    ev = Event(_OrderCreated, PubSub)
    bound = abind(emitter, ev, config=None)
    payload = _OrderCreated(1, "pending")

    # Act
    result = await bound(payload)

    # Assert
    assert result is payload


async def test_abind_without_factory_forwards_serializer_to_emitter() -> None:
    """
    Calling an AsyncBoundEvent produced by abind forwards the serializer to the emitter.

    Given: An AsyncBoundEvent produced by abind with a serializer
    When: The AsyncBoundEvent is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = Event(_OrderCreated, PubSub)
    serializer = Mock()
    bound = abind(emitter, ev, config=None, serializer=serializer)
    payload = _OrderCreated(1, "pending")

    # Act
    await bound(payload)

    # Assert
    emitter.assert_awaited_once_with(payload, ev, None, serializer=serializer)


# endregion
