"""
Unit tests for abind.

This test suite verifies the following behaviors:

abind — with a factory:
- Calling the returned callable constructs the payload via the factory and awaits the emitter.
- Calling the returned callable returns the emitter's result.
- The serializer is forwarded to the emitter.
- An async factory is awaited before its result is forwarded to the emitter.

abind — without a factory:
- Calling the returned callable forwards an already-built payload to the emitter.
- Calling the returned callable returns the emitter's result.
- The serializer is forwarded to the emitter.
"""

import asyncio
from typing import Any
from unittest.mock import Mock, create_autospec

from stratae.events import Event, PubSub, abind


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


async def test_abind_with_factory_calling_constructs_schema_and_awaits_emitter() -> None:
    """
    Calling a factory-bound callable produced by abind constructs the schema and awaits emitter.

    Given: A factory-bound callable produced by abind
    When: The callable is called with arguments
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
    Calling a factory-bound callable produced by abind returns the emitter's result.

    Given: A factory-bound callable whose emitter returns the constructed payload
    When: The callable is called
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


async def test_abind_with_factory_forwards_serializer_to_emitter() -> None:
    """
    Calling a factory-bound callable produced by abind forwards the serializer to the emitter.

    Given: A factory-bound callable produced by abind with a serializer
    When: The callable is called
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
    When: The callable produced by abind is called
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


async def test_abind_without_factory_calling_forwards_payload_to_emitter() -> None:
    """
    Calling a passthrough callable produced by abind forwards an already-built payload.

    Given: A passthrough callable produced by abind with no factory
    When: The callable is called with a ready-made payload
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
    Calling a passthrough callable produced by abind returns the emitter's result.

    Given: A passthrough callable whose emitter echoes the payload
    When: The callable is called
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
    Calling a passthrough callable produced by abind forwards the serializer to the emitter.

    Given: A passthrough callable produced by abind with a serializer
    When: The callable is called
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
