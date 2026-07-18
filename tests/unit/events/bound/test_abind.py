"""
Unit tests for abind and abind_factory.

This test suite verifies the following behaviors:

abind direct form:
- Returns an AsyncBoundEvent.
- The AsyncBoundEvent uses the event's schema as its factory.
- The AsyncBoundEvent stores the provided config.
- Calling the AsyncBoundEvent constructs the schema and awaits the emitter.
- Calling the AsyncBoundEvent returns the emitter's result.
- An async factory is awaited before its result is forwarded to the emitter.

abind decorator form:
- Returns a callable when no event is provided.
- Applying the callable to an Event returns an AsyncBoundEvent.
- The returned AsyncBoundEvent uses the event's schema as its factory.
- The returned AsyncBoundEvent stores the provided config.
- An async factory is awaited before its result is forwarded to the emitter.
"""

import asyncio
from typing import Any
from unittest.mock import Mock, create_autospec

from stratae.events.bound import AsyncBoundEvent, abind
from stratae.events.event import EventConfig, PubSub


async def _async_emit(
    payload: Any, event: EventConfig[..., Any, Any], config: Any, serializer: Any = None
): ...


class _OrderCreated:
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderCreated):
            return False
        return self.order_id == other.order_id and self.status == other.status


# region: Abind Direct


def test_abind_direct_returns_async_bound_event() -> None:
    """
    Abind with an event returns an AsyncBoundEvent.

    Given: An async emitter, an Event, and a config
    When: abind is called with all three
    Then: The result should be an AsyncBoundEvent instance
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = abind(emitter, ev, config=None)

    # Assert
    assert isinstance(result, AsyncBoundEvent)


def test_abind_direct_uses_event_schema_as_factory() -> None:
    """
    Abind stores the event's schema class as the AsyncBoundEvent's factory.

    Given: An Event whose schema is _OrderCreated
    When: abind is called in direct form
    Then: The AsyncBoundEvent's factory should be _OrderCreated
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = abind(emitter, ev, config=None)

    # Assert
    assert result.event.factory is _OrderCreated


def test_abind_direct_stores_config() -> None:
    """
    Abind stores the provided config on the returned AsyncBoundEvent.

    Given: A distinct config object
    When: abind is called in direct form with that config
    Then: The AsyncBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)
    config = object()

    # Act
    result = abind(emitter, ev, config=config)

    # Assert
    assert result.config is config


async def test_abind_direct_calling_constructs_schema_and_awaits_emitter() -> None:
    """
    Calling an AsyncBoundEvent produced by abind constructs the schema and awaits the emitter.

    Given: An AsyncBoundEvent produced by abind in direct form
    When: The AsyncBoundEvent is called with arguments
    Then: The emitter should receive the constructed payload, the EventConfig, and the config
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)
    bound = abind(emitter, ev, config=None)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(_OrderCreated(1, "pending"), ev, None, serializer=None)


async def test_abind_direct_returns_emitter_result() -> None:
    """
    Calling an AsyncBoundEvent produced by abind returns the emitter's result.

    Given: An AsyncBoundEvent whose emitter returns the constructed payload
    When: The AsyncBoundEvent is called
    Then: The return value should match the constructed payload
    """
    # Arrange
    emitter = create_autospec(_async_emit)

    def _return(
        payload: object, event: object, config: object, serializer: object = None
    ) -> object:
        return payload

    emitter.side_effect = _return
    ev = EventConfig(_OrderCreated, PubSub)
    bound = abind(emitter, ev, config=None)

    # Act
    result = await bound(1, "pending")

    # Assert
    assert result == _OrderCreated(1, "pending")


def test_abind_direct_stores_serializer() -> None:
    """
    Abind stores the provided serializer on the returned AsyncBoundEvent.

    Given: A serializer callable
    When: abind is called in direct form with that serializer
    Then: The AsyncBoundEvent's serializer should reference the same callable
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)
    serializer = Mock()

    # Act
    result = abind(emitter, ev, config=None, serializer=serializer)

    # Assert
    assert result.serializer is serializer


async def test_abind_direct_forwards_serializer_to_emitter() -> None:
    """
    Calling an AsyncBoundEvent produced by abind forwards the serializer to the emitter.

    Given: An AsyncBoundEvent produced by abind in direct form with a serializer
    When: The AsyncBoundEvent is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)
    serializer = Mock()
    bound = abind(emitter, ev, config=None, serializer=serializer)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(_OrderCreated(1, "pending"), ev, None, serializer=serializer)


async def test_abind_direct_awaits_async_factory_then_awaits_emitter() -> None:
    """
    Abind with an async factory awaits the factory before forwarding to the emitter.

    Given: An EventConfig whose factory is a coroutine function
    When: The AsyncBoundEvent produced by abind is called
    Then: The emitter should receive the resolved payload, not the coroutine
    """
    # Arrange
    emitter = create_autospec(_async_emit)

    async def _async_factory(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    ev = EventConfig(_async_factory, PubSub, payload_type=_OrderCreated)
    bound = abind(emitter, ev, config=None)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(_OrderCreated(1, "pending"), ev, None, serializer=None)


# endregion

# region: Abind Decorator


def test_abind_decorator_form_returns_callable() -> None:
    """
    Abind without an event returns a callable decorator.

    Given: An async emitter and a config, but no event
    When: abind is called
    Then: The result should be callable
    """
    # Act
    emitter = create_autospec(_async_emit)
    result = abind(emitter, config=None)

    # Assert
    assert callable(result)


def test_abind_decorator_form_applied_to_event_returns_async_bound_event() -> None:
    """
    The decorator returned by abind produces an AsyncBoundEvent when applied to an Event.

    Given: A decorator returned by abind and an Event
    When: The decorator is applied to the Event
    Then: The result should be an AsyncBoundEvent instance
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = abind(emitter, config=None)(ev)

    # Assert
    assert isinstance(result, AsyncBoundEvent)


def test_abind_decorator_form_uses_event_schema_as_factory() -> None:
    """
    The AsyncBoundEvent produced by the abind decorator uses the event's schema as its factory.

    Given: A decorator returned by abind applied to an Event
    When: The AsyncBoundEvent is produced
    Then: Its factory should be the event's schema class
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = abind(emitter, config=None)(ev)

    # Assert
    assert result.event.factory is _OrderCreated


def test_abind_decorator_form_stores_config() -> None:
    """
    The AsyncBoundEvent produced by the abind decorator stores the provided config.

    Given: A distinct config object passed to abind
    When: The decorator is applied to an Event
    Then: The AsyncBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)
    config = object()

    # Act
    result = abind(emitter, config=config)(ev)

    # Assert
    assert result.config is config


def test_abind_decorator_form_stores_serializer() -> None:
    """
    The AsyncBoundEvent produced by the abind decorator stores the provided serializer.

    Given: A serializer callable passed to abind
    When: The decorator is applied to an Event
    Then: The AsyncBoundEvent's serializer should reference the same callable
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)
    serializer = Mock()

    # Act
    result = abind(emitter, config=None, serializer=serializer)(ev)

    # Assert
    assert result.serializer is serializer


async def test_abind_decorator_form_forwards_serializer_to_emitter() -> None:
    """
    Calling an AsyncBoundEvent produced by the abind decorator forwards the serializer.

    Given: An AsyncBoundEvent produced by the abind decorator with a serializer
    When: The AsyncBoundEvent is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = EventConfig(_OrderCreated, PubSub)
    serializer = Mock()
    bound = abind(emitter, config=None, serializer=serializer)(ev)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(_OrderCreated(1, "pending"), ev, None, serializer=serializer)


async def test_abind_decorator_awaits_async_factory_then_awaits_emitter() -> None:
    """
    The decorator form of abind with an async factory awaits the factory before the emitter.

    Given: An EventConfig whose factory is a coroutine function
    When: The decorator returned by abind is applied and the result is called
    Then: The emitter should receive the resolved payload, not the coroutine
    """
    # Arrange
    emitter = create_autospec(_async_emit)

    async def _async_factory(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    ev = EventConfig(_async_factory, PubSub, payload_type=_OrderCreated)
    bound = abind(emitter, config=None)(ev)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(_OrderCreated(1, "pending"), ev, None, serializer=None)


# endregion
