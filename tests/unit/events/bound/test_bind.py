"""
Unit tests for bind.

This test suite verifies the following behaviors:

bind — with a factory:
- Returns a FactoryBoundEvent.
- The FactoryBoundEvent stores the provided event, factory, and config.
- Calling the FactoryBoundEvent constructs the schema and invokes the emitter.
- Calling the FactoryBoundEvent returns the emitter's result.
- Raises TypeError when the factory is async.

bind — without a factory:
- Returns a BoundEvent.
- The BoundEvent stores the provided event and config.
- Calling the BoundEvent forwards an already-built payload to the emitter.
- Calling the BoundEvent returns the emitter's result.
"""

import asyncio
from unittest.mock import Mock, create_autospec

import pytest
from pytest_mock import MockerFixture

from stratae.events import BoundEvent, EmitCallable, Event, FactoryBoundEvent, PubSub, bind


class _OrderCreated:
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderCreated):
            return False
        return self.order_id == other.order_id and self.status == other.status


# region: Bind With Factory


def test_bind_with_factory_returns_factory_bound_event() -> None:
    """
    Bind with a factory returns a FactoryBoundEvent.

    Given: An emitter, an Event, a factory, and a config
    When: bind is called with all four
    Then: The result should be a FactoryBoundEvent instance
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, ev, factory=_OrderCreated, config=None)

    # Assert
    assert isinstance(result, FactoryBoundEvent)


def test_bind_with_factory_stores_event() -> None:
    """
    Bind stores the Event on the returned FactoryBoundEvent.

    Given: An Event
    When: bind is called with a factory
    Then: The FactoryBoundEvent's event should be the supplied Event
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, ev, factory=_OrderCreated, config=None)

    # Assert
    assert result.event is ev


def test_bind_with_factory_stores_config() -> None:
    """
    Bind stores the provided config on the returned FactoryBoundEvent.

    Given: A distinct config object
    When: bind is called with a factory and that config
    Then: The FactoryBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)
    config = object()

    # Act
    result = bind(emitter, ev, factory=_OrderCreated, config=config)

    # Assert
    assert result.config is config


def test_bind_with_factory_calling_constructs_schema_and_invokes_emitter(
    mocker: MockerFixture,
) -> None:
    """
    Calling a FactoryBoundEvent produced by bind constructs the schema and calls the emitter.

    Given: A FactoryBoundEvent produced by bind
    When: The FactoryBoundEvent is called with arguments
    Then: The schema constructor should be called with those arguments and the emitter
          should receive the constructed payload, the Event, and the config
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    spy = mocker.spy(_OrderCreated, "__init__")
    ev = Event(_OrderCreated, PubSub)
    bound = bind(emitter, ev, factory=_OrderCreated, config=None)

    # Act
    bound(1, "pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(_OrderCreated(1, "pending"), ev, None, serializer=None)


def test_bind_with_factory_returns_emitter_result() -> None:
    """
    Calling a FactoryBoundEvent produced by bind returns the emitter's result.

    Given: A FactoryBoundEvent whose emitter echoes the constructed payload
    When: The FactoryBoundEvent is called
    Then: The return value should match the constructed payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)

    def _return(
        payload: object, event: object, config: object, serializer: object = None
    ) -> object:
        return payload

    emitter.side_effect = _return

    ev = Event(_OrderCreated, PubSub)
    bound = bind(emitter, ev, factory=_OrderCreated, config=None)

    # Act
    result = bound(1, "pending")

    # Assert
    assert result == _OrderCreated(1, "pending")


def test_bind_with_factory_stores_serializer() -> None:
    """
    Bind stores the provided serializer on the returned FactoryBoundEvent.

    Given: A serializer callable
    When: bind is called with a factory and that serializer
    Then: The FactoryBoundEvent's serializer should reference the same callable
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)
    serializer = Mock()

    # Act
    result = bind(emitter, ev, factory=_OrderCreated, config=None, serializer=serializer)

    # Assert
    assert result.serializer is serializer


def test_bind_with_factory_forwards_serializer_to_emitter() -> None:
    """
    Calling a FactoryBoundEvent produced by bind forwards the serializer to the emitter.

    Given: A FactoryBoundEvent produced by bind with a serializer
    When: The FactoryBoundEvent is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)
    serializer = Mock()
    bound = bind(emitter, ev, factory=_OrderCreated, config=None, serializer=serializer)

    # Act
    bound(1, "pending")

    # Assert
    emitter.assert_called_once_with(_OrderCreated(1, "pending"), ev, None, serializer=serializer)


def test_bind_with_factory_raises_for_async_factory() -> None:
    """
    Bind raises TypeError when the supplied factory is async.

    Given: An async factory
    When: bind is called with that factory
    Then: A TypeError should be raised
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)

    async def _async_factory(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    # Act / Assert
    with pytest.raises(TypeError):
        bind(emitter, ev, factory=_async_factory, config=None)


# endregion

# region: Bind Without Factory


def test_bind_without_factory_returns_bound_event() -> None:
    """
    Bind with no factory returns a BoundEvent.

    Given: An emitter, an Event, and a config, but no factory
    When: bind is called with just those three
    Then: The result should be a BoundEvent instance
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, ev, config=None)

    # Assert
    assert isinstance(result, BoundEvent)


def test_bind_without_factory_stores_event() -> None:
    """
    Bind stores the Event on the returned BoundEvent.

    Given: An Event
    When: bind is called with no factory
    Then: The BoundEvent's event should be the supplied Event
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, ev, config=None)

    # Assert
    assert result.event is ev


def test_bind_without_factory_stores_config() -> None:
    """
    Bind stores the provided config on the returned BoundEvent.

    Given: A distinct config object
    When: bind is called with no factory and that config
    Then: The BoundEvent's config should reference the same object
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)
    config = object()

    # Act
    result = bind(emitter, ev, config=config)

    # Assert
    assert result.config is config


def test_bind_without_factory_calling_forwards_payload_to_emitter() -> None:
    """
    Calling a BoundEvent produced by bind forwards an already-built payload.

    Given: A BoundEvent produced by bind with no factory
    When: The BoundEvent is called with a ready-made payload
    Then: The emitter should receive that exact payload, the Event, and the config
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)
    bound = bind(emitter, ev, config=None)
    payload = _OrderCreated(1, "pending")

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(payload, ev, None, serializer=None)


def test_bind_without_factory_returns_emitter_result() -> None:
    """
    Calling a BoundEvent produced by bind returns the emitter's result.

    Given: A BoundEvent whose emitter echoes the payload
    When: The BoundEvent is called
    Then: The return value should match the payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)

    def _return(
        payload: object, event: object, config: object, serializer: object = None
    ) -> object:
        return payload

    emitter.side_effect = _return

    ev = Event(_OrderCreated, PubSub)
    bound = bind(emitter, ev, config=None)
    payload = _OrderCreated(1, "pending")

    # Act
    result = bound(payload)

    # Assert
    assert result is payload


def test_bind_without_factory_forwards_serializer_to_emitter() -> None:
    """
    Calling a BoundEvent produced by bind forwards the serializer to the emitter.

    Given: A BoundEvent produced by bind with a serializer
    When: The BoundEvent is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(_OrderCreated, PubSub)
    serializer = Mock()
    bound = bind(emitter, ev, config=None, serializer=serializer)
    payload = _OrderCreated(1, "pending")

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(payload, ev, None, serializer=serializer)


# endregion
