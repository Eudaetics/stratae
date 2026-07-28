"""
Unit tests for bind.

This test suite verifies the following behaviors:

bind — with a factory:
- Calling the returned callable constructs the payload via the factory and invokes the emitter.
- Calling the returned callable returns the emitter's result.
- The serializer is forwarded to the emitter.
- Raises TypeError when the factory is async.

bind — without a factory:
- Calling the returned callable forwards an already-built payload to the emitter.
- Calling the returned callable returns the emitter's result.
- The serializer is forwarded to the emitter.
"""

import asyncio
from unittest.mock import Mock, create_autospec

import pytest
from pytest_mock import MockerFixture

from stratae.events import EmitCallable, Event, PubSub, bind


class _OrderCreated:
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderCreated):
            return False
        return self.order_id == other.order_id and self.status == other.status


# region: Bind With Factory


def test_bind_with_factory_calling_constructs_schema_and_invokes_emitter(
    mocker: MockerFixture,
) -> None:
    """
    Calling a factory-bound callable produced by bind constructs the schema and calls the emitter.

    Given: A factory-bound callable produced by bind
    When: The callable is called with arguments
    Then: The schema constructor should be called with those arguments and the emitter
          should receive the Event, the config, and the constructed payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    spy = mocker.spy(_OrderCreated, "__init__")
    ev = Event(PubSub, _OrderCreated)
    bound = bind(emitter, ev, factory=_OrderCreated, config=None)

    # Act
    bound(1, "pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(ev, None, _OrderCreated(1, "pending"), serializer=None)


def test_bind_with_factory_returns_emitter_result() -> None:
    """
    Calling a factory-bound callable produced by bind returns the emitter's result.

    Given: A factory-bound callable whose emitter echoes the constructed payload
    When: The callable is called
    Then: The return value should match the constructed payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)

    def _return(
        event: object, config: object, payload: object, serializer: object = None
    ) -> object:
        return payload

    emitter.side_effect = _return

    ev = Event(PubSub, _OrderCreated)
    bound = bind(emitter, ev, factory=_OrderCreated, config=None)

    # Act
    result = bound(1, "pending")

    # Assert
    assert result == _OrderCreated(1, "pending")


def test_bind_with_factory_forwards_serializer_to_emitter() -> None:
    """
    Calling a factory-bound callable produced by bind forwards the serializer to the emitter.

    Given: A factory-bound callable produced by bind with a serializer
    When: The callable is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(PubSub, _OrderCreated)
    serializer = Mock()
    bound = bind(emitter, ev, factory=_OrderCreated, config=None, serializer=serializer)

    # Act
    bound(1, "pending")

    # Assert
    emitter.assert_called_once_with(ev, None, _OrderCreated(1, "pending"), serializer=serializer)


def test_bind_with_factory_raises_for_async_factory() -> None:
    """
    Bind raises TypeError when the supplied factory is async.

    Given: An async factory
    When: bind is called with that factory
    Then: A TypeError should be raised
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(PubSub, _OrderCreated)

    async def _async_factory(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    # Act / Assert
    with pytest.raises(TypeError):
        bind(emitter, ev, factory=_async_factory, config=None)


# endregion

# region: Bind Without Factory


def test_bind_without_factory_calling_forwards_payload_to_emitter() -> None:
    """
    Calling a passthrough callable produced by bind forwards an already-built payload.

    Given: A passthrough callable produced by bind with no factory
    When: The callable is called with a ready-made payload
    Then: The emitter should receive the Event, the config, and that exact payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(PubSub, _OrderCreated)
    bound = bind(emitter, ev, config=None)
    payload = _OrderCreated(1, "pending")

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(ev, None, payload, serializer=None)


def test_bind_without_factory_returns_emitter_result() -> None:
    """
    Calling a passthrough callable produced by bind returns the emitter's result.

    Given: A passthrough callable whose emitter echoes the payload
    When: The callable is called
    Then: The return value should match the payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)

    def _return(
        event: object, config: object, payload: object, serializer: object = None
    ) -> object:
        return payload

    emitter.side_effect = _return

    ev = Event(PubSub, _OrderCreated)
    bound = bind(emitter, ev, config=None)
    payload = _OrderCreated(1, "pending")

    # Act
    result = bound(payload)

    # Assert
    assert result is payload


def test_bind_without_factory_forwards_serializer_to_emitter() -> None:
    """
    Calling a passthrough callable produced by bind forwards the serializer to the emitter.

    Given: A passthrough callable produced by bind with a serializer
    When: The callable is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = Event(PubSub, _OrderCreated)
    serializer = Mock()
    bound = bind(emitter, ev, config=None, serializer=serializer)
    payload = _OrderCreated(1, "pending")

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(ev, None, payload, serializer=serializer)


# endregion
