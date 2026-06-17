"""
Unit tests for bind and bind_factory.

This test suite verifies the following behaviors:

bind — direct form:
- Returns a BoundEvent.
- The BoundEvent uses the event's schema as its factory.
- The BoundEvent stores the provided config.
- Calling the BoundEvent constructs the schema and invokes the emitter.
- Calling the BoundEvent returns the emitter's result.
- Raises TypeError when the event's factory is async.

bind — decorator form:
- Returns a callable when no event is provided.
- Applying the callable to an Event returns a BoundEvent.
- The returned BoundEvent uses the event's schema as its factory.
- The returned BoundEvent stores the provided config.
- Raises TypeError when the event's factory is async.
"""

import asyncio
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stratae.events.bound import BoundEvent, bind
from stratae.events.event import EventConfig, Payload, PubSub


class _OrderCreated(Payload):
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderCreated):
            return False
        return self.order_id == other.order_id and self.status == other.status


# region: Bind Direct


def test_bind_direct_returns_bound_event() -> None:
    """
    Bind with an event returns a BoundEvent.

    Given: An emitter, an Event, and a config
    When: bind is called with all three
    Then: The result should be a BoundEvent instance
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, ev, config=None)

    # Assert
    assert isinstance(result, BoundEvent)


def test_bind_direct_stores_event() -> None:
    """
    Bind stores the EventConfig on the returned BoundEvent.

    Given: An EventConfig
    When: bind is called in direct form
    Then: The BoundEvent's event should be the supplied EventConfig
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, ev, config=None)

    # Assert
    assert result.event is ev


def test_bind_direct_stores_config() -> None:
    """
    Bind stores the provided config on the returned BoundEvent.

    Given: A distinct config object
    When: bind is called in direct form with that config
    Then: The BoundEvent's config should reference the same object
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)
    config = object()

    # Act
    result = bind(emitter, ev, config=config)

    # Assert
    assert result.config is config


def test_bind_direct_calling_constructs_schema_and_invokes_emitter(mocker: MockerFixture) -> None:
    """
    Calling a BoundEvent produced by bind constructs the schema and calls the emitter.

    Given: A BoundEvent produced by bind in direct form
    When: The BoundEvent is called with arguments
    Then: The schema constructor should be called with those arguments and the emitter
          should receive the constructed payload, the EventConfig, and the config
    """
    # Arrange
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)
    bound = bind(emitter, ev, config=None)

    # Act
    bound(1, "pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(_OrderCreated(1, "pending"), ev, None)


def test_bind_direct_returns_emitter_result() -> None:
    """
    Calling a BoundEvent produced by bind returns the emitter's result.

    Given: A BoundEvent whose emitter returns a known value
    When: The BoundEvent is called
    Then: The return value should match the emitter's return value
    """
    # Arrange
    emitter = Mock(return_value="dispatched")
    ev = EventConfig(_OrderCreated, PubSub)
    bound = bind(emitter, ev, config=None)

    # Act
    result = bound(1, "pending")

    # Assert
    assert result == "dispatched"


def test_bind_direct_raises_for_async_factory() -> None:
    """
    Bind raises TypeError when the event's factory is async.

    Given: An EventConfig whose factory is a coroutine function
    When: bind is called in direct form
    Then: A TypeError should be raised
    """

    # Arrange
    async def _async_factory(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    emitter = Mock()
    ev = EventConfig(_async_factory, PubSub, payload_type=_OrderCreated)

    # Act / Assert
    with pytest.raises(TypeError):
        bind(emitter, ev, config=None)


# endregion

# region: Bind Decorator


def test_bind_decorator_form_returns_callable() -> None:
    """
    Bind without an event returns a callable decorator.

    Given: An emitter and a config, but no event
    When: bind is called
    Then: The result should be callable
    """
    # Arrange
    emitter = Mock()

    # Act
    result = bind(emitter, config=None)

    # Assert
    assert callable(result)


def test_bind_decorator_form_applied_to_event_returns_bound_event() -> None:
    """
    The decorator returned by bind produces a BoundEvent when applied to an Event.

    Given: A decorator returned by bind and an Event
    When: The decorator is applied to the Event
    Then: The result should be a BoundEvent instance
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, config=None)(ev)

    # Assert
    assert isinstance(result, BoundEvent)


def test_bind_decorator_form_stores_event() -> None:
    """
    The BoundEvent produced by the bind decorator stores the supplied EventConfig.

    Given: A decorator returned by bind applied to an EventConfig
    When: The BoundEvent is produced
    Then: Its event should be the supplied EventConfig
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, config=None)(ev)

    # Assert
    assert result.event is ev


def test_bind_decorator_form_stores_config() -> None:
    """
    The BoundEvent produced by the bind decorator stores the provided config.

    Given: A distinct config object passed to bind
    When: The decorator is applied to an Event
    Then: The BoundEvent's config should reference the same object
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)
    config = object()

    # Act
    result = bind(emitter, config=config)(ev)

    # Assert
    assert result.config is config


def test_bind_decorator_raises_for_async_factory() -> None:
    """
    The decorator returned by bind raises TypeError when the event's factory is async.

    Given: An EventConfig whose factory is a coroutine function
    When: The decorator returned by bind is applied to that event
    Then: A TypeError should be raised
    """

    # Arrange
    async def _async_factory(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    emitter = Mock()
    ev = EventConfig(_async_factory, PubSub, payload_type=_OrderCreated)

    # Act / Assert
    with pytest.raises(TypeError):
        bind(emitter, config=None)(ev)


# endregion
