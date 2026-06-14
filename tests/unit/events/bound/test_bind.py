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

bind_factory — direct form:
- Returns a BoundEvent.
- The BoundEvent uses the provided factory, not the event's schema.
- The BoundEvent stores the provided config.
- Calling the BoundEvent invokes the factory and passes its result to the emitter.
- Calling the BoundEvent returns the emitter's result.

bind_factory — decorator form:
- Returns a callable when no factory is provided.
- Applying the callable to a factory function returns a BoundEvent.
- The returned BoundEvent uses the decorated function as its factory.
- The returned BoundEvent stores the provided config.
"""

import asyncio
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stratae.events.bound import BoundEvent, bind, bind_factory
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


def test_bind_direct_uses_event_schema_as_factory() -> None:
    """
    Bind stores the event's schema class as the BoundEvent's factory.

    Given: An Event whose schema is _OrderCreated
    When: bind is called in direct form
    Then: The BoundEvent's factory should be _OrderCreated
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, ev, config=None)

    # Assert
    assert result.factory is _OrderCreated


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
          should receive the constructed payload and the BoundEvent itself
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
    emitter.assert_called_once_with(_OrderCreated(1, "pending"), bound)


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


def test_bind_decorator_form_uses_event_schema_as_factory() -> None:
    """
    The BoundEvent produced by the bind decorator uses the event's schema as its factory.

    Given: A decorator returned by bind applied to an Event
    When: The BoundEvent is produced
    Then: Its factory should be the event's schema class
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = bind(emitter, config=None)(ev)

    # Assert
    assert result.factory is _OrderCreated


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

# region: Bind Factory Direct


def test_bind_factory_direct_returns_bound_event() -> None:
    """
    bind_factory with an event and factory returns a BoundEvent.

    Given: An emitter, an Event, a factory callable, and a config
    When: bind_factory is called with all four
    Then: The result should be a BoundEvent instance
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)
    factory = Mock(return_value=_OrderCreated(1, "pending"))

    # Act
    result = bind_factory(emitter, ev, factory, config=None)

    # Assert
    assert isinstance(result, BoundEvent)


def test_bind_factory_direct_uses_factory_not_schema() -> None:
    """
    bind_factory stores the provided factory, not the event's schema class.

    Given: A factory callable distinct from the event's schema
    When: bind_factory is called in direct form
    Then: The BoundEvent's factory should be the provided callable, not the schema
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)
    factory = Mock(return_value=_OrderCreated(1, "pending"))

    # Act
    result = bind_factory(emitter, ev, factory, config=None)

    # Assert
    assert result.factory is factory
    assert result.factory is not _OrderCreated


def test_bind_factory_direct_stores_config() -> None:
    """
    bind_factory stores the provided config on the returned BoundEvent.

    Given: A distinct config object
    When: bind_factory is called in direct form with that config
    Then: The BoundEvent's config should reference the same object
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)
    factory = Mock(return_value=_OrderCreated(1, "pending"))
    config = object()

    # Act
    result = bind_factory(emitter, ev, factory, config=config)

    # Assert
    assert result.config is config


def test_bind_factory_direct_calling_invokes_factory_then_emitter() -> None:
    """
    Calling a BoundEvent from bind_factory calls the factory then passes its result to the emitter.

    Given: A BoundEvent produced by bind_factory in direct form
    When: The BoundEvent is called with arguments
    Then: The factory should be called with those arguments and the emitter should
          receive the factory's return value and the BoundEvent itself
    """
    # Arrange
    payload = _OrderCreated(1, "pending")
    factory = Mock(return_value=payload)
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)
    bound = bind_factory(emitter, ev, factory, config=None)

    # Act
    bound(1, "pending")

    # Assert
    factory.assert_called_once_with(1, "pending")
    emitter.assert_called_once_with(payload, bound)


def test_bind_factory_direct_returns_emitter_result() -> None:
    """
    Calling a BoundEvent from bind_factory returns the emitter's result.

    Given: A BoundEvent whose emitter returns a known value
    When: The BoundEvent is called
    Then: The return value should match the emitter's return value
    """
    # Arrange
    factory = Mock(return_value=_OrderCreated(1, "pending"))
    emitter = Mock(return_value="dispatched")
    ev = EventConfig(_OrderCreated, PubSub)
    bound = bind_factory(emitter, ev, factory, config=None)

    # Act
    result = bound(1, "pending")

    # Assert
    assert result == "dispatched"


# endregion

# region: Bind Factory Decorator


def test_bind_factory_decorator_form_returns_callable() -> None:
    """
    bind_factory without a factory returns a callable decorator.

    Given: An emitter, an Event, and a config, but no factory
    When: bind_factory is called
    Then: The result should be callable
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)

    # Act
    result = bind_factory(emitter, ev, config=None)

    # Assert
    assert callable(result)


def test_bind_factory_decorator_form_applied_to_factory_returns_bound_event() -> None:
    """
    The decorator returned by bind_factory produces a BoundEvent when applied to a factory.

    Given: A decorator returned by bind_factory and a factory function
    When: The decorator is applied to the factory
    Then: The result should be a BoundEvent instance
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)

    def _factory(order_id: int, status: str) -> _OrderCreated:
        return _OrderCreated(order_id, status.strip())

    # Act
    result = bind_factory(emitter, ev, config=None)(_factory)

    # Assert
    assert isinstance(result, BoundEvent)


def test_bind_factory_decorator_form_uses_decorated_function_as_factory() -> None:
    """
    The BoundEvent from bind_factory's decorator form uses the decorated function as its factory.

    Given: A decorator returned by bind_factory applied to a factory function
    When: The BoundEvent is produced
    Then: Its factory should be the decorated function
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)

    def _factory(order_id: int, status: str) -> _OrderCreated:
        return _OrderCreated(order_id, status.strip())

    # Act
    result = bind_factory(emitter, ev, config=None)(_factory)

    # Assert
    assert result.factory is _factory


def test_bind_factory_decorator_form_stores_config() -> None:
    """
    The BoundEvent from bind_factory's decorator form stores the provided config.

    Given: A distinct config object passed to bind_factory
    When: The decorator is applied to a factory
    Then: The BoundEvent's config should reference the same object
    """
    # Arrange
    emitter = Mock()
    ev = EventConfig(_OrderCreated, PubSub)
    config = object()

    def _factory(order_id: int, status: str) -> _OrderCreated:
        return _OrderCreated(order_id, status.strip())

    # Act
    result = bind_factory(emitter, ev, config=config)(_factory)

    # Assert
    assert result.config is config


# endregion
