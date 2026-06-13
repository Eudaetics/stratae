"""
Unit tests for abind and abind_factory.

This test suite verifies the following behaviors:

abind — direct form:
- Returns an AsyncBoundEvent.
- The AsyncBoundEvent uses the event's schema as its factory.
- The AsyncBoundEvent stores the provided config.
- Calling the AsyncBoundEvent constructs the schema and awaits the emitter.
- Calling the AsyncBoundEvent returns the emitter's result.

abind — decorator form:
- Returns a callable when no event is provided.
- Applying the callable to an Event returns an AsyncBoundEvent.
- The returned AsyncBoundEvent uses the event's schema as its factory.
- The returned AsyncBoundEvent stores the provided config.

abind_factory — direct form:
- Returns an AsyncBoundEvent.
- The AsyncBoundEvent uses the provided factory, not the event's schema.
- The AsyncBoundEvent stores the provided config.
- Calling the AsyncBoundEvent invokes the factory and awaits the emitter with its result.
- Calling the AsyncBoundEvent returns the emitter's result.

abind_factory — direct form (async factory):
- Returns an AsyncBoundEvent.
- The AsyncBoundEvent uses the provided async factory, not the event's schema.
- The AsyncBoundEvent stores the provided config.
- Calling the AsyncBoundEvent awaits the factory then awaits the emitter with its result.
- Calling the AsyncBoundEvent returns the emitter's result.

abind_factory — decorator form (sync factory):
- Returns a callable when no factory is provided.
- Applying the callable to a factory function returns an AsyncBoundEvent.
- The returned AsyncBoundEvent uses the decorated function as its factory.
- The returned AsyncBoundEvent stores the provided config.

abind_factory — decorator form (async factory):
- Applying the callable to an async factory function returns an AsyncBoundEvent.
- The returned AsyncBoundEvent uses the decorated async function as its factory.
- The returned AsyncBoundEvent stores the provided config.
"""

from unittest.mock import AsyncMock

from pytest_mock import MockerFixture

from stratae.events.bound import AsyncBoundEvent, abind, abind_factory
from stratae.events.event import Event, EventSchema, PubSub


class _OrderCreated(EventSchema):
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
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)

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
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)

    # Act
    result = abind(emitter, ev, config=None)

    # Assert
    assert result.factory is _OrderCreated


def test_abind_direct_stores_config() -> None:
    """
    Abind stores the provided config on the returned AsyncBoundEvent.

    Given: A distinct config object
    When: abind is called in direct form with that config
    Then: The AsyncBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    config = object()

    # Act
    result = abind(emitter, ev, config=config)

    # Assert
    assert result.config is config


async def test_abind_direct_calling_constructs_schema_and_awaits_emitter(
    mocker: MockerFixture,
) -> None:
    """
    Calling an AsyncBoundEvent produced by abind constructs the schema and awaits the emitter.

    Given: An AsyncBoundEvent produced by abind in direct form
    When: The AsyncBoundEvent is called with arguments
    Then: The schema constructor should be called with those arguments and the emitter
          should receive the constructed payload and the AsyncBoundEvent itself
    """
    # Arrange
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    bound = abind(emitter, ev, config=None)

    # Act
    await bound(1, "pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_awaited_once_with(_OrderCreated(1, "pending"), bound)


async def test_abind_direct_returns_emitter_result() -> None:
    """
    Calling an AsyncBoundEvent produced by abind returns the emitter's result.

    Given: An AsyncBoundEvent whose emitter resolves to a known value
    When: The AsyncBoundEvent is called
    Then: The return value should match the emitter's resolved value
    """
    # Arrange
    emitter = AsyncMock(return_value="dispatched")
    ev = Event(_OrderCreated, PubSub)
    bound = abind(emitter, ev, config=None)

    # Act
    result = await bound(1, "pending")

    # Assert
    assert result == "dispatched"


# endregion

# region: Abind Decorator


def test_abind_decorator_form_returns_callable() -> None:
    """
    Abind without an event returns a callable decorator.

    Given: An async emitter and a config, but no event
    When: abind is called
    Then: The result should be callable
    """
    # Arrange
    emitter = AsyncMock()

    # Act
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
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)

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
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)

    # Act
    result = abind(emitter, config=None)(ev)

    # Assert
    assert result.factory is _OrderCreated


def test_abind_decorator_form_stores_config() -> None:
    """
    The AsyncBoundEvent produced by the abind decorator stores the provided config.

    Given: A distinct config object passed to abind
    When: The decorator is applied to an Event
    Then: The AsyncBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    config = object()

    # Act
    result = abind(emitter, config=config)(ev)

    # Assert
    assert result.config is config


# endregion

# region: Abind Factory Direct


def test_abind_factory_direct_returns_async_bound_event() -> None:
    """
    abind_factory with an event and factory returns an AsyncBoundEvent.

    Given: An async emitter, an Event, a factory callable, and a config
    When: abind_factory is called with all four
    Then: The result should be an AsyncBoundEvent instance
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))

    # Act
    result = abind_factory(emitter, ev, factory, config=None)

    # Assert
    assert isinstance(result, AsyncBoundEvent)


def test_abind_factory_direct_uses_factory_not_schema() -> None:
    """
    abind_factory stores the provided factory, not the event's schema class.

    Given: A factory callable distinct from the event's schema
    When: abind_factory is called in direct form
    Then: The AsyncBoundEvent's factory should be the provided callable, not the schema
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))

    # Act
    result = abind_factory(emitter, ev, factory, config=None)

    # Assert
    assert result.factory is factory
    assert result.factory is not _OrderCreated


def test_abind_factory_direct_stores_config() -> None:
    """
    abind_factory stores the provided config on the returned AsyncBoundEvent.

    Given: A distinct config object
    When: abind_factory is called in direct form with that config
    Then: The AsyncBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))
    config = object()

    # Act
    result = abind_factory(emitter, ev, factory, config=config)

    # Assert
    assert result.config is config


async def test_abind_factory_direct_calling_invokes_factory_then_awaits_emitter() -> None:
    """
    Calling an AsyncBoundEvent from abind_factory calls the factory then awaits the emitter.

    Given: An AsyncBoundEvent produced by abind_factory in direct form
    When: The AsyncBoundEvent is called with arguments
    Then: The factory should be called with those arguments and the emitter should be
          awaited with the factory's return value and the AsyncBoundEvent itself
    """
    # Arrange
    payload = _OrderCreated(1, "pending")

    def _factory(order_id: int, status: str) -> _OrderCreated:
        return payload

    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    bound = abind_factory(emitter, ev, _factory, config=None)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(payload, bound)


async def test_abind_factory_direct_returns_emitter_result() -> None:
    """
    Calling an AsyncBoundEvent from abind_factory returns the emitter's resolved value.

    Given: An AsyncBoundEvent whose emitter resolves to a known value
    When: The AsyncBoundEvent is called
    Then: The return value should match the emitter's resolved value
    """

    # Arrange
    def _factory(order_id: int, status: str) -> _OrderCreated:
        return _OrderCreated(order_id, status)

    emitter = AsyncMock(return_value="dispatched")
    ev = Event(_OrderCreated, PubSub)
    bound = abind_factory(emitter, ev, _factory, config=None)

    # Act
    result = await bound(1, "pending")

    # Assert
    assert result == "dispatched"


# endregion

# region: Abind Factory Decorator Sync


def test_abind_factory_decorator_form_returns_callable() -> None:
    """
    abind_factory without a factory returns a callable decorator.

    Given: An async emitter, an Event, and a config, but no factory
    When: abind_factory is called
    Then: The result should be callable
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)

    # Act
    result = abind_factory(emitter, ev, config=None)

    # Assert
    assert callable(result)


def test_abind_factory_decorator_form_applied_to_factory_returns_async_bound_event() -> None:
    """
    The decorator returned by abind_factory produces an AsyncBoundEvent when applied to a factory.

    Given: A decorator returned by abind_factory and a factory function
    When: The decorator is applied to the factory
    Then: The result should be an AsyncBoundEvent instance
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)

    def _factory(order_id: int, status: str) -> _OrderCreated:
        return _OrderCreated(order_id, status.strip())

    # Act
    result = abind_factory(emitter, ev, config=None)(_factory)

    # Assert
    assert isinstance(result, AsyncBoundEvent)


def test_abind_factory_decorator_form_uses_decorated_function_as_factory() -> None:
    """
    The AsyncBoundEvent from abind_factory's decorator uses the decorated function as its factory.

    Given: A decorator returned by abind_factory applied to a factory function
    When: The AsyncBoundEvent is produced
    Then: Its factory should be the decorated function
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)

    def _factory(order_id: int, status: str) -> _OrderCreated:
        return _OrderCreated(order_id, status.strip())

    # Act
    result = abind_factory(emitter, ev, config=None)(_factory)

    # Assert
    assert result.factory is _factory


def test_abind_factory_decorator_form_stores_config() -> None:
    """
    The AsyncBoundEvent from abind_factory's decorator form stores the provided config.

    Given: A distinct config object passed to abind_factory
    When: The decorator is applied to a factory
    Then: The AsyncBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    config = object()

    def _factory(order_id: int, status: str) -> _OrderCreated:
        return _OrderCreated(order_id, status.strip())

    # Act
    result = abind_factory(emitter, ev, config=config)(_factory)

    # Assert
    assert result.config is config


# endregion

# region: Abind Factory Direct Async


def test_abind_factory_async_factory_direct_returns_async_bound_event() -> None:
    """
    abind_factory with an async factory returns an AsyncBoundEvent.

    Given: An async emitter, an Event, an async factory, and a config
    When: abind_factory is called with all four
    Then: The result should be an AsyncBoundEvent instance
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))

    # Act
    result = abind_factory(emitter, ev, factory, config=None)

    # Assert
    assert isinstance(result, AsyncBoundEvent)


def test_abind_factory_async_factory_direct_uses_factory_not_schema() -> None:
    """
    abind_factory with an async factory stores that factory, not the event's schema.

    Given: An async factory distinct from the event's schema
    When: abind_factory is called in direct form
    Then: The AsyncBoundEvent's factory should be the provided async callable, not the schema
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))

    # Act
    result = abind_factory(emitter, ev, factory, config=None)

    # Assert
    assert result.factory is factory
    assert result.factory is not _OrderCreated


def test_abind_factory_async_factory_direct_stores_config() -> None:
    """
    abind_factory with an async factory stores the provided config.

    Given: A distinct config object
    When: abind_factory is called in direct form with an async factory and that config
    Then: The AsyncBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))
    config = object()

    # Act
    result = abind_factory(emitter, ev, factory, config=config)

    # Assert
    assert result.config is config


async def test_abind_factory_async_factory_direct_awaits_factory_then_awaits_emitter() -> None:
    """
    Calling an AsyncBoundEvent with an async factory awaits the factory then awaits the emitter.

    Given: An AsyncBoundEvent produced by abind_factory with an async factory
    When: The AsyncBoundEvent is called with arguments
    Then: The async factory should be awaited with those arguments and the emitter should be
          awaited with the factory's resolved value and the AsyncBoundEvent itself
    """
    # Arrange
    payload = _OrderCreated(1, "pending")
    factory = AsyncMock(return_value=payload)
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    bound = abind_factory(emitter, ev, factory, config=None)

    # Act
    await bound(1, "pending")

    # Assert
    factory.assert_awaited_once_with(1, "pending")
    emitter.assert_awaited_once_with(payload, bound)


async def test_abind_factory_async_factory_direct_returns_emitter_result() -> None:
    """
    Calling an AsyncBoundEvent with an async factory returns the emitter's resolved value.

    Given: An AsyncBoundEvent with an async factory whose emitter resolves to a known value
    When: The AsyncBoundEvent is called
    Then: The return value should match the emitter's resolved value
    """
    # Arrange
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))
    emitter = AsyncMock(return_value="dispatched")
    ev = Event(_OrderCreated, PubSub)
    bound = abind_factory(emitter, ev, factory, config=None)

    # Act
    result = await bound(1, "pending")

    # Assert
    assert result == "dispatched"


# endregion

# region: Abind Factory Decorator Async


def test_abind_factory_async_factory_decorator_form_returns_async_bound_event() -> None:
    """
    The abind_factory decorator produces an AsyncBoundEvent when applied to an async factory.

    Given: A decorator returned by abind_factory and an async factory
    When: The decorator is applied to the async factory
    Then: The result should be an AsyncBoundEvent instance
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))

    # Act
    result = abind_factory(emitter, ev, config=None)(factory)

    # Assert
    assert isinstance(result, AsyncBoundEvent)


def test_abind_factory_async_factory_decorator_form_uses_decorated_async_function_as_factory() -> (
    None
):
    """
    The AsyncBoundEvent from the decorator form uses the decorated async function as its factory.

    Given: A decorator returned by abind_factory applied to an async factory
    When: The AsyncBoundEvent is produced
    Then: Its factory should be the decorated async factory
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))

    # Act
    result = abind_factory(emitter, ev, config=None)(factory)

    # Assert
    assert result.factory is factory


def test_abind_factory_async_factory_decorator_form_stores_config() -> None:
    """
    The AsyncBoundEvent from the decorator form with an async factory stores the provided config.

    Given: A distinct config object passed to abind_factory
    When: The decorator is applied to an async factory
    Then: The AsyncBoundEvent's config should reference the same object
    """
    # Arrange
    emitter = AsyncMock()
    ev = Event(_OrderCreated, PubSub)
    config = object()
    factory = AsyncMock(return_value=_OrderCreated(1, "pending"))

    # Act
    result = abind_factory(emitter, ev, config=config)(factory)

    # Assert
    assert result.config is config


# endregion
