"""
Unit tests for Event, EventType, PubSub, and the event decorator.

This test suite verifies the following behaviors:

EventConfig:
- The factory and event_type are stored on initialization.
- payload_type is derived from factory when factory is a class.
- payload_type accepts an explicit override.
- Raises TypeError when factory is not a class and payload_type is not provided.
- name defaults to factory.__name__ when not provided.
- name accepts an explicit override.

AsyncEventConfig:
- Is a subclass of EventConfig.
- The factory and event_type are stored on initialization.
- payload_type accepts an explicit override.
- Raises TypeError when payload_type is not provided.
- name defaults to factory.__name__ when not provided.
- name accepts an explicit override.
- factory is typed and callable as an async factory.

PubSub:
- Is a subclass of EventType.

Request:
- Is a subclass of EventType.
- EventConfig accepts a subscripted Request discriminant.
- EventConfig raises TypeError for a bare Request discriminant.
- The event decorator accepts a subscripted Request discriminant.

reply_type:
- Returns the type Request was subscripted with.
- Raises TypeError when the discriminant is not a subscripted Request.

event decorator:
- Returns an EventConfig instance.
- The returned EventConfig stores the decorated class as its factory.
- The returned EventConfig stores the supplied event_type.
- payload_type is derived from the decorated class when not provided.
- payload_type accepts an explicit value for factory functions.
- name defaults to the decorated callable's __name__ when not provided.
- name accepts an explicit override.
- Returns an AsyncEventConfig when an async factory is supplied with payload_type.
"""

import asyncio
from typing import cast

import pytest

from stratae.events.event import (
    AsyncEventConfig,
    EventConfig,
    EventType,
    PubSub,
    Request,
    event,
    reply_type,
)


def test_event_stores_schema_and_event_type() -> None:
    """
    Test that schema and event_type are stored during initialization.

    Given: A class and an event_type
    When: An Event is created
    Then: The schema and event_type attributes should reference the supplied objects
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    ev = EventConfig(_OrderPlaced, PubSub)

    # Assert
    assert ev.factory is _OrderPlaced
    assert ev.event_type is PubSub


def test_pubsub_is_subclass_of_event_type() -> None:
    """
    Test that PubSub is a subclass of EventType.

    Given: PubSub and EventType
    When: The class hierarchy is inspected
    Then: PubSub should be a subclass of EventType
    """
    assert issubclass(PubSub, EventType)


def test_request_is_subclass_of_event_type() -> None:
    """
    Test that Request is a subclass of EventType.

    Given: Request and EventType
    When: The class hierarchy is inspected
    Then: Request should be a subclass of EventType
    """
    assert issubclass(Request, EventType)


def test_eventconfig_accepts_subscripted_request() -> None:
    """
    Test that EventConfig stores a subscripted Request discriminant.

    Given: A payload class and a Request discriminant subscripted with a reply type
    When: An EventConfig is created
    Then: event_type should equal the subscripted discriminant
    """

    # Arrange
    class _BookFound:
        def __init__(self, title: str) -> None:
            self.title = title

    class _FindBook:
        def __init__(self, query: str) -> None:
            self.query = query

    # Act
    ev = EventConfig(_FindBook, Request[_BookFound])

    # Assert
    assert ev.event_type == Request[_BookFound]


def test_eventconfig_raises_for_bare_request() -> None:
    """
    Test that EventConfig rejects an unsubscripted Request discriminant.

    Given: A payload class and the bare Request class
    When: An EventConfig is created
    Then: A TypeError should be raised
    """

    # Arrange
    class _FindBook:
        def __init__(self, query: str) -> None:
            self.query = query

    # Act / Assert
    with pytest.raises(TypeError):
        EventConfig(_FindBook, Request)


def test_event_decorator_accepts_subscripted_request() -> None:
    """
    Test that the event decorator accepts a subscripted Request discriminant.

    Given: A payload class decorated with @event(Request[...])
    When: The decorator is applied
    Then: The EventConfig should store the subscripted discriminant
    """

    # Arrange
    class _BookFound:
        def __init__(self, title: str) -> None:
            self.title = title

    # Act
    @event(Request[_BookFound])
    class _FindBook:
        def __init__(self, query: str) -> None:
            self.query = query

    # Assert
    assert isinstance(_FindBook, EventConfig)
    assert _FindBook.event_type == Request[_BookFound]


def test_reply_type_returns_subscripted_reply() -> None:
    """
    Test that reply_type recovers the type Request was subscripted with.

    Given: An EventConfig with a Request discriminant subscripted with a reply type
    When: reply_type is called with the event
    Then: The result should be the reply type class
    """

    # Arrange
    class _BookFound:
        def __init__(self, title: str) -> None:
            self.title = title

    @event(Request[_BookFound])
    class _FindBook:
        def __init__(self, query: str) -> None:
            self.query = query

    # Act
    recovered = reply_type(_FindBook)

    # Assert
    assert recovered is _BookFound


def test_reply_type_raises_for_non_request_discriminant() -> None:
    """
    Test that reply_type rejects an event whose discriminant is not a Request.

    Given: A PubSub EventConfig cast to a request event type
    When: reply_type is called with the event
    Then: A TypeError should be raised
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    ev = EventConfig(_OrderPlaced, PubSub)
    mistyped = cast(EventConfig[[int], _OrderPlaced, Request[object]], ev)

    # Act / Assert
    with pytest.raises(TypeError):
        reply_type(mistyped)


def test_event_decorator_returns_event_instance() -> None:
    """
    Test that the event decorator returns an Event instance.

    Given: A class decorated with @event
    When: The decorator is applied
    Then: The result should be an Event instance
    """

    # Arrange / Act
    @event(PubSub)
    class _Schema:
        def __init__(self, value: int) -> None:
            self.value = value

    # Assert
    assert isinstance(_Schema, EventConfig)


def test_event_decorator_stores_schema() -> None:
    """
    Test that the event decorator stores the decorated class as the schema.

    Given: A class decorated with @event
    When: The decorator is applied
    Then: The Event's schema should be the decorated class
    """

    # Arrange
    class _Schema:
        def __init__(self, value: int) -> None:
            self.value = value

    # Act
    decorated = event(PubSub)(_Schema)

    # Assert
    assert decorated.factory is _Schema


def test_event_decorator_stores_event_type() -> None:
    """
    Test that the event decorator stores the supplied event_type.

    Given: A class decorated with @event(PubSub)
    When: The decorator is applied
    Then: The Event's event_type should be PubSub
    """

    # Arrange
    class _Schema:
        def __init__(self, value: int) -> None:
            self.value = value

    # Act
    decorated = event(PubSub)(_Schema)

    # Assert
    assert decorated.event_type is PubSub


def test_event_decorator_with_factory_stores_explicit_payload_type() -> None:
    """
    Test that a factory function decorated with @event stores the explicit payload_type.

    Given: A class and a factory function returning it
    When: The factory is decorated with @event specifying payload_type explicitly
    Then: The EventConfig's payload_type should reference the supplied class
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    @event(PubSub, payload_type=_OrderPlaced)
    def create_order(order_id: int) -> _OrderPlaced:
        return _OrderPlaced(order_id)

    # Assert
    assert create_order.payload_type is _OrderPlaced
    assert not isinstance(create_order, AsyncEventConfig)


def test_eventconfig_derives_payload_type_from_class_factory() -> None:
    """
    Test that payload_type is derived from factory when factory is a class.

    Given: A class used directly as the factory
    When: An EventConfig is created without an explicit payload_type
    Then: payload_type should reference the factory class
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    ev = EventConfig(_OrderPlaced, PubSub)

    # Assert
    assert ev.payload_type is _OrderPlaced


def test_eventconfig_accepts_explicit_payload_type() -> None:
    """
    Test that payload_type accepts an explicit override.

    Given: A class and a factory function returning it
    When: An EventConfig is created with an explicit payload_type
    Then: payload_type should reference the supplied class
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    def create_order(order_id: int) -> _OrderPlaced:
        return _OrderPlaced(order_id)

    # Act
    ev = EventConfig(create_order, PubSub, payload_type=_OrderPlaced)

    # Assert
    assert ev.payload_type is _OrderPlaced


def test_eventconfig_raises_for_factory_without_payload_type() -> None:
    """
    Test that EventConfig raises TypeError when factory is not a class.

    Given: A factory function returning a payload without explicit payload_type
    When: An EventConfig is created
    Then: A TypeError should be raised
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    def create_order(order_id: int) -> _OrderPlaced:
        return _OrderPlaced(order_id)

    # Act / Assert
    with pytest.raises(TypeError):
        EventConfig(create_order, PubSub)


def test_eventconfig_derives_name_from_factory() -> None:
    """
    Test that name defaults to factory.__name__ when not provided.

    Given: A class
    When: An EventConfig is created without an explicit name
    Then: name should equal the factory's __name__
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    ev = EventConfig(_OrderPlaced, PubSub)

    # Assert
    assert ev.name == _OrderPlaced.__name__


def test_eventconfig_accepts_explicit_name() -> None:
    """
    Test that name accepts an explicit override.

    Given: A class and an explicit name
    When: An EventConfig is created with name provided
    Then: name should equal the supplied string
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    ev = EventConfig(_OrderPlaced, PubSub, name="order_placed")

    # Assert
    assert ev.name == "order_placed"


def test_event_decorator_derives_payload_type_from_class() -> None:
    """
    Test that the event decorator derives payload_type from the decorated class.

    Given: A class decorated with @event
    When: The decorator is applied without explicit payload_type
    Then: The EventConfig's payload_type should reference the decorated class
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    decorated = event(PubSub)(_OrderPlaced)

    # Assert
    assert decorated.payload_type is _OrderPlaced


def test_event_decorator_derives_name_from_class() -> None:
    """
    Test that the event decorator derives name from the decorated class's __name__.

    Given: A class decorated with @event
    When: The decorator is applied without explicit name
    Then: The EventConfig's name should equal the class's __name__
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    decorated = event(PubSub)(_OrderPlaced)

    # Assert
    assert decorated.name == "_OrderPlaced"


def test_event_decorator_accepts_explicit_name() -> None:
    """
    Test that the event decorator passes an explicit name through to EventConfig.

    Given: A class and an explicit name
    When: The decorator is applied with name provided
    Then: The EventConfig's name should equal the supplied string
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    decorated = event(PubSub, name="order_placed")(_OrderPlaced)

    # Assert
    assert decorated.name == "order_placed"


def test_asynceventconfig_is_subclass_of_eventconfig() -> None:
    """
    Test that AsyncEventConfig is a subclass of EventConfig.

    Given: AsyncEventConfig and EventConfig
    When: The class hierarchy is inspected
    Then: AsyncEventConfig should be a subclass of EventConfig
    """
    assert issubclass(AsyncEventConfig, EventConfig)


def test_asynceventconfig_stores_factory_and_event_type() -> None:
    """
    Test that factory and event_type are stored during initialization.

    Given: An async factory function and an event_type
    When: An AsyncEventConfig is created
    Then: The factory and event_type attributes should reference the supplied objects
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    async def create_order(order_id: int) -> _OrderPlaced:
        await asyncio.sleep(0)
        return _OrderPlaced(order_id)

    # Act
    ev = AsyncEventConfig(create_order, PubSub, payload_type=_OrderPlaced)

    # Assert
    assert ev.factory is create_order
    assert ev.event_type is PubSub


def test_asynceventconfig_stores_payload_type() -> None:
    """
    Test that payload_type is stored when provided explicitly.

    Given: An async factory and an explicit payload_type
    When: An AsyncEventConfig is created
    Then: payload_type should reference the supplied class
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    async def create_order(order_id: int) -> _OrderPlaced:
        await asyncio.sleep(0)
        return _OrderPlaced(order_id)

    # Act
    ev = AsyncEventConfig(create_order, PubSub, payload_type=_OrderPlaced)

    # Assert
    assert ev.payload_type is _OrderPlaced


def test_asynceventconfig_raises_without_payload_type() -> None:
    """
    Test that AsyncEventConfig raises TypeError when payload_type is not provided.

    Given: An async factory without an explicit payload_type
    When: An AsyncEventConfig is created
    Then: A TypeError should be raised
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    async def create_order(order_id: int) -> _OrderPlaced:
        await asyncio.sleep(0)
        return _OrderPlaced(order_id)

    # Act / Assert
    with pytest.raises(TypeError):
        AsyncEventConfig(create_order, PubSub)


def test_asynceventconfig_derives_name_from_factory() -> None:
    """
    Test that name defaults to factory.__name__ when not provided.

    Given: An async factory without an explicit name
    When: An AsyncEventConfig is created
    Then: name should equal the factory's __name__
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    async def create_order(order_id: int) -> _OrderPlaced:
        await asyncio.sleep(0)
        return _OrderPlaced(order_id)

    # Act
    ev = AsyncEventConfig(create_order, PubSub, payload_type=_OrderPlaced)

    # Assert
    assert ev.name == "create_order"


def test_asynceventconfig_accepts_explicit_name() -> None:
    """
    Test that name accepts an explicit override.

    Given: An async factory and an explicit name
    When: An AsyncEventConfig is created with name provided
    Then: name should equal the supplied string
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    async def create_order(order_id: int) -> _OrderPlaced:
        await asyncio.sleep(0)
        return _OrderPlaced(order_id)

    # Act
    ev = AsyncEventConfig(create_order, PubSub, payload_type=_OrderPlaced, name="order_placed")

    # Assert
    assert ev.name == "order_placed"


async def test_asynceventconfig_factory_is_awaitable() -> None:
    """
    Test that the factory property returns a callable whose result can be awaited.

    Given: An AsyncEventConfig wrapping an async factory
    When: factory is called and awaited
    Then: The result should be the expected payload instance
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    async def create_order(order_id: int) -> _OrderPlaced:
        await asyncio.sleep(0)
        return _OrderPlaced(order_id)

    ev = AsyncEventConfig(create_order, PubSub, payload_type=_OrderPlaced)

    # Act
    result = await ev.factory(1)

    # Assert
    assert result.order_id == 1


def test_event_decorator_returns_asynceventconfig_for_async_factory() -> None:
    """
    Test that the event decorator returns an AsyncEventConfig when an async factory is supplied.

    Given: An async factory function and an explicit payload_type
    When: The factory is decorated with @event specifying payload_type
    Then: The result should be an AsyncEventConfig instance
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    @event(PubSub, payload_type=_OrderPlaced)
    async def create_order(order_id: int) -> _OrderPlaced:
        await asyncio.sleep(0)
        return _OrderPlaced(order_id)

    # Assert
    assert isinstance(create_order, AsyncEventConfig)
    assert create_order.payload_type is _OrderPlaced


async def test_event_decorator_async_factory_is_awaitable() -> None:
    """
    Test that an async factory decorated with @event produces an awaitable factory.

    Given: An async factory decorated with @event and an explicit payload_type
    When: factory is called and awaited
    Then: The result should be the expected payload instance
    """

    # Arrange
    class _OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    # Act
    @event(PubSub, payload_type=_OrderPlaced)
    async def create_order(order_id: int) -> _OrderPlaced:
        await asyncio.sleep(0)
        return _OrderPlaced(order_id)

    result = await create_order.factory(1)

    # Assert
    assert result.order_id == 1
