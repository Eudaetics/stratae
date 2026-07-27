"""
Unit tests for the FactoryBoundEvent class.

This test suite verifies the following behaviors:
- The Event, factory, emitter, and config are stored on initialization.
- Calling the bound event constructs the payload with positional arguments.
- Calling the bound event constructs the payload with keyword arguments.
- Calling the bound event with mixed positional and keyword arguments forwards them correctly.
- The return value from the emitter is returned to the caller.
"""

import asyncio
from unittest.mock import Mock, create_autospec

import pytest
from pytest_mock import MockerFixture

from stratae.events import EmitCallable, Event, FactoryBoundEvent, PubSub


class _OrderCreated:
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, _OrderCreated):
            return False
        return self.order_id == value.order_id and self.status == value.status


_order_created = Event(_OrderCreated, PubSub)


def test_init_stores_event_emitter_and_config():
    """
    Test that the Event, emitter, and config are stored during initialization.

    Given: An Event, an emitter callable, and a config object
    When: A FactoryBoundEvent is created
    Then: The event, emitter, and config attributes should reference the supplied objects
    """
    emitter = create_autospec(EmitCallable)
    config = object()

    bound = FactoryBoundEvent(emitter, _order_created, factory=_OrderCreated, config=config)

    assert bound.event is _order_created
    assert bound.emitter is emitter
    assert bound.config is config


def test_init_defaults_serializer_to_none():
    """
    Test that serializer defaults to None when not supplied.

    Given: No serializer argument
    When: A FactoryBoundEvent is created
    Then: The serializer attribute should be None
    """
    emitter = create_autospec(EmitCallable)

    bound = FactoryBoundEvent(emitter, _order_created, factory=_OrderCreated, config=None)

    assert bound.serializer is None


def test_init_stores_serializer():
    """
    Test that a supplied serializer is stored during initialization.

    Given: A serializer callable
    When: A FactoryBoundEvent is created with that serializer
    Then: The serializer attribute should reference the supplied callable
    """
    emitter = create_autospec(EmitCallable)
    serializer = Mock()

    bound = FactoryBoundEvent(
        emitter, _order_created, factory=_OrderCreated, config=None, serializer=serializer
    )

    assert bound.serializer is serializer


def test_call_passes_positional_args_to_factory(
    mocker: MockerFixture,
):
    """
    Test that positional arguments are forwarded to the factory.

    Given: A FactoryBoundEvent whose factory accepts positional arguments
    When: The FactoryBoundEvent is called with positional arguments
    Then: The factory should be called with those values and the emitter
          should receive the constructed payload, the Event, and the config
    """
    emitter = create_autospec(EmitCallable)
    spy = mocker.spy(_OrderCreated, "__init__")
    bound = FactoryBoundEvent(emitter, _order_created, factory=_OrderCreated, config=None)

    bound(1, "pending")

    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(
        _OrderCreated(1, "pending"), _order_created, None, serializer=None
    )


def test_call_passes_keyword_args_to_factory(
    mocker: MockerFixture,
):
    """
    Test that keyword arguments are forwarded to the factory.

    Given: A FactoryBoundEvent whose factory accepts keyword arguments
    When: The FactoryBoundEvent is called with keyword arguments
    Then: The factory should be called with those values and the emitter
          should receive the constructed payload, the Event, and the config
    """
    emitter = create_autospec(EmitCallable)
    spy = mocker.spy(_OrderCreated, "__init__")
    bound = FactoryBoundEvent(emitter, _order_created, factory=_OrderCreated, config=None)

    bound(order_id=2, status="complete")

    spy.assert_called_once_with(mocker.ANY, order_id=2, status="complete")
    emitter.assert_called_once_with(
        _OrderCreated(2, "complete"), _order_created, None, serializer=None
    )


def test_call_passes_mixed_args_to_factory(
    mocker: MockerFixture,
):
    """
    Test that a mix of positional and keyword arguments are forwarded to the factory.

    Given: A FactoryBoundEvent whose factory accepts positional and keyword args
    When: The FactoryBoundEvent is called with one positional and one keyword argument
    Then: The factory should be called with args in the same form and the emitter
          should receive the constructed payload, the Event, and the config
    """
    emitter = create_autospec(EmitCallable)
    spy = mocker.spy(_OrderCreated, "__init__")
    bound = FactoryBoundEvent(emitter, _order_created, factory=_OrderCreated, config=None)

    bound(1, status="pending")

    spy.assert_called_once_with(mocker.ANY, 1, status="pending")
    emitter.assert_called_once_with(
        _OrderCreated(1, "pending"), _order_created, None, serializer=None
    )


def test_call_returns_emitter_result():
    """
    Test that the return value from the emitter is returned to the caller.

    Given: A FactoryBoundEvent whose emitter returns the constructed payload
    When: The FactoryBoundEvent is called
    Then: The return value should match the constructed payload
    """
    emitter = create_autospec(EmitCallable)

    def _return(
        payload: object, event: object, config: object, serializer: object = None
    ) -> object:
        return payload

    emitter.side_effect = _return
    bound = FactoryBoundEvent(emitter, _order_created, factory=_OrderCreated, config=None)

    result = bound(1, "pending")

    assert result == _OrderCreated(1, "pending")


def test_call_forwards_serializer_to_emitter():
    """
    Test that the bound serializer is forwarded to the emitter when called.

    Given: A FactoryBoundEvent constructed with a serializer
    When: The FactoryBoundEvent is called
    Then: The emitter should receive that same serializer
    """
    emitter = create_autospec(EmitCallable)
    serializer = Mock()
    bound = FactoryBoundEvent(
        emitter, _order_created, factory=_OrderCreated, config=None, serializer=serializer
    )

    bound(1, "pending")

    emitter.assert_called_once_with(
        _OrderCreated(1, "pending"), _order_created, None, serializer=serializer
    )


def test_init_raises_for_async_factory():
    """
    Test that FactoryBoundEvent raises TypeError when its factory is a coroutine function.

    Given: An async factory
    When: A FactoryBoundEvent is constructed with that factory
    Then: A TypeError should be raised
    """
    emitter = create_autospec(EmitCallable)

    async def _async_order_created(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    with pytest.raises(TypeError):
        FactoryBoundEvent(emitter, _order_created, factory=_async_order_created, config=None)


def test_call_with_zero_arg_factory_needs_no_payload():
    """
    Test that a zero-arg factory lets the bound event be called with no arguments.

    Given: A FactoryBoundEvent whose factory takes no arguments (a kickstart-style event
           carrying no payload data)
    When: The FactoryBoundEvent is called with no arguments
    Then: The factory should be invoked with no arguments and the emitter should receive
          the constructed payload
    """

    # Arrange
    class _JobStarted:
        pass

    job_started = Event(_JobStarted, PubSub)
    emitter = create_autospec(EmitCallable)
    bound = FactoryBoundEvent(emitter, job_started, factory=_JobStarted, config=None)

    # Act
    bound()

    # Assert
    emitter.assert_called_once()
    payload = emitter.call_args.args[0]
    assert isinstance(payload, _JobStarted)
