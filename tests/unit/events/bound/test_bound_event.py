"""
Unit tests for the BoundEvent class.

This test suite verifies the following behaviors:
- The EventConfig, emitter, and config are stored on initialization.
- Calling the bound event constructs the payload with positional arguments.
- Calling the bound event constructs the payload with keyword arguments.
- Calling the bound event with mixed positional and keyword arguments forwards them correctly.
- The return value from the emitter is returned to the caller.
"""

import asyncio
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stratae.events.bound import BoundEvent
from stratae.events.event import EventConfig, PubSub


class _OrderCreated:
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, _OrderCreated):
            return False
        return self.order_id == value.order_id and self.status == value.status


_order_created = EventConfig(_OrderCreated, PubSub)


def test_init_stores_event_emitter_and_config():
    """
    Test that the EventConfig, emitter, and config are stored during initialization.

    Given: An EventConfig, an emitter callable, and a config object
    When: A BoundEvent is created
    Then: The event, emitter, and config attributes should reference the supplied objects
    """
    emitter = Mock()
    config = object()

    bound = BoundEvent(emitter, _order_created, config=config)

    assert bound.event is _order_created
    assert bound.emitter is emitter
    assert bound.config is config


def test_call_passes_positional_args_to_factory(mocker: MockerFixture):
    """
    Test that positional arguments are forwarded to the factory.

    Given: A BoundEvent wrapping an EventConfig whose factory accepts positional arguments
    When: The BoundEvent is called with positional arguments
    Then: The factory should be called with those values and the emitter
          should receive the constructed payload, the EventConfig, and the config
    """
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    bound = BoundEvent(emitter, _order_created, config=None)

    bound(1, "pending")

    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(_OrderCreated(1, "pending"), _order_created, None)


def test_call_passes_keyword_args_to_factory(mocker: MockerFixture):
    """
    Test that keyword arguments are forwarded to the factory.

    Given: A BoundEvent wrapping an EventConfig whose factory accepts keyword arguments
    When: The BoundEvent is called with keyword arguments
    Then: The factory should be called with those values and the emitter
          should receive the constructed payload, the EventConfig, and the config
    """
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    bound = BoundEvent(emitter, _order_created, config=None)

    bound(order_id=2, status="complete")

    spy.assert_called_once_with(mocker.ANY, order_id=2, status="complete")
    emitter.assert_called_once_with(_OrderCreated(2, "complete"), _order_created, None)


def test_call_passes_mixed_args_to_factory(mocker: MockerFixture):
    """
    Test that a mix of positional and keyword arguments are forwarded to the factory.

    Given: A BoundEvent wrapping an EventConfig whose factory accepts positional and keyword args
    When: The BoundEvent is called with one positional and one keyword argument
    Then: The factory should be called with args in the same form and the emitter
          should receive the constructed payload, the EventConfig, and the config
    """
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    bound = BoundEvent(emitter, _order_created, config=None)

    bound(1, status="pending")

    spy.assert_called_once_with(mocker.ANY, 1, status="pending")
    emitter.assert_called_once_with(_OrderCreated(1, "pending"), _order_created, None)


def test_call_returns_emitter_result():
    """
    Test that the return value from the emitter is returned to the caller.

    Given: A BoundEvent whose emitter returns a known value
    When: The BoundEvent is called
    Then: The return value should match the emitter's return value
    """
    emitter = Mock(return_value="dispatched")
    bound = BoundEvent(emitter, _order_created, config=None)

    result = bound(1, "pending")

    assert result == "dispatched"


def test_init_raises_for_async_factory():
    """
    Test that BoundEvent raises TypeError when its factory is a coroutine function.

    Given: An EventConfig whose factory is async
    When: A BoundEvent is constructed with that EventConfig
    Then: A TypeError should be raised
    """

    async def _async_order_created(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    ev = EventConfig(_async_order_created, PubSub, payload_type=_OrderCreated)

    with pytest.raises(TypeError):
        BoundEvent(Mock(), ev, config=None)
