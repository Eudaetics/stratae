"""
Unit tests for the BoundEvent class.

This test suite verifies the following behaviors:
- The schema, emitter, and config are stored on initialization.
- Calling the bound event constructs the event with positional arguments.
- Calling the bound event constructs the event with keyword arguments.
- Calling the bound event with mixed positional and keyword arguments forwards them correctly.
- The return value from the emitter is returned to the caller.
"""

from typing import Any
from unittest.mock import Mock

from pytest_mock import MockerFixture

from stratae.events.event import BoundEvent, EventSchema


class _OrderCreated(EventSchema):
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _OrderCreated):
            return False
        return self.order_id == value.order_id and self.status == value.status


def test_init_stores_schema_emitter_and_config():
    """
    Test that the schema, emitter, and config are stored during initialization.

    Given: A schema, an emitter callable, and a config object
    When: A BoundEvent is created
    Then: The schema, emitter, and config attributes should reference the supplied objects
    """
    emitter = Mock()
    config = object()

    bound: BoundEvent[[int, str], Any, Any] = BoundEvent(_OrderCreated, emitter, config=config)

    assert bound.schema is _OrderCreated
    assert bound.emitter is emitter
    assert bound.config is config


def test_call_passes_positional_args_to_schema(mocker: MockerFixture):
    """
    Test that positional arguments are forwarded to the schema constructor.

    Given: A BoundEvent wrapping a schema that accepts positional arguments
    When: The BoundEvent is called with positional arguments
    Then: The schema constructor should be called with those values and the emitter
          should receive the constructed payload and the BoundEvent itself
    """
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    bound: BoundEvent[[int, str], Any, Any] = BoundEvent(_OrderCreated, emitter)

    bound(1, "pending")

    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(_OrderCreated(1, "pending"), bound)


def test_call_passes_keyword_args_to_schema(mocker: MockerFixture):
    """
    Test that keyword arguments are forwarded to the schema constructor.

    Given: A BoundEvent wrapping a schema that accepts keyword arguments
    When: The BoundEvent is called with keyword arguments
    Then: The schema constructor should be called with those values and the emitter
          should receive the constructed payload and the BoundEvent itself
    """
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    bound = BoundEvent(_OrderCreated, emitter)

    bound(order_id=2, status="complete")

    spy.assert_called_once_with(mocker.ANY, order_id=2, status="complete")
    emitter.assert_called_once_with(_OrderCreated(2, "complete"), bound)


def test_call_passes_mixed_args_to_schema(mocker: MockerFixture):
    """
    Test that a mix of positional and keyword arguments are forwarded to the schema constructor.

    Given: A BoundEvent wrapping a schema that accepts positional and keyword arguments
    When: The BoundEvent is called with one positional and one keyword argument
    Then: The schema constructor should be called with args in the same form and the emitter
          should receive the constructed payload and the BoundEvent itself
    """
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    bound = BoundEvent(_OrderCreated, emitter)

    bound(1, status="pending")

    spy.assert_called_once_with(mocker.ANY, 1, status="pending")
    emitter.assert_called_once_with(_OrderCreated(1, "pending"), bound)


def test_call_returns_emitter_result():
    """
    Test that the return value from the emitter is returned to the caller.

    Given: A BoundEvent whose emitter returns a known value
    When: The BoundEvent is called
    Then: The return value should match the emitter's return value
    """
    emitter = Mock(return_value="dispatched")
    bound: BoundEvent[[int, str], Any, str] = BoundEvent(_OrderCreated, emitter)

    result = bound(1, "pending")

    assert result == "dispatched"
