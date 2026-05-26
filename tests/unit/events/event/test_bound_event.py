"""
Unit tests for the BoundEvent class.

This test suite verifies the following behaviors:
- The event type and emitter are stored on initialization.
- Calling the bound event constructs the event with positional arguments.
- Calling the bound event constructs the event with keyword arguments.
- The return value from the emitter is returned to the caller.
"""

from typing import Any
from unittest.mock import Mock

from pytest_mock import MockerFixture

from stratae.events.event import BoundEvent, Event


class _OrderCreated(Event):
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _OrderCreated):
            return False
        return self.order_id == value.order_id and self.status == value.status


def test_init_stores_event_and_emitter():
    """
    Test that the event type and emitter are stored during initialization.

    Given: An event type and an emitter callable
    When: A BoundEvent is created
    Then: The event and emitter attributes should reference the supplied objects
    """
    # Arrange
    emitter = Mock()

    # Act
    bound: BoundEvent[[int, str], Any] = BoundEvent(_OrderCreated, emitter)

    # Assert
    assert bound.event is _OrderCreated
    assert bound.emitter is emitter


def test_call_passes_positional_args_to_event(mocker: MockerFixture):
    """
    Test that positional arguments are forwarded to the event constructor.

    Given: A BoundEvent wrapping an event that accepts positional arguments
    When: The BoundEvent is called with positional arguments
    Then: The event constructor should be called with those values as positional args
    """
    # Arrange
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    bound: BoundEvent[[int, str], Any] = BoundEvent(_OrderCreated, emitter)

    # Act
    bound(1, "pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(_OrderCreated(1, "pending"))


def test_call_passes_keyword_args_to_event(mocker: MockerFixture):
    """
    Test that keyword arguments are forwarded to the event constructor.

    Given: A BoundEvent wrapping an event that accepts keyword arguments
    When: The BoundEvent is called with keyword arguments
    Then: The event constructor should be called with those values as keyword args
    """
    # Arrange
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    bound = BoundEvent(_OrderCreated, emitter)

    # Act
    bound(order_id=2, status="complete")

    # Assert
    spy.assert_called_once_with(mocker.ANY, order_id=2, status="complete")
    emitter.assert_called_once_with(_OrderCreated(2, "complete"))


def test_call_passes_mixed_args_to_event(mocker: MockerFixture):
    """
    Test that a mix of positional and keyword arguments are forwarded to the event constructor.

    Given: A BoundEvent wrapping an event that accepts positional and keyword arguments
    When: The BoundEvent is called with one positional and one keyword argument
    Then: The event constructor should be called with args in the same positional and keyword form
    """
    # Arrange
    spy = mocker.spy(_OrderCreated, "__init__")
    emitter = Mock()
    bound = BoundEvent(_OrderCreated, emitter)

    # Act
    bound(1, status="pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, status="pending")
    emitter.assert_called_once_with(_OrderCreated(1, "pending"))


def test_call_returns_emitter_result():
    """
    Test that the return value from the emitter is returned to the caller.

    Given: A BoundEvent whose emitter returns a known value
    When: The BoundEvent is called
    Then: The return value should match the emitter's return value
    """
    # Arrange
    emitter = Mock(return_value="dispatched")
    bound: BoundEvent[[int, str], str] = BoundEvent(_OrderCreated, emitter)

    # Act
    result = bound(1, "pending")

    # Assert
    assert result == "dispatched"
