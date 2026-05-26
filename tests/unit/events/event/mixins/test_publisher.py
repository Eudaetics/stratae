"""
Unit tests for the Publisher mixin.

This test suite verifies the following behaviors:

Publisher:
- publish returns a BoundEvent bound to emit_publish.
- The BoundEvent stores the correct event class and emitter.
- Publisher cannot be instantiated directly (abstract).
- Calling the BoundEvent constructs the event and calls emit_publish.
- The return value from emit_publish is returned to the caller.

"""

from typing import Any
from unittest.mock import Mock, patch

import pytest
from pytest_mock import MockerFixture

from stratae.events.event import BoundEvent, Event
from stratae.events.mixins.publish import Publisher


class _ItemShipped(Event):
    def __init__(self, item_id: int, quantity: int) -> None:
        self.item_id = item_id
        self.quantity = quantity

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _ItemShipped):
            return False
        return self.item_id == value.item_id and self.quantity == value.quantity


@pytest.fixture
def publisher() -> Publisher[None]:
    """Yield a Publisher instance with abstract methods cleared for testing."""
    with patch.object(Publisher, "__abstractmethods__", frozenset[str]()):
        return Publisher()  # pyright: ignore[reportAbstractUsage]


def test_publish_bus_is_abstract():
    """
    Publisher should raise TypeError when instantiated directly.

    Given: The abstract Publisher class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError):
        Publisher()  # pyright: ignore[reportAbstractUsage]


def test_publish_returns_bound_event(publisher: Publisher[None]):
    """
    Publish should return a BoundEvent instance.

    Given: A Publisher instance with abstract methods cleared
    When: publish is called with an event class
    Then: A BoundEvent instance should be returned
    """
    # Act
    bound = publisher.publish(_ItemShipped)

    # Assert
    assert isinstance(bound, BoundEvent)


def test_publish_bound_event_stores_event_and_emitter(publisher: Publisher[None]):
    """
    BoundEvent returned by publish should store the event class and emit_publish.

    Given: A Publisher instance with abstract methods cleared
    When: publish is called with an event class
    Then: The BoundEvent should store that event class and emit_publish as the emitter
    """
    # Act
    bound = publisher.publish(_ItemShipped)

    # Assert
    assert bound.event is _ItemShipped
    assert bound.emitter == publisher.emit_publish


def test_publish_bound_event_calls_emit_publish_with_positional_args(
    publisher: Publisher[None], mocker: MockerFixture
):
    """
    BoundEvent called with positional args should construct the event and call emit_publish.

    Given: A BoundEvent returned by publish
    When: The BoundEvent is called with positional arguments
    Then: emit_publish should be called with the constructed event
    """
    # Arrange
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock())
    bound = publisher.publish(_ItemShipped)

    # Act
    bound(1, 10)

    # Assert
    mock_emit.assert_called_once_with(_ItemShipped(1, 10))


def test_publish_bound_event_calls_emit_publish_with_keyword_args(
    publisher: Publisher[None], mocker: MockerFixture
):
    """
    BoundEvent called with keyword args should construct the event and call emit_publish.

    Given: A BoundEvent returned by publish
    When: The BoundEvent is called with keyword arguments
    Then: emit_publish should be called with the constructed event
    """
    # Arrange
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock())
    bound = publisher.publish(_ItemShipped)

    # Act
    bound(item_id=2, quantity=5)

    # Assert
    mock_emit.assert_called_once_with(_ItemShipped(2, 5))


def test_publish_bound_event_returns_emit_publish_result(publisher: Publisher[None]):
    """
    Return value from emit_publish should be returned to the caller.

    Given: A BoundEvent returned by publish whose emit_publish returns a known value
    When: The BoundEvent is called
    Then: The return value should match what emit_publish returned
    """
    # Arrange
    mock_emit = Mock(return_value="dispatched")
    publisher.emit_publish = mock_emit  # type: ignore[method-assign]
    bound = publisher.publish(_ItemShipped)

    # Act
    result = bound(1, 10)

    # Assert
    assert result == "dispatched"
