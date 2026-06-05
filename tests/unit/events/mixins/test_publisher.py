"""
Unit tests for the Publisher mixin.

This test suite verifies the following behaviors:

Publisher:
- Publisher cannot be instantiated directly (abstract).
- publish returns a BoundEvent bound to emit_publish.
- The BoundEvent stores the correct schema, emitter, and config.
- Calling the BoundEvent constructs the event and calls emit_publish with the payload and itself.
- Calling the BoundEvent with keyword args calls emit_publish with the payload and itself.
- The return value from emit_publish is returned to the caller.
- publish without config stores None on the BoundEvent.
"""

from typing import Any
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stratae.events.event import BoundEvent, EventSchema
from stratae.events.mixins.publish import Publisher


class _ItemShipped(EventSchema):
    def __init__(self, item_id: int, quantity: int) -> None:
        self.item_id = item_id
        self.quantity = quantity

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, _ItemShipped):
            return False
        return self.item_id == value.item_id and self.quantity == value.quantity


@pytest.fixture
def publisher() -> Publisher[Any, None]:
    """Return a Publisher instance with abstract methods cleared for testing."""
    from unittest.mock import patch

    with patch.object(Publisher, "__abstractmethods__", frozenset[str]()):
        return Publisher()  # pyright: ignore[reportAbstractUsage]


def test_publish_bus_is_abstract():
    """
    Publisher should raise TypeError when instantiated directly.

    Given: The abstract Publisher class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class Publisher"):
        Publisher()  # pyright: ignore[reportAbstractUsage]


def test_publish_returns_bound_event(publisher: Publisher[Any, None]):
    """
    Publish should return a BoundEvent instance.

    Given: A Publisher instance with abstract methods cleared
    When: publish is called with a schema
    Then: A BoundEvent instance should be returned
    """
    assert isinstance(publisher.publish(_ItemShipped), BoundEvent)


def test_publish_stores_schema_emitter_and_config(publisher: Publisher[Any, None]):
    """
    BoundEvent returned by publish should store the schema, emit_publish, and config.

    Given: A Publisher instance with abstract methods cleared
    When: publish is called with a schema and config
    Then: The BoundEvent should store that schema, emit_publish, and config
    """
    config = object()

    bound = publisher.publish(_ItemShipped, config=config)

    assert bound.schema is _ItemShipped
    assert bound.emitter == publisher.emit_publish
    assert bound.config is config


def test_publish_bound_event_calls_emit_publish_with_positional_args(
    publisher: Publisher[Any, None], mocker: MockerFixture
):
    """
    BoundEvent called with positional args should construct the event and call emit_publish.

    Given: A BoundEvent returned by publish
    When: The BoundEvent is called with positional arguments
    Then: emit_publish should be called with the constructed payload and the BoundEvent itself
    """
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock())
    bound = publisher.publish(_ItemShipped)

    bound(1, 10)

    mock_emit.assert_called_once_with(_ItemShipped(1, 10), bound)


def test_publish_bound_event_calls_emit_publish_with_keyword_args(
    publisher: Publisher[Any, None], mocker: MockerFixture
):
    """
    BoundEvent called with keyword args should construct the event and call emit_publish.

    Given: A BoundEvent returned by publish
    When: The BoundEvent is called with keyword arguments
    Then: emit_publish should be called with the constructed payload and the BoundEvent itself
    """
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock())
    bound = publisher.publish(_ItemShipped)

    bound(item_id=2, quantity=5)

    mock_emit.assert_called_once_with(_ItemShipped(2, 5), bound)


def test_publish_bound_event_returns_emit_publish_result(publisher: Publisher[Any, None]):
    """
    Return value from emit_publish should be returned to the caller.

    Given: A BoundEvent returned by publish whose emit_publish returns a known value
    When: The BoundEvent is called
    Then: The return value should match what emit_publish returned
    """
    mock_emit = Mock(return_value="dispatched")
    publisher.emit_publish = mock_emit  # pyright: ignore[reportAttributeAccessIssue]
    bound = publisher.publish(_ItemShipped)

    result = bound(1, 10)

    assert result == "dispatched"


def test_publish_without_config_stores_none(publisher: Publisher[Any, None]):
    """
    Publish called without config should return a BoundEvent with config set to None.

    Given: A Publisher instance with abstract methods cleared
    When: publish is called with only a schema
    Then: A BoundEvent should be returned with config set to None
    """
    bound = publisher.publish(_ItemShipped)

    assert isinstance(bound, BoundEvent)
    assert bound.config is None
