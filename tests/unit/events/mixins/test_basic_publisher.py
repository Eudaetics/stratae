"""
Unit tests for the BasicPublisher mixin.

This test suite verifies the following behaviors:

BasicPublisher:
- BasicPublisher cannot be instantiated directly (abstract).
- publish returns a BoundEvent bound to emit_publish.
- The BoundEvent stores the correct schema, emitter, and None config.
- Calling the BoundEvent constructs the event and calls emit_publish with the payload and itself.
- Calling the BoundEvent with keyword args calls emit_publish with the payload and itself.
- The return value from emit_publish is returned to the caller.
- publish stores None on the BoundEvent config.
"""

from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stratae.events.event import BoundEvent, EventSchema
from stratae.events.mixins.publish import BasicPublisher


class _ItemShipped(EventSchema):
    def __init__(self, item_id: int, quantity: int) -> None:
        self.item_id = item_id
        self.quantity = quantity

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, _ItemShipped):
            return False
        return self.item_id == value.item_id and self.quantity == value.quantity


@pytest.fixture
def publisher() -> BasicPublisher[None]:
    """Return a BasicPublisher instance with abstract methods cleared for testing."""
    from unittest.mock import patch

    with patch.object(BasicPublisher, "__abstractmethods__", frozenset[str]()):
        return BasicPublisher()  # pyright: ignore[reportAbstractUsage]


def test_basic_publish_bus_is_abstract():
    """
    BasicPublisher should raise TypeError when instantiated directly.

    Given: The abstract BasicPublisher class
    When: An attempt is made to instantiate it
    Then: A TypeError should be raised
    """
    with pytest.raises(TypeError, match="Can't instantiate abstract class BasicPublisher"):
        BasicPublisher()  # pyright: ignore[reportAbstractUsage]


def test_basic_publish_returns_bound_event(publisher: BasicPublisher[None]):
    """
    Publish should return a BoundEvent instance.

    Given: A BasicPublisher instance with abstract methods cleared
    When: publish is called with a schema
    Then: A BoundEvent instance should be returned
    """
    assert isinstance(publisher.publish(_ItemShipped), BoundEvent)


def test_basic_publish_stores_schema_emitter_and_none_config(publisher: BasicPublisher[None]):
    """
    BoundEvent returned by publish should store the schema, emit_publish, and None config.

    Given: A BasicPublisher instance with abstract methods cleared
    When: publish is called with a schema
    Then: The BoundEvent should store that schema, emit_publish, and None as config
    """
    bound = publisher.publish(_ItemShipped)

    assert bound.schema is _ItemShipped
    assert bound.emitter == publisher.emit_publish
    assert bound.config is None


def test_basic_publish_bound_event_calls_emit_publish_with_positional_args(
    publisher: BasicPublisher[None], mocker: MockerFixture
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


def test_basic_publish_bound_event_calls_emit_publish_with_keyword_args(
    publisher: BasicPublisher[None], mocker: MockerFixture
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


def test_basic_publish_bound_event_returns_emit_publish_result(publisher: BasicPublisher[None]):
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
