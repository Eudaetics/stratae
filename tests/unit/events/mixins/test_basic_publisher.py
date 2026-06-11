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
- Each call to publish returns a distinct BoundEvent instance.
"""

from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from stratae.events.bound import BoundEvent
from stratae.events.event import EventSchema
from stratae.events.mixins.publish import BasicPublisher


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
    When: publish is used as a decorator
    Then: A BoundEvent instance should be returned
    """

    @publisher.publish
    class _ItemShipped(EventSchema): ...

    assert isinstance(_ItemShipped, BoundEvent)


def test_basic_publish_stores_schema_emitter_and_none_config(publisher: BasicPublisher[None]):
    """
    BoundEvent returned by publish should store the schema, emit_publish, and None config.

    Given: A BasicPublisher instance with abstract methods cleared
    When: publish is used as a decorator
    Then: The BoundEvent should store an EventSchema subclass, emit_publish, and None as config
    """

    @publisher.publish
    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

    assert isinstance(_ItemShipped.schema, type)
    assert _ItemShipped.emitter == publisher.emit_publish
    assert _ItemShipped.config is None


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

    @publisher.publish
    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, type(self)):
                return False
            return self.item_id == value.item_id and self.quantity == value.quantity

    _ItemShipped(1, 10)

    mock_emit.assert_called_once_with(_ItemShipped.schema(1, 10), _ItemShipped)


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

    @publisher.publish
    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, type(self)):
                return False
            return self.item_id == value.item_id and self.quantity == value.quantity

    _ItemShipped(item_id=2, quantity=5)

    mock_emit.assert_called_once_with(_ItemShipped.schema(item_id=2, quantity=5), _ItemShipped)


def test_basic_publish_bound_event_returns_emit_publish_result(
    publisher: BasicPublisher[None], mocker: MockerFixture
):
    """
    The return value from emit_publish should propagate out of the BoundEvent call.

    Given: A BoundEvent created via the decorator
    When: The BoundEvent is called
    Then: The return value should match what emit_publish returned
    """
    mock_emit = mocker.patch.object(publisher, "emit_publish", new=Mock(return_value="dispatched"))

    @publisher.publish
    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

    result = _ItemShipped(1, 10)

    assert result == mock_emit.return_value


def test_basic_publish_returns_distinct_bound_events(publisher: BasicPublisher[None]):
    """
    Each call to publish should return a distinct BoundEvent instance.

    Given: A BasicPublisher instance with abstract methods cleared
    When: publish is called twice with the same schema
    Then: The two BoundEvents should be different objects
    """

    class _ItemShipped(EventSchema):
        def __init__(self, item_id: int, quantity: int) -> None:
            self.item_id = item_id
            self.quantity = quantity

    first = publisher.publish(_ItemShipped)
    second = publisher.publish(_ItemShipped)

    assert first is not second
    assert first.schema is second.schema
