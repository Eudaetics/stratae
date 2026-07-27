"""
Unit tests for the BoundEvent class (the no-factory passthrough binding).

This test suite verifies the following behaviors:
- The Event, emitter, and config are stored on initialization.
- Calling the bound event forwards an already-built payload to the emitter.
- The return value from the emitter is returned to the caller.
"""

from unittest.mock import Mock, create_autospec

from stratae.events import BoundEvent, EmitCallable, Event, PubSub


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
    When: A BoundEvent is created
    Then: The event, emitter, and config attributes should reference the supplied objects
    """
    emitter = create_autospec(EmitCallable)
    config = object()

    bound = BoundEvent(emitter, _order_created, config=config)

    assert bound.event is _order_created
    assert bound.emitter is emitter
    assert bound.config is config


def test_init_defaults_serializer_to_none():
    """
    Test that serializer defaults to None when not supplied.

    Given: No serializer argument
    When: A BoundEvent is created
    Then: The serializer attribute should be None
    """
    emitter = create_autospec(EmitCallable)

    bound = BoundEvent(emitter, _order_created, config=None)

    assert bound.serializer is None


def test_init_stores_serializer():
    """
    Test that a supplied serializer is stored during initialization.

    Given: A serializer callable
    When: A BoundEvent is created with that serializer
    Then: The serializer attribute should reference the supplied callable
    """
    emitter = create_autospec(EmitCallable)
    serializer = Mock()

    bound = BoundEvent(emitter, _order_created, config=None, serializer=serializer)

    assert bound.serializer is serializer


def test_call_forwards_payload_to_emitter():
    """
    Test that calling the bound event forwards the payload unchanged.

    Given: A BoundEvent with no factory
    When: The BoundEvent is called with an already-built payload
    Then: The emitter should receive that exact payload, the Event, and the config
    """
    emitter = create_autospec(EmitCallable)
    bound = BoundEvent(emitter, _order_created, config=None)
    payload = _OrderCreated(1, "pending")

    bound(payload)

    emitter.assert_called_once_with(payload, _order_created, None, serializer=None)


def test_call_returns_emitter_result():
    """
    Test that the return value from the emitter is returned to the caller.

    Given: A BoundEvent whose emitter returns the payload
    When: The BoundEvent is called
    Then: The return value should match the payload
    """
    emitter = create_autospec(EmitCallable)

    def _return(
        payload: object, event: object, config: object, serializer: object = None
    ) -> object:
        return payload

    emitter.side_effect = _return
    bound = BoundEvent(emitter, _order_created, config=None)
    payload = _OrderCreated(1, "pending")

    result = bound(payload)

    assert result is payload


def test_call_forwards_serializer_to_emitter():
    """
    Test that the bound serializer is forwarded to the emitter when called.

    Given: A BoundEvent constructed with a serializer
    When: The BoundEvent is called
    Then: The emitter should receive that same serializer
    """
    emitter = create_autospec(EmitCallable)
    serializer = Mock()
    bound = BoundEvent(emitter, _order_created, config=None, serializer=serializer)
    payload = _OrderCreated(1, "pending")

    bound(payload)

    emitter.assert_called_once_with(payload, _order_created, None, serializer=serializer)
