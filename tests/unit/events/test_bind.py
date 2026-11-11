"""
Unit tests for bind.

This test suite verifies the following behaviors:

bind with a factory:
- Calling the returned callable constructs the payload via the factory and invokes the emitter.
- Calling the returned callable returns the emitter's result.
- The serializer is forwarded to the emitter.
- Raises TypeError when the factory is async.

bind without a factory:
- Calling the returned callable forwards an already-built payload to the emitter.
- Calling the returned callable returns the emitter's result.
- The serializer is forwarded to the emitter.

bind with hooks:
- The hooks are forwarded to the emitter.
- A serializer and hooks given together are both forwarded.
- An emitter without a hooks parameter works without hooks, and refuses them loudly.
- An emitter without a serializer parameter works without one, and refuses one loudly.

bound callable signatures:
- Without a factory, the signature is a payload of the event's schema, returning its reply
  type, and get_type_hints reads the same types.
- A pub/sub event's bound callable returns None.
- A payload-less event's bound callable takes no parameters, and still emits NoPayload.
- With a factory, the signature has the factory's parameters and the event's reply type.
- A factory's string annotations are resolved; one that can't be resolved is kept as written.
- DirectBus.bind gives the same typed signature.
"""

import asyncio
import inspect
from typing import Any, cast, get_type_hints
from unittest.mock import Mock, create_autospec

import pytest
from pytest_mock import MockerFixture

from stratae.events import DirectBus, EmitCallable, Hooks, NoPayload, PubSub, Request, bind


class _OrderCreated:
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderCreated):
            return False
        return self.order_id == other.order_id and self.status == other.status


# region: Bind With Factory


def test_bind_with_factory_calling_constructs_schema_and_invokes_emitter(
    mocker: MockerFixture,
) -> None:
    """
    Calling a factory-bound callable produced by bind constructs the schema and calls the emitter.

    Given: A factory-bound callable produced by bind
    When: The callable is called with arguments
    Then: The schema constructor should be called with those arguments and the emitter
          should receive the event, the config, and the constructed payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    spy = mocker.spy(_OrderCreated, "__init__")
    ev = PubSub(_OrderCreated)
    bound = bind(ev, emitter, factory=_OrderCreated, config=None)

    # Act
    bound(1, "pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(ev, None, _OrderCreated(1, "pending"))


def test_bind_with_factory_returns_emitter_result() -> None:
    """
    Calling a factory-bound callable produced by bind returns the emitter's result.

    Given: A factory-bound callable whose emitter echoes the constructed payload
    When: The callable is called
    Then: The return value should match the constructed payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)

    def _return(
        event: object,
        config: object,
        payload: object,
        serializer: object = None,
        hooks: object = None,
    ) -> object:
        return payload

    emitter.side_effect = _return

    ev = PubSub(_OrderCreated)
    bound = bind(ev, emitter, factory=_OrderCreated, config=None)

    # Act
    result = bound(1, "pending")

    # Assert
    assert result == _OrderCreated(1, "pending")


def test_bind_with_factory_forwards_serializer_to_emitter() -> None:
    """
    Calling a factory-bound callable produced by bind forwards the serializer to the emitter.

    Given: A factory-bound callable produced by bind with a serializer
    When: The callable is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = PubSub(_OrderCreated)
    serializer = Mock()
    bound = bind(ev, emitter, factory=_OrderCreated, config=None, serializer=serializer)

    # Act
    bound(1, "pending")

    # Assert
    emitter.assert_called_once_with(ev, None, _OrderCreated(1, "pending"), serializer=serializer)


def test_bind_with_factory_raises_for_async_factory() -> None:
    """
    Bind raises TypeError when the supplied factory is async.

    Given: An async factory
    When: bind is called with that factory
    Then: A TypeError should be raised
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = PubSub(_OrderCreated)

    async def _async_factory(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    # Act / Assert
    with pytest.raises(TypeError):
        bind(ev, emitter, factory=_async_factory, config=None)  # pyright: ignore[reportCallIssue, reportArgumentType]


# endregion

# region: Bind Without Factory


def test_bind_without_factory_calling_forwards_payload_to_emitter() -> None:
    """
    Calling a passthrough callable produced by bind forwards an already-built payload.

    Given: A passthrough callable produced by bind with no factory
    When: The callable is called with a ready-made payload
    Then: The emitter should receive the event, the config, and that exact payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = PubSub(_OrderCreated)
    bound = bind(ev, emitter, config=None)
    payload = _OrderCreated(1, "pending")

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(ev, None, payload)


def test_bind_without_factory_returns_emitter_result() -> None:
    """
    Calling a passthrough callable produced by bind returns the emitter's result.

    Given: A passthrough callable whose emitter echoes the payload
    When: The callable is called
    Then: The return value should match the payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)

    def _return(
        event: object,
        config: object,
        payload: object,
        serializer: object = None,
        hooks: object = None,
    ) -> object:
        return payload

    emitter.side_effect = _return

    ev = PubSub(_OrderCreated)
    bound = bind(ev, emitter, config=None)
    payload = _OrderCreated(1, "pending")

    # Act
    result = bound(payload)

    # Assert
    assert result is payload


def test_bind_without_factory_forwards_serializer_to_emitter() -> None:
    """
    Calling a passthrough callable produced by bind forwards the serializer to the emitter.

    Given: A passthrough callable produced by bind with a serializer
    When: The callable is called
    Then: The emitter should receive that same serializer
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = PubSub(_OrderCreated)
    serializer = Mock()
    bound = bind(ev, emitter, config=None, serializer=serializer)
    payload = _OrderCreated(1, "pending")

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(ev, None, payload, serializer=serializer)


# endregion


def test_bind_with_factory_raises_for_async_callable_factory() -> None:
    """
    Bind raises TypeError when the factory is an object with async __call__.

    Given: An instance of a class whose __call__ is async
    When: bind is called with it as the factory
    Then: A TypeError should be raised
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = PubSub(_OrderCreated)

    class Factory:
        async def __call__(self, order_id: int, status: str) -> _OrderCreated:
            return _OrderCreated(order_id, status)

    # Act / Assert
    with pytest.raises(TypeError):
        bind(ev, emitter, factory=Factory(), config=None)  # pyright: ignore[reportCallIssue, reportArgumentType]


def test_bind_forwards_hooks_to_emitter() -> None:
    """
    Hooks given to bind should be forwarded to the emitter with every call.

    Given: A callable produced by bind with hooks
    When: The callable is called with a payload
    Then: The emitter should receive the same hooks
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    hooks: Hooks[Any, Any] = Hooks()
    ev = PubSub(_OrderCreated)
    bound = bind(ev, emitter, config=None, hooks=hooks)
    payload = _OrderCreated(1, "pending")

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(ev, None, payload, hooks=hooks)


def test_emitter_without_hooks_parameter() -> None:
    """
    An emitter that doesn't take hooks should work until hooks are bound with it.

    Given: An emitter with no hooks parameter
    When: It's bound without hooks and called, then bound with hooks and called
    Then: The first call reaches the emitter; the second raises TypeError
    """
    # Arrange
    received: list[object] = []

    def emitter(event: object, config: object, payload: object, serializer: object = None) -> None:
        received.append(payload)

    # An untyped adapter: typed ones accept hooks, as EmitCallable declares.
    untyped_emitter = cast(Any, emitter)
    ev = PubSub(_OrderCreated)
    payload = _OrderCreated(1, "pending")
    plain = bind(ev, untyped_emitter, config=None)
    hooked = bind(ev, untyped_emitter, config=None, hooks=Hooks())

    # Act / Assert
    plain(payload)
    assert received == [payload]
    with pytest.raises(TypeError, match="hooks"):
        hooked(payload)


def test_bind_forwards_serializer_and_hooks_together() -> None:
    """
    A callable bound with both a serializer and hooks should pass both to the emitter.

    Given: A callable bound with a serializer and hooks
    When: It's called
    Then: The emitter receives the payload, the serializer and the hooks
    """
    # Arrange
    emitter = Mock()
    hooks: Hooks[_OrderCreated, None] = Hooks()
    ev = PubSub(_OrderCreated)
    payload = _OrderCreated(1, "pending")
    bound = bind(ev, emitter, config=None, serializer=repr, hooks=hooks)

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(ev, None, payload, serializer=repr, hooks=hooks)


def test_emitter_without_serializer_parameter() -> None:
    """
    An emitter that doesn't take a serializer should work until one is bound with it.

    Given: An emitter with no serializer parameter
    When: It's bound without a serializer and called, then bound with one and called
    Then: The first call reaches the emitter; the second raises TypeError
    """
    # Arrange
    received: list[object] = []

    def emitter(event: object, config: object, payload: object) -> None:
        received.append(payload)

    # An untyped adapter: typed ones accept a serializer, as EmitCallable declares.
    untyped_emitter = cast(Any, emitter)
    ev = PubSub(_OrderCreated)
    payload = _OrderCreated(1, "pending")
    plain = bind(ev, untyped_emitter, config=None)
    serialized = bind(ev, untyped_emitter, config=None, serializer=repr)

    # Act / Assert
    plain(payload)
    assert received == [payload]
    with pytest.raises(TypeError, match="serializer"):
        serialized(payload)


# region: Bound Callable Signatures


class _Order:
    def __init__(self, sku: str) -> None:
        self.sku = sku


class _Receipt:
    pass


_place_order = Request(_Order, reply=_Receipt)


def test_bound_signature_without_factory_is_schema_to_reply() -> None:
    """
    A bound callable without a factory should report the event's schema and reply type.

    Given: A request event with a schema and a reply type
    When: It's bound without a factory
    Then: Its signature is (payload: schema) -> reply, and get_type_hints reads the same
    """
    # Act
    bound = bind(_place_order, create_autospec(EmitCallable), config=None)

    # Assert
    assert inspect.signature(bound) == inspect.Signature(
        [inspect.Parameter("payload", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=_Order)],
        return_annotation=_Receipt,
    )
    assert get_type_hints(bound) == {"payload": _Order, "return": _Receipt}


def test_bound_pubsub_returns_none() -> None:
    """
    A bound pub/sub event should report that it returns None.

    Given: A pub/sub event
    When: It's bound without a factory
    Then: Its return annotation is None, not NoneType
    """
    # Act
    bound = bind(PubSub(_Order), create_autospec(EmitCallable), config=None)

    # Assert
    assert inspect.signature(bound).return_annotation is None


def test_bound_signal_takes_no_parameters() -> None:
    """
    A payload-less event's bound callable should take no parameters.

    Given: A payload-less request event with a reply type
    When: It's bound and called
    Then: Its signature is () -> reply, and the emitter receives a NoPayload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    event = Request(name="ping", reply=_Receipt)
    bound = bind(event, emitter, config=None)

    # Act
    bound()

    # Assert
    assert inspect.signature(bound) == inspect.Signature([], return_annotation=_Receipt)
    assert isinstance(emitter.call_args.args[2], NoPayload)


def test_bound_signature_with_factory_has_factory_parameters() -> None:
    """
    A bound callable with a factory should report the factory's parameters.

    Given: A request event, and a factory taking a keyword-only quantity
    When: It's bound with the factory
    Then: Its signature has the factory's parameters and the event's reply type
    """

    # Arrange
    def order(sku: str, *, quantity: int = 1) -> _Order:
        return _Order(sku * quantity)

    # Act
    bound = bind(_place_order, create_autospec(EmitCallable), factory=order, config=None)

    # Assert
    assert inspect.signature(bound) == inspect.signature(order).replace(return_annotation=_Receipt)
    assert get_type_hints(bound) == {"sku": str, "quantity": int, "return": _Receipt}


def test_bound_factory_string_annotations_resolved() -> None:
    """
    A factory's string annotations should be resolved where they can be.

    Given: Factories annotated with strings, one naming a known class and one an unknown one
    When: Each is bound
    Then: The known annotation is resolved to its class; the unknown one is kept as a string
    """

    # Arrange
    def known(sku: "str") -> _Order:
        return _Order(sku)

    def unknown(sku: object) -> _Order:
        return _Order(str(sku))

    unknown.__annotations__ = {"sku": "Missing", "return": _Order}

    # Act
    resolved = bind(_place_order, create_autospec(EmitCallable), factory=known, config=None)
    kept = bind(_place_order, create_autospec(EmitCallable), factory=unknown, config=None)

    # Assert
    assert inspect.signature(resolved).parameters["sku"].annotation is str
    assert inspect.signature(kept).parameters["sku"].annotation == "Missing"


def test_direct_bus_bind_is_typed() -> None:
    """
    DirectBus.bind should give a bound callable the event's types.

    Given: A DirectBus and a request event with a reply type
    When: The event is bound
    Then: Its signature is (payload: schema) -> reply
    """
    # Act
    bound = DirectBus().bind(_place_order)

    # Assert
    assert inspect.signature(bound) == inspect.Signature(
        [inspect.Parameter("payload", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=_Order)],
        return_annotation=_Receipt,
    )
