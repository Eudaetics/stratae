"""
Unit tests for bind.

This test suite verifies the following behaviors:

bind with a factory:
- Calling the returned callable constructs the payload via the factory and invokes the emitter.
- Calling the returned callable returns the emitter's result.
- Raises TypeError when the factory is async.

bind without a factory:
- Calling the returned callable forwards an already-built payload to the emitter.
- Calling the returned callable returns the emitter's result.

bind with hooks:
- The hooks are forwarded to the emitter.
- An emitter without a hooks parameter works without hooks, and refuses them loudly.

bind binds by the emitter's kind:
- An async emitter gives an async callable, with a sync or an async factory, or none.
- The emitter's kind decides: a plain function returning a coroutine, paired with ASYNC,
  gives an async callable.
- A function not paired with a kind is refused.
- A sync emitter refuses async hooks when bound, naming the event, and an async factory.

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
from collections.abc import Coroutine
from typing import Any, cast, get_type_hints
from unittest.mock import create_autospec

import pytest
from pytest_mock import MockerFixture

from stratae.events import (
    ASYNC,
    SYNC,
    DirectBus,
    EmitCallable,
    Emitter,
    Hooks,
    NoPayload,
    PubSub,
    Request,
    bind,
)

type _AnyEmitter = EmitCallable[Any, Any, Any, Any]
"""How a mock emitter is passed: for any event, payload and hooks."""


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
          should receive the event and the constructed payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    spy = mocker.spy(_OrderCreated, "__init__")
    ev = PubSub(_OrderCreated)
    bound = bind(ev, Emitter(SYNC, emitter), factory=_OrderCreated)

    # Act
    bound(1, "pending")

    # Assert
    spy.assert_called_once_with(mocker.ANY, 1, "pending")
    emitter.assert_called_once_with(ev, _OrderCreated(1, "pending"))


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
        payload: object,
        hooks: object = None,
    ) -> object:
        return payload

    emitter.side_effect = _return

    ev = PubSub(_OrderCreated)
    bound = bind(ev, Emitter(SYNC, emitter), factory=_OrderCreated)

    # Act
    result = bound(1, "pending")

    # Assert
    assert result == _OrderCreated(1, "pending")


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
    with pytest.raises(TypeError, match="a sync emitter needs a sync factory"):
        bind(ev, Emitter(SYNC, emitter), factory=_async_factory)


# endregion

# region: Bind Without Factory


def test_bind_without_factory_calling_forwards_payload_to_emitter() -> None:
    """
    Calling a passthrough callable produced by bind forwards an already-built payload.

    Given: A passthrough callable produced by bind with no factory
    When: The callable is called with a ready-made payload
    Then: The emitter should receive the event and that exact payload
    """
    # Arrange
    emitter = create_autospec(EmitCallable)
    ev = PubSub(_OrderCreated)
    bound = bind(ev, Emitter(SYNC, cast(_AnyEmitter, emitter)))
    payload = _OrderCreated(1, "pending")

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(ev, payload)


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
        payload: object,
        hooks: object = None,
    ) -> object:
        return payload

    emitter.side_effect = _return

    ev = PubSub(_OrderCreated)
    bound = bind(ev, Emitter(SYNC, cast(_AnyEmitter, emitter)))
    payload = _OrderCreated(1, "pending")

    # Act
    result = bound(payload)

    # Assert
    assert result is payload


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
    with pytest.raises(TypeError, match="a sync emitter needs a sync factory"):
        bind(ev, Emitter(SYNC, emitter), factory=Factory())


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
    bound = bind(ev, Emitter(SYNC, cast(_AnyEmitter, emitter)), hooks=hooks)
    payload = _OrderCreated(1, "pending")

    # Act
    bound(payload)

    # Assert
    emitter.assert_called_once_with(ev, payload, hooks=hooks)


def test_emitter_without_hooks_parameter() -> None:
    """
    An emitter that doesn't take hooks should work until hooks are bound with it.

    Given: An emitter with no hooks parameter
    When: It's bound without hooks and called, then bound with hooks and called
    Then: The first call reaches the emitter; the second raises TypeError
    """
    # Arrange
    received: list[object] = []

    def emitter(event: object, payload: object) -> None:
        received.append(payload)

    ev = PubSub(_OrderCreated)
    payload = _OrderCreated(1, "pending")
    plain = bind(ev, Emitter(SYNC, emitter))
    hooked = bind(ev, Emitter(SYNC, cast(_AnyEmitter, emitter)), hooks=Hooks())

    # Act / Assert
    plain(payload)
    assert received == [payload]
    with pytest.raises(TypeError, match="hooks"):
        hooked(payload)


# region: Bound By The Emitter's Kind


async def _async_emitter(event: object, payload: _OrderCreated, /) -> str:
    await asyncio.sleep(0)
    return payload.status


async def _build_async(order_id: int, status: str) -> _OrderCreated:
    await asyncio.sleep(0)
    return _OrderCreated(order_id, status)


def _sync_emitter(event: object, payload: _OrderCreated, /) -> str:
    return payload.status


async def _async_before(event: object, payload: object) -> None:
    await asyncio.sleep(0)


async def test_async_emitter_gives_async_callable():
    """
    An async emitter should give an async callable, whatever its factory.

    Given: An async emitter
    When: It's bound without a factory, with a sync factory and with an async factory
    Then: Each callable is a coroutine function, and awaiting it gives the emitter's result
    """
    # Arrange
    event = PubSub(_OrderCreated)

    # Act
    built = bind(event, Emitter(ASYNC, _async_emitter))
    sync_factory = bind(event, Emitter(ASYNC, _async_emitter), factory=_OrderCreated)
    async_factory = bind(event, Emitter(ASYNC, _async_emitter), factory=_build_async)

    # Assert
    assert all(inspect.iscoroutinefunction(b) for b in (built, sync_factory, async_factory))
    assert await built(_OrderCreated(1, "built")) == "built"
    assert await sync_factory(1, "sync") == "sync"
    assert await async_factory(1, "async") == "async"


async def test_kind_decides_not_the_call():
    """
    The emitter's kind should decide how it's bound, not what its call looks like.

    Given: A plain function returning a coroutine, paired with ASYNC
    When: It's bound with an async factory
    Then: The callable is a coroutine function, and awaiting it gives the call's result
    """

    # Arrange
    def emitter(event: object, payload: _OrderCreated, /) -> Coroutine[Any, Any, str]:
        return _async_emitter(event, payload)

    # Act
    bound = bind(PubSub(_OrderCreated), Emitter(ASYNC, emitter), factory=_build_async)

    # Assert
    assert inspect.iscoroutinefunction(bound)
    assert await bound(1, "stated") == "stated"


def test_bare_function_refused():
    """
    A function given to bind without its kind should be refused.

    Given: A sync function, not paired with a kind
    When: It's bound
    Then: TypeError says bind takes an Emitter
    """
    # Act / Assert
    with pytest.raises(TypeError, match="bind takes an Emitter"):
        bind(PubSub(_OrderCreated), _sync_emitter)  # pyright: ignore[reportCallIssue, reportArgumentType]


def test_sync_emitter_refuses_async_hooks_and_factory():
    """
    A sync emitter should refuse async hooks and an async factory when bound.

    Given: A sync emitter
    When: It's bound with an async hook, then with an async factory
    Then: Each raises TypeError when binding; the hooks refusal names the event
    """
    # Arrange
    event = PubSub(_OrderCreated)
    hooks: Hooks[Any, Any] = Hooks(before=(_async_before,))

    # Act / Assert
    with pytest.raises(TypeError, match="'_OrderCreated' is bound to a sync emitter"):
        bind(event, Emitter(SYNC, cast(_AnyEmitter, _sync_emitter)), hooks=hooks)
    with pytest.raises(TypeError, match="a sync emitter needs a sync factory"):
        bind(event, Emitter(SYNC, _sync_emitter), factory=_build_async)  # pyright: ignore[reportCallIssue, reportArgumentType]


# endregion


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
    bound = bind(_place_order, Emitter(SYNC, cast(_AnyEmitter, create_autospec(EmitCallable))))

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
    bound = bind(PubSub(_Order), Emitter(SYNC, cast(_AnyEmitter, create_autospec(EmitCallable))))

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
    bound = bind(event, Emitter(SYNC, cast(_AnyEmitter, emitter)))

    # Act
    bound()

    # Assert
    assert inspect.signature(bound) == inspect.Signature([], return_annotation=_Receipt)
    assert isinstance(emitter.call_args.args[1], NoPayload)


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
    bound = bind(_place_order, Emitter(SYNC, create_autospec(EmitCallable)), factory=order)

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
    resolved = bind(_place_order, Emitter(SYNC, create_autospec(EmitCallable)), factory=known)
    kept = bind(_place_order, Emitter(SYNC, create_autospec(EmitCallable)), factory=unknown)

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
