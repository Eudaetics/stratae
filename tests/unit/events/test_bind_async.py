"""
Unit tests for bind with async emitters.

This test suite verifies the following behaviors:

bind with an async emitter and a factory:
- Calling the returned callable constructs the payload via the factory and awaits the emitter.
- Calling the returned callable returns the emitter's result.
- An async factory is awaited before its result is forwarded to the emitter.

bind with an async emitter and no factory:
- Calling the returned callable forwards an already-built payload to the emitter.
- Calling the returned callable returns the emitter's result.

bind with an async emitter and hooks:
- The hooks are forwarded to the emitter.

bound callable signatures:
- Without a factory, the bound coroutine function reports a payload of the event's schema,
  returning its reply type.
- A payload-less event's bound coroutine function takes no parameters, and still emits
  NoPayload.
- With an async factory, it reports the factory's parameters and the event's reply type.
- AsyncDirectBus.bind gives the same typed, awaitable signature.
"""

import asyncio
import inspect
from typing import Any, cast
from unittest.mock import Mock, create_autospec

from stratae.events import (
    ASYNC,
    AsyncDirectBus,
    EmitCallable,
    Emitter,
    EventType,
    Hooks,
    NoPayload,
    PubSub,
    Request,
    bind,
)

type _AnyEmitter = EmitCallable[Any, Any, Any, Any]
"""How a mock emitter is passed: for any event, payload and hooks."""


async def _async_emit(
    event: EventType[Any, Any, Any, Any],
    payload: Any,
    hooks: Any = None,
): ...


class _OrderCreated:
    def __init__(self, order_id: int, status: str) -> None:
        self.order_id = order_id
        self.status = status

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _OrderCreated):
            return False
        return self.order_id == other.order_id and self.status == other.status


# region: With Factory


async def test_bind_async_with_factory_calling_constructs_schema_and_awaits_emitter() -> None:
    """
    A factory-bound callable for an async emitter should build the payload and await the emitter.

    Given: A factory-bound callable bound to an async emitter
    When: The callable is called with arguments
    Then: The emitter should receive the event and the constructed payload
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = PubSub(_OrderCreated)
    bound = bind(ev, Emitter(ASYNC, emitter), factory=_OrderCreated)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(ev, _OrderCreated(1, "pending"))


async def test_bind_async_with_factory_returns_emitter_result() -> None:
    """
    Calling a factory-bound callable bound to an async emitter returns the emitter's result.

    Given: A factory-bound callable whose emitter returns the constructed payload
    When: The callable is called
    Then: The return value should match the constructed payload
    """
    # Arrange
    emitter = create_autospec(_async_emit)

    def _return(
        event: object,
        payload: object,
        hooks: object = None,
    ) -> object:
        return payload

    emitter.side_effect = _return
    ev = PubSub(_OrderCreated)
    bound = bind(ev, Emitter(ASYNC, emitter), factory=_OrderCreated)

    # Act
    result = await bound(1, "pending")

    # Assert
    assert result == _OrderCreated(1, "pending")


async def test_bind_async_with_factory_awaits_async_factory_then_awaits_emitter() -> None:
    """
    An async factory should be awaited the factory before forwarding to the emitter.

    Given: An async factory
    When: The callable bound to an async emitter is called
    Then: The emitter should receive the resolved payload, not the coroutine
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = PubSub(_OrderCreated)

    async def _async_factory(order_id: int, status: str) -> _OrderCreated:
        await asyncio.sleep(0)
        return _OrderCreated(order_id, status)

    bound = bind(ev, Emitter(ASYNC, emitter), factory=_async_factory)

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(ev, _OrderCreated(1, "pending"))


# endregion

# region: Without Factory


async def test_bind_async_without_factory_calling_forwards_payload_to_emitter() -> None:
    """
    Calling a passthrough callable bound to an async emitter forwards an already-built payload.

    Given: A passthrough callable bound to an async emitter with no factory
    When: The callable is called with a ready-made payload
    Then: The emitter should receive the event and that exact payload
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = PubSub(_OrderCreated)
    bound = bind(ev, Emitter(ASYNC, cast(_AnyEmitter, emitter)))
    payload = _OrderCreated(1, "pending")

    # Act
    await bound(payload)

    # Assert
    emitter.assert_awaited_once_with(ev, payload)


async def test_bind_async_without_factory_returns_emitter_result() -> None:
    """
    Calling a passthrough callable bound to an async emitter returns the emitter's result.

    Given: A passthrough callable whose emitter echoes the payload
    When: The callable is called
    Then: The return value should match the payload
    """
    # Arrange
    emitter = create_autospec(_async_emit)

    def _return(
        event: object,
        payload: object,
        hooks: object = None,
    ) -> object:
        return payload

    emitter.side_effect = _return
    ev = PubSub(_OrderCreated)
    bound = bind(ev, Emitter(ASYNC, cast(_AnyEmitter, emitter)))
    payload = _OrderCreated(1, "pending")

    # Act
    result = await bound(payload)

    # Assert
    assert result is payload


# endregion


async def test_bind_async_awaits_async_callable_factory() -> None:
    """
    A factory that's an object with async __call__ should be awaited.

    Given: An instance of a class whose __call__ is async, as the factory
    When: The callable bound to an async emitter is called
    Then: The emitter receives the resolved payload, not a coroutine
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    ev = PubSub(_OrderCreated)

    class Factory:
        async def __call__(self, order_id: int, status: str) -> _OrderCreated:
            return _OrderCreated(order_id, status)

    bound = bind(ev, Emitter(ASYNC, emitter), factory=Factory())

    # Act
    await bound(1, "pending")

    # Assert
    emitter.assert_awaited_once_with(ev, _OrderCreated(1, "pending"))


async def test_bind_async_forwards_hooks_to_emitter() -> None:
    """
    Hooks given to bind with an async emitter should be forwarded to the emitter with every call.

    Given: A callable bound to an async emitter with hooks
    When: The callable is awaited with a payload
    Then: The emitter should receive the same hooks
    """
    # Arrange
    emitter = create_autospec(_async_emit)
    hooks: Hooks[Any, Any] = Hooks()
    ev = PubSub(_OrderCreated)
    bound = bind(ev, Emitter(ASYNC, cast(_AnyEmitter, emitter)), hooks=hooks)
    payload = _OrderCreated(1, "pending")

    # Act
    await bound(payload)

    # Assert
    emitter.assert_awaited_once_with(ev, payload, hooks=hooks)


# region: Bound Callable Signatures


class _Order:
    def __init__(self, sku: str) -> None:
        self.sku = sku


class _Receipt:
    pass


_place_order = Request(_Order, reply=_Receipt)

_payload_signature = inspect.Signature(
    [inspect.Parameter("payload", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=_Order)],
    return_annotation=_Receipt,
)


def test_bound_async_signature_without_factory_is_schema_to_reply() -> None:
    """
    An async bound callable without a factory should report the event's schema and reply.

    Given: A request event with a schema and a reply type
    When: It's bound with bind without a factory
    Then: It's a coroutine function whose signature is (payload: schema) -> reply
    """
    # Act
    bound = bind(_place_order, Emitter(ASYNC, _async_emit))

    # Assert
    assert inspect.iscoroutinefunction(bound)
    assert inspect.signature(bound) == _payload_signature


async def test_bound_async_signal_takes_no_parameters() -> None:
    """
    A payload-less event's async bound callable should take no parameters.

    Given: A payload-less request event with a reply type
    When: It's bound with bind and awaited
    Then: Its signature is () -> reply, and the emitter receives a NoPayload
    """
    # Arrange
    emitter = Mock(side_effect=_async_emit)
    bound = bind(Request(name="ping", reply=_Receipt), Emitter(ASYNC, emitter))

    # Act
    await bound()

    # Assert
    assert inspect.signature(bound) == inspect.Signature([], return_annotation=_Receipt)
    assert isinstance(emitter.call_args.args[1], NoPayload)


def test_bound_async_signature_with_async_factory_has_factory_parameters() -> None:
    """
    An async bound callable with an async factory should report the factory's parameters.

    Given: A request event, and an async factory
    When: It's bound with the factory
    Then: Its signature has the factory's parameters and the event's reply type
    """

    # Arrange
    async def order(sku: str) -> _Order:
        await asyncio.sleep(0)
        return _Order(sku)

    # Act
    bound = bind(_place_order, Emitter(ASYNC, _async_emit), factory=order)

    # Assert
    assert inspect.signature(bound) == inspect.signature(order).replace(return_annotation=_Receipt)


def test_async_direct_bus_bind_is_typed_and_awaitable() -> None:
    """
    AsyncDirectBus.bind should give a bound callable the event's types.

    Given: An AsyncDirectBus and a request event with a reply type
    When: The event is bound
    Then: It's a coroutine function whose signature is (payload: schema) -> reply
    """
    # Act
    bound = AsyncDirectBus().bind(_place_order)

    # Assert
    assert inspect.iscoroutinefunction(bound)
    assert inspect.signature(bound) == _payload_signature
