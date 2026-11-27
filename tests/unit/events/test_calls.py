"""
Unit tests for the steps that build the call made for a function.

This test suite verifies the following behaviors:

payload_call:
- Returns the function itself for an event with a payload.
- Calls a payload-less event's function with nothing.

hooked:
- Runs the call inside sync hooks, which see its payload.

hooked_async:
- Runs the call inside hooks, sync or async, as a coroutine.

deferred:
- Makes a sync call awaitable, run only once it's awaited.
"""

import asyncio
from typing import Any

from stratae.events import Hooks, PubSub
from stratae.events.calls import deferred, hooked, hooked_async, payload_call


class _Order:
    def __init__(self, item: str) -> None:
        self.item = item


_order_placed = PubSub(_Order)


def _shout(order: _Order) -> str:
    return order.item.upper()


def test_payload_call_with_payload() -> None:
    """
    An event with a payload should have its function called with the payload, as itself.

    Given: An event with a payload and a function taking it
    When: payload_call builds its call
    Then: The call is the function itself
    """
    # Act
    call = payload_call(_order_placed, _shout)

    # Assert
    assert call is _shout


def test_payload_call_payload_less() -> None:
    """
    A payload-less event's function should be called with nothing.

    Given: A payload-less event and a function taking no arguments
    When: payload_call builds its call, and it's called with a payload
    Then: The function runs, and its return is returned
    """
    # Arrange
    tick = PubSub(name="tick")

    # Act
    call = payload_call(tick, lambda: "tocked")

    # Assert
    assert call(object()) == "tocked"


def test_hooked() -> None:
    """
    A call should run inside sync hooks, which see its payload.

    Given: A call of the payload, a before hook and a success hook, both logging
    When: hooked wraps the call, and it's called with a payload
    Then: The before hook sees the payload, the success hook the reply, and the reply is
        returned
    """
    # Arrange
    log: list[object] = []

    def before(event: Any, payload: _Order) -> None:
        log.append(("before", event, payload.item))

    def success(event: Any, payload: _Order, reply: str) -> None:
        log.append(("success", reply))

    hooks: Hooks[_Order, str] = Hooks(before=(before,), success=(success,))

    # Act
    reply = hooked(hooks, _order_placed, _shout)(_Order("lamp"))

    # Assert
    assert reply == "LAMP"
    assert log == [("before", _order_placed, "lamp"), ("success", "LAMP")]


async def test_hooked_async() -> None:
    """
    A call should run inside hooks, sync or async, as a coroutine.

    Given: An async call of the payload, an async before hook and a sync success hook
    When: hooked_async wraps the call, and it's awaited with a payload
    Then: Both hooks run, around the call, and the reply is returned
    """
    # Arrange
    log: list[object] = []

    async def before(event: Any, payload: _Order) -> None:
        await asyncio.sleep(0)
        log.append(("before", payload.item))

    def success(event: Any, payload: _Order, reply: str) -> None:
        log.append(("success", reply))

    async def ashout(order: _Order) -> str:
        await asyncio.sleep(0)
        log.append("call")
        return order.item.upper()

    hooks: Hooks[_Order, str] = Hooks(before=(before,), success=(success,))

    # Act
    reply = await hooked_async(hooks, _order_placed, ashout)(_Order("lamp"))

    # Assert
    assert reply == "LAMP"
    assert log == [("before", "lamp"), "call", ("success", "LAMP")]


async def test_deferred_runs_when_awaited() -> None:
    """
    A sync call made awaitable should run only once it's awaited.

    Given: A sync call recording what it's called with
    When: deferred wraps it, it's called, then the result is awaited
    Then: Nothing ran before the await, and the call ran once after it
    """
    # Arrange
    ran: list[_Order] = []

    def call(order: _Order) -> str:
        ran.append(order)
        return order.item.upper()

    order = _Order("lamp")

    # Act
    pending = deferred(call)(order)
    before_await = list(ran)
    reply = await pending

    # Assert
    assert before_await == []
    assert ran == [order]
    assert reply == "LAMP"
