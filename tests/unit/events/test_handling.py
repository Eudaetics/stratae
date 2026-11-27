"""
Unit tests for how hooks and middleware compose around an adapter's handler.

This test suite verifies the following behaviors:

wrap_handler:
- Without hooks or middleware, calls the handler with the message's payload, as a sync call.
- Wraps a sync handler in sync hooks, then sync middleware, as a sync call; middleware sees
  the message, and hooks its payload.
- Wraps a handler as an async call when the handler, a hook or the middleware is async.
- A sync handler under an async hook or async middleware runs only when its call is awaited.
- Calls a payload-less event's handler with nothing.
- Handle middleware, sync or async, reads the headers of the message it's given.
"""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from types import MappingProxyType
from typing import Any

import pytest

from stratae.events import HandleCall, Hooks, PubSub, Request
from stratae.events.handler import invoke
from stratae.events.handling import wrap_handler
from stratae.events.middleware import NO_HEADERS


class _Order:
    def __init__(self, item: str) -> None:
        self.item = item


_order_placed = PubSub(_Order)


def _received(order: _Order, headers: Mapping[str, object] = NO_HEADERS) -> HandleCall:
    """Return the message a direct bus hands its handlers for `order`."""
    return HandleCall(_order_placed, None, order, headers)


def _shout(order: _Order) -> str:
    return order.item.upper()


async def _ashout(order: _Order) -> str:
    await asyncio.sleep(0)
    return order.item.upper()


def test_wrap_handler_without_hooks_or_middleware() -> None:
    """
    A handler without hooks or middleware should be called with the message's payload.

    Given: A sync handler, no hooks and no middleware
    When: wrap_handler builds its call, and it's called with a message
    Then: It's sync, and returns what the handler returns for the message's payload
    """
    # Act
    handler = wrap_handler(_order_placed, _shout, None, None, (), asynchronous=False)

    # Assert
    assert handler.is_async is False
    assert handler(_received(_Order("lamp"))) == "LAMP"


def test_wrap_handler_sync_layers() -> None:
    """
    A sync handler should be wrapped in its hooks, then middleware, as a sync call.

    Given: A sync handler, a before hook and a sync handle middleware, all logging
    When: The built call is called with a payload
    Then: The middleware runs outside the hook, sees the event and payload, and the
        handler's return is returned
    """
    # Arrange
    log: list[str] = []

    def hook(event: Any, payload: _Order) -> None:
        log.append("hook")

    def middleware(call: HandleCall, next: Callable[[HandleCall], Any]) -> Any:
        log.append(f"middleware {call.event.name} {call.payload.item}")
        return next(call)

    # Act
    handler = wrap_handler(
        _order_placed, _shout, None, Hooks(before=(hook,)), (middleware,), asynchronous=False
    )
    assert handler.is_async is False
    reply = handler(_received(_Order("lamp")))

    # Assert
    assert reply == "LAMP"
    assert log == ["middleware _Order lamp", "hook"]


@pytest.mark.parametrize(
    ("handler", "hook_is_async", "with_middleware"),
    [(_ashout, False, False), (_shout, True, False), (_shout, False, True)],
    ids=["async handler", "async hook", "async middleware"],
)
async def test_wrap_handler_async(handler: Any, hook_is_async: bool, with_middleware: bool) -> None:
    """
    A handler should become an async call when the handler, a hook or the middleware is async.

    Given: A handler with a before hook, and maybe async handle middleware
    When: The built call is awaited with a payload
    Then: It's async, and the handler's return is returned after its hook ran
    """
    # Arrange
    log: list[str] = []

    def sync_hook(event: Any, payload: _Order) -> None:
        log.append("hook")

    async def async_hook(event: Any, payload: _Order) -> None:
        await asyncio.sleep(0)
        log.append("hook")

    async def middleware(call: HandleCall, next: Callable[[HandleCall], Awaitable[Any]]) -> Any:
        log.append("middleware")
        return await next(call)

    hooks: Hooks[Any, Any] = Hooks(before=(async_hook if hook_is_async else sync_hook,))
    stack = (middleware,) if with_middleware else ()

    # Act
    built = wrap_handler(_order_placed, handler, None, hooks, stack, asynchronous=True)
    assert built.is_async is True
    reply = await built(_received(_Order("lamp")))

    # Assert
    assert reply == "LAMP"
    assert log == (["middleware", "hook"] if with_middleware else ["hook"])


@pytest.mark.parametrize("through", ["async hook", "async middleware"])
async def test_wrap_handler_sync_handler_runs_when_awaited(through: str) -> None:
    """
    A sync handler under an async runner should run only once its call is awaited.

    Given: A sync handler wrapped with an async hook, or with async handle middleware
    When: The built call is made, then awaited
    Then: The handler hasn't run after the call, and has run once after the await
    """
    # Arrange
    ran: list[_Order] = []

    def handler(order: _Order) -> str:
        ran.append(order)
        return order.item.upper()

    async def async_hook(event: Any, payload: _Order) -> None:
        await asyncio.sleep(0)

    async def middleware(call: HandleCall, next: Callable[[HandleCall], Awaitable[Any]]) -> Any:
        return await next(call)

    hooks: Hooks[Any, Any] | None = Hooks(before=(async_hook,)) if through == "async hook" else None
    stack = (middleware,) if through == "async middleware" else ()
    built = wrap_handler(_order_placed, handler, None, hooks, stack, asynchronous=True)
    order = _Order("lamp")
    assert built.is_async is True

    # Act
    pending = built(_received(order))
    before_await = list(ran)
    reply = await pending

    # Assert
    assert before_await == []
    assert ran == [order]
    assert reply == "LAMP"


def test_wrap_handler_payload_less() -> None:
    """
    A payload-less event's handler should be called with nothing.

    Given: A payload-less event and a handler taking no arguments
    When: The built call is called with a message
    Then: The handler runs, and its return is returned
    """
    # Arrange
    ping = Request(reply=str, name="ping")

    # Act
    handler = wrap_handler(ping, lambda: "pong", None, None, (), asynchronous=False)

    # Assert
    assert handler.is_async is False
    assert handler(HandleCall(ping, None, object())) == "pong"


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_wrap_handler_middleware_reads_message_headers(asynchronous: bool) -> None:
    """
    Handle middleware should read the headers of the message it's given.

    Given: A handler wrapped in handle middleware recording the headers it reads
    When: It's called with a message carrying a header, then with one carrying none
    Then: The middleware read each message's headers
    """
    # Arrange
    seen: list[dict[str, object]] = []

    def record(call: HandleCall, next: Callable[[HandleCall], Any]) -> Any:
        seen.append(dict(call.headers))
        return next(call)

    async def arecord(call: HandleCall, next: Callable[[HandleCall], Awaitable[Any]]) -> Any:
        seen.append(dict(call.headers))
        return await next(call)

    stack = (arecord,) if asynchronous else (record,)
    handler = wrap_handler(_order_placed, _shout, None, None, stack, asynchronous)

    async def handle(message: HandleCall) -> None:
        await invoke(handler, message)

    # Act
    await handle(_received(_Order("lamp"), MappingProxyType({"x-tenant": "acme"})))
    await handle(_received(_Order("lamp")))

    # Assert
    assert seen == [{"x-tenant": "acme"}, {}]
