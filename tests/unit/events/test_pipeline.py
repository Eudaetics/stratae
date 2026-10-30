"""
Unit tests for how hooks and middleware compose around an adapter's emit.

This test suite verifies the following behaviors:

run_emit:
- Without hooks or middleware, calls the emit directly.
- With hooks only, runs them around the emit.
- With middleware only, runs it around the emit.
- With both, runs the middleware outside the hooks.

wrap_handler:
- Leaves a handler unwrapped without hooks or middleware.
- Wraps a sync handler in sync hooks, then sync middleware, as a sync call; middleware sees
  the event and payload.
- Wraps a handler as an async call when the handler, a hook or the middleware is async.
- Adapts a payload-less event's handler to take the payload.
"""

from contextlib import asynccontextmanager, contextmanager
from typing import Any, AsyncGenerator, Generator

import pytest

from stratae.events import HandleCall, Hooks, PublishCall, PubSub, Request
from stratae.events.pipeline import run_emit, wrap_handler


class _Order:
    def __init__(self, item: str) -> None:
        self.item = item


_order_placed = PubSub(_Order)


@pytest.mark.parametrize(
    ("with_hooks", "with_middleware", "expected"),
    [
        (False, False, ["call"]),
        (True, False, ["hook", "call"]),
        (False, True, ["middleware", "call"]),
        (True, True, ["middleware", "hook", "call"]),
    ],
    ids=["neither", "hooks", "middleware", "both"],
)
def test_run_emit_nests_middleware_outside_hooks(
    with_hooks: bool, with_middleware: bool, expected: list[str]
) -> None:
    """
    run_emit should run middleware, then hooks, around the emit, whichever are given.

    Given: An emit, with or without a before hook and a publish middleware
    When: run_emit runs it
    Then: Whatever is given runs in order, middleware outermost, and the reply is returned
    """
    # Arrange
    log: list[str] = []

    def call(order: _Order) -> str:
        log.append("call")
        return order.item

    def hook(event: Any, payload: Any) -> None:
        log.append("hook")

    @contextmanager
    def middleware(publish: PublishCall) -> Generator[None, None, None]:
        log.append("middleware")
        yield

    hooks: Hooks[Any, Any] | None = Hooks(before=(hook,)) if with_hooks else None
    stack = (middleware,) if with_middleware else ()

    # Act
    reply = run_emit(hooks, stack, _order_placed, None, _Order("lamp"), call, _Order("lamp"))

    # Assert
    assert reply == "lamp"
    assert log == expected


def _shout(order: _Order) -> str:
    return order.item.upper()


async def _ashout(order: _Order) -> str:
    return order.item.upper()


def test_wrap_handler_without_hooks_or_middleware() -> None:
    """
    A handler without hooks or middleware should be registered as it is.

    Given: A sync handler, no hooks and no middleware
    When: wrap_handler builds its call
    Then: The handler itself is returned, still sync
    """
    # Act
    call, is_async = wrap_handler(_order_placed, None, _shout, None, (), asynchronous=False)

    # Assert
    assert call is _shout
    assert is_async is False


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

    @contextmanager
    def middleware(call: HandleCall) -> Generator[None, None, None]:
        log.append(f"middleware {call.event.name} {call.payload.item}")
        yield

    # Act
    call, is_async = wrap_handler(
        _order_placed, None, _shout, Hooks(before=(hook,)), (middleware,), asynchronous=False
    )
    reply = call(_Order("lamp"))

    # Assert
    assert is_async is False
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
        log.append("hook")

    @asynccontextmanager
    async def middleware(call: HandleCall) -> AsyncGenerator[None, None]:
        log.append("middleware")
        yield

    hooks: Hooks[Any, Any] = Hooks(before=(async_hook if hook_is_async else sync_hook,))
    stack = (middleware,) if with_middleware else ()

    # Act
    call, is_async = wrap_handler(_order_placed, None, handler, hooks, stack, asynchronous=True)
    reply = await call(_Order("lamp"))

    # Assert
    assert is_async is True
    assert reply == "LAMP"
    assert log == (["middleware", "hook"] if with_middleware else ["hook"])


def test_wrap_handler_payload_less() -> None:
    """
    A payload-less event's handler should be adapted to take, and ignore, the payload.

    Given: A payload-less event and a handler taking no arguments
    When: The built call is called with a payload
    Then: The handler runs, and its return is returned
    """
    # Arrange
    ping = Request(reply=str, name="ping")

    # Act
    call, _ = wrap_handler(ping, None, lambda: "pong", None, (), asynchronous=False)

    # Assert
    assert call(object()) == "pong"
