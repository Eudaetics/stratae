"""
Unit tests for how hooks and middleware compose around an adapter's emit.

This test suite verifies the following behaviors:

publish:
- Without hooks or middleware, calls the emit directly.
- With hooks only, runs them around the emit.
- With middleware only, runs it around the emit.
- With both, runs the middleware outside the hooks.
- Publish middleware adds to the headers the emit was given, and the emit sends them.
- A reserved header, given or added by middleware, is refused once, after the middleware runs
  and before the hooks and the send.
"""

from collections.abc import Callable
from typing import Any

import pytest

from stratae.events import Hooks, PublishCall, PubSub
from stratae.events.envelope import EnvelopeHeaders
from stratae.events.pipeline import Outgoing, Publishing, publish

_HEADERS = EnvelopeHeaders()
"""The default names an envelope travels under."""


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
def test_publish_nests_middleware_outside_hooks(
    with_hooks: bool, with_middleware: bool, expected: list[str]
) -> None:
    """
    Publish should run middleware, then hooks, around the emit, whichever are given.

    Given: An emit, with or without a before hook and a publish middleware
    When: publish runs it
    Then: Whatever is given runs in order, middleware outermost, and the reply is returned
    """
    # Arrange
    log: list[str] = []

    def call(message: Outgoing[None, None]) -> str:
        log.append("call")
        return message.payload.item

    def hook(event: Any, payload: Any) -> None:
        log.append("hook")

    def middleware(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
        log.append("middleware")
        return next(call)

    hooks: Hooks[Any, Any] | None = Hooks(before=(hook,)) if with_hooks else None
    stack = (middleware,) if with_middleware else ()

    # Act
    publishing: Publishing[None, None] = Publishing(stack)
    reply = publish(
        publishing, call, _HEADERS.names, _order_placed, _Order("lamp"), None, hooks=hooks
    )

    # Assert
    assert reply == "lamp"
    assert log == expected


def test_publish_middleware_adds_headers() -> None:
    """
    Publish middleware should add to the headers the emit was given, for the emit to send.

    Given: Headers holding a tenant, and publish middleware adding a trace header
    When: publish runs an emit with them
    Then: The message sent holds both headers
    """
    # Arrange
    sent: list[Outgoing[None, None]] = []

    def trace(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
        return next(call.with_headers({"traceparent": "00-trace"}))

    publishing: Publishing[None, None] = Publishing((trace,))

    # Act
    publish(
        publishing,
        sent.append,
        _HEADERS.names,
        _order_placed,
        _Order("lamp"),
        None,
        headers={"x-tenant": "acme"},
    )

    # Assert
    assert dict(sent[0].headers) == {"x-tenant": "acme", "traceparent": "00-trace"}


@pytest.mark.parametrize("source", ["given", "middleware"])
def test_publish_refuses_reserved_header_after_middleware(source: str) -> None:
    """
    A reserved header should be refused once, after publish middleware runs.

    Given: A route reserving x-attempts, middleware recording each call, and hooks recording
        each emit
    When: An emit is given x-attempts, or middleware adds X-Attempts
    Then: The emit raises ValueError naming it; the middleware ran, the hooks and the send
        didn't
    """
    # Arrange
    ran: list[str] = []
    sent: list[Outgoing[None, None]] = []
    given = {"x-attempts": 1} if source == "given" else None

    def add(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
        ran.append("middleware")
        return next(call if given else call.with_headers({"X-Attempts": 1}))

    hooks = Hooks[_Order, None](before=(lambda event, payload: ran.append("hook"),))
    publishing: Publishing[None, None] = Publishing((add,))
    reserved = _HEADERS.names | {"x-attempts"}

    # Act / Assert
    with pytest.raises(ValueError, match="is a header the bus sets itself"):
        publish(
            publishing,
            sent.append,
            reserved,
            _order_placed,
            _Order("lamp"),
            None,
            hooks=hooks,
            headers=given,
        )
    assert ran == ["middleware"]
    assert sent == []
