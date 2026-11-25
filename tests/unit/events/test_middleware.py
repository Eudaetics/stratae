"""
Unit tests for a bus's middleware: the publish and handle halves, and the calls they see.

This test suite verifies the following behaviors:

Middleware:
- Lists given for either half are frozen as tuples.
- A lone middleware, or a string, in place of a tuple raises a clear TypeError.
- An entry that isn't callable raises a clear TypeError, for Middleware and Hooks alike.
- require refuses an async def, a generator function or a contextlib context manager for a
  sync bus, and anything but an async def for an async bus, and can check only one half.

PublishCall:
- with_headers returns a new call with the headers added, leaving the original unchanged.

On a direct bus, sync and async:
- Middleware runs outermost first around the event's hooks, around the handler.
- Publish middleware sees the event, payload and no config, and the reply comes back from next.
- Headers a publish middleware adds reach handle middleware; with none added, it reads an
  empty mapping.
- A header the bus sets itself, added by publish middleware in any case, with with_headers or
  by replacing the headers, refuses the emit before any handler runs.
- The payload a publish middleware passes to next is the one handlers get.
- A publish middleware passing on another event or config, or a handle middleware passing on
  another event, config or headers, raises ValueError before any handler runs, on sync and
  async buses.
- The payload a handle middleware passes to next is the one the handler gets.
- A publish middleware that skips next keeps the emit from reaching any handler.
- A handle middleware that catches the handler's error makes the emit succeed with its reply.
"""

import asyncio
from collections.abc import Awaitable, Callable, Generator, Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest

from stratae.events import (
    AsyncDirectBus,
    DirectBus,
    EnvelopeConfig,
    HandleCall,
    Hooks,
    Middleware,
    PublishCall,
    PubSub,
    Request,
)
from stratae.middleware import MiddlewareKindError


class _Order:
    def __init__(self, item: str = "lamp") -> None:
        self.item = item


_placed = PubSub(_Order)
_place = Request(_Order, reply=str)


def _sync(call: Any, next: Callable[[Any], Any]) -> Any:
    return next(call)


async def _async(call: Any, next: Callable[[Any], Awaitable[Any]]) -> Any:
    return await next(call)


def _generator(call: Any, next: Callable[[Any], Any]) -> Iterator[Any]:
    yield next(call)


@contextmanager
def _context_manager(call: Any) -> Generator[None]:
    yield


# region: Middleware


def test_middleware_freezes_lists() -> None:
    """
    Middleware given as lists should be stored as tuples.

    Given: Publish and handle middleware passed as lists
    When: Middleware is built
    Then: Both halves are tuples of the same middleware
    """
    middleware = Middleware(publish=cast(Any, [_sync]), handle=cast(Any, [_sync]))

    assert middleware.publish == (_sync,)
    assert middleware.handle == (_sync,)


@pytest.mark.parametrize(
    "given", [pytest.param(_sync, id="lone-middleware"), pytest.param("trace", id="str")]
)
def test_lone_middleware_raises_clear_error(given: object) -> None:
    """
    A single middleware, or a string, given where a tuple belongs should raise a TypeError.

    Given: One middleware, or a string, passed as publish= instead of a tuple
    When: Middleware is built
    Then: A TypeError says Middleware publish= takes a tuple of callables
    """
    with pytest.raises(TypeError, match="Middleware publish= takes a tuple of callables"):
        Middleware(publish=cast(Any, given))


def test_uncallable_middleware_raises_clear_error() -> None:
    """
    An entry that isn't callable should raise a clear TypeError when Middleware is built.

    Given: A tuple holding a middleware and None
    When: Middleware is built with it as handle=
    Then: A TypeError names the entry that isn't callable
    """
    with pytest.raises(TypeError, match="Middleware handle= holds None, which isn't callable"):
        Middleware(handle=cast(Any, (_sync, None)))


def test_uncallable_hook_raises_clear_error() -> None:
    """
    Hooks share the check: an entry that isn't callable should raise a clear TypeError.

    Given: A string inside the before= tuple
    When: Hooks is built
    Then: A TypeError names the entry that isn't callable
    """
    with pytest.raises(TypeError, match="Hooks before= holds 'audit', which isn't callable"):
        Hooks(before=cast(Any, ("audit",)))


@pytest.mark.parametrize(
    ("bus", "asynchronous", "wrong"),
    [
        ("DirectBus", False, _async),
        ("DirectBus", False, _generator),
        ("DirectBus", False, _context_manager),
        ("AsyncDirectBus", True, _sync),
    ],
    ids=["sync-bus-async-def", "sync-bus-generator", "sync-bus-contextmanager", "async-bus"],
)
def test_require_refuses_the_wrong_kind(bus: str, asynchronous: bool, wrong: Any) -> None:
    """
    Require should refuse middleware of the wrong kind for the bus, in either half.

    Given: Handle middleware of the wrong kind for a sync or an async bus, including a
        generator function or a contextlib context manager left over from older middleware
    When: require is called for that bus
    Then: MiddlewareKindError names the bus
    """
    with pytest.raises(MiddlewareKindError, match=bus):
        Middleware(handle=(wrong,)).require(bus, asynchronous=asynchronous)


def test_require_checks_only_selected_half() -> None:
    """
    Require should check only the halves asked for.

    Given: Sync publish middleware and async handle middleware
    When: require checks only publish for a sync bus
    Then: It doesn't raise
    """
    middleware = Middleware(publish=(_sync,), handle=(_async,))

    middleware.require("DirectBus", asynchronous=False, handle=False)


# endregion

# region: PublishCall


def test_with_headers_adds_to_a_new_call() -> None:
    """
    with_headers should return a new call with the headers added, leaving the original.

    Given: A publish call with one header
    When: Another is added with with_headers
    Then: The new call has both, and the original still has one
    """
    call = PublishCall(_placed, None, _Order(), {"x-trace": "1"})

    added = call.with_headers({"x-tenant": "acme"})

    assert dict(added.headers) == {"x-trace": "1", "x-tenant": "acme"}
    assert dict(call.headers) == {"x-trace": "1"}


# endregion

# region: On a direct bus


def test_sync_bus_runs_middleware_around_hooks_around_the_handler() -> None:
    """
    A sync bus should run middleware outermost first around hooks around the handler.

    Given: Two publish middleware, a before hook on the emit, and a responder
    When: The request is emitted
    Then: The middleware run outer then inner, the hook, then the handler, and each
        middleware gets the reply back from next
    """
    log: list[str] = []

    def labelled(label: str) -> Any:
        def middleware(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
            log.append(f"{label} in {call.event.name} {call.payload.item} {call.config}")
            reply = next(call)
            log.append(f"{label} out {reply}")
            return reply

        return middleware

    bus = DirectBus(middleware=Middleware(publish=(labelled("outer"), labelled("inner"))))
    bus.handle(_place, lambda order: log.append("handler") or f"placed {order.item}")
    hooks = Hooks[Any, Any](before=(lambda event, payload: log.append("hook"),))

    reply = bus.emit(_place, _Order(), hooks=hooks)

    assert reply == "placed lamp"
    assert log == [
        "outer in _Order lamp None",
        "inner in _Order lamp None",
        "hook",
        "handler",
        "inner out placed lamp",
        "outer out placed lamp",
    ]


async def test_async_bus_runs_async_middleware() -> None:
    """
    An async bus should run async middleware around its handler, each awaiting next.

    Given: Async publish and handle middleware recording what they see
    When: A request is emitted
    Then: Both see the call, and the publish middleware sees the reply
    """
    log: list[str] = []

    async def publish(call: PublishCall, next: Callable[[PublishCall], Awaitable[Any]]) -> Any:
        reply = await next(call)
        log.append(f"publish {reply}")
        return reply

    async def handle(call: HandleCall, next: Callable[[HandleCall], Awaitable[Any]]) -> Any:
        log.append(f"handle {call.payload.item}")
        return await next(call)

    async def responder(order: _Order) -> str:
        await asyncio.sleep(0)
        return f"placed {order.item}"

    bus = AsyncDirectBus(middleware=Middleware(publish=(publish,), handle=(handle,)))
    bus.handle(_place, responder)

    assert await bus.emit(_place, _Order()) == "placed lamp"
    assert log == ["handle lamp", "publish placed lamp"]


def _tenant(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
    return next(call.with_headers({"x-tenant": "acme"}))


async def _atenant(call: PublishCall, next: Callable[[PublishCall], Awaitable[Any]]) -> Any:
    return await next(call.with_headers({"x-tenant": "acme"}))


@pytest.mark.parametrize("added", [True, False], ids=["added", "none-added"])
def test_headers_reach_handle_middleware(added: bool) -> None:
    """
    Headers publish middleware adds should reach handle middleware, on a sync bus.

    Given: Handle middleware recording its headers, with or without publish middleware
        adding a tenant
    When: An event is emitted
    Then: The handle middleware reads the tenant, or an empty mapping
    """
    seen: list[dict[str, object]] = []

    def read(call: HandleCall, next: Callable[[HandleCall], Any]) -> Any:
        seen.append(dict(call.headers))
        return next(call)

    bus = DirectBus(middleware=Middleware(publish=(_tenant,) if added else (), handle=(read,)))
    bus.handle(_placed, lambda order: None)

    bus.emit(_placed, _Order())

    assert seen == [{"x-tenant": "acme"} if added else {}]


async def test_headers_reach_async_handle_middleware() -> None:
    """
    Headers publish middleware adds should reach handle middleware, on an async bus.

    Given: Async publish middleware adding a tenant, and handle middleware recording it
    When: An event is emitted
    Then: The handle middleware reads the tenant
    """
    seen: list[dict[str, object]] = []

    async def read(call: HandleCall, next: Callable[[HandleCall], Awaitable[Any]]) -> Any:
        seen.append(dict(call.headers))
        return await next(call)

    async def handler(order: _Order) -> None:
        await asyncio.sleep(0)

    bus = AsyncDirectBus(middleware=Middleware(publish=(_atenant,), handle=(read,)))
    bus.handle(_placed, handler)

    await bus.emit(_placed, _Order())

    assert seen == [{"x-tenant": "acme"}]


def _with_headers(name: str) -> Any:
    def forge(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
        return next(call.with_headers({name: "forged"}))

    return forge


def _replacing(name: str) -> Any:
    def forge(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
        return next(call._replace(headers={name: "forged"}))

    return forge


@pytest.mark.parametrize("forge", [_with_headers, _replacing], ids=["with-headers", "replace"])
@pytest.mark.parametrize("name", ["x-message-id", "X-Message-ID"])
def test_reserved_header_from_middleware_refuses_the_emit(forge: Any, name: str) -> None:
    """
    A header the bus sets itself should refuse the emit, however publish middleware adds it.

    Given: A bus with an envelope, publish middleware adding an envelope header, in lower or
        mixed case, with
        with_headers or by replacing the call's headers, and handle middleware recording calls
    When: An event is emitted
    Then: ValueError names the header, and no handle middleware or handler runs
    """
    handled: list[HandleCall] = []

    def record(call: HandleCall, next: Callable[[HandleCall], Any]) -> Any:
        handled.append(call)
        return next(call)

    bus = DirectBus(
        envelope=EnvelopeConfig(), middleware=Middleware(publish=(forge(name),), handle=(record,))
    )
    bus.handle(_placed, lambda order: None)

    with pytest.raises(ValueError, match=name):
        bus.emit(_placed, _Order())
    assert handled == []


def test_publish_middleware_passes_its_payload_on() -> None:
    """
    The payload publish middleware passes to next should be the one handlers get.

    Given: Publish middleware passing on a call with another payload
    When: An event is emitted
    Then: The handler gets the new payload
    """
    handled: list[str] = []

    def swap(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
        return next(call._replace(payload=_Order("desk")))

    bus = DirectBus(middleware=Middleware(publish=(swap,)))
    bus.handle(_placed, lambda order: handled.append(order.item))

    bus.emit(_placed, _Order())

    assert handled == ["desk"]


_other = PubSub(_Order, name="other")


@pytest.mark.parametrize(
    "changed", [{"event": _other}, {"config": "other"}], ids=["event", "config"]
)
def test_publish_middleware_cannot_change_the_route(changed: dict[str, Any]) -> None:
    """
    Publish middleware passing on a call with another event or config should raise.

    Given: Publish middleware passing on a call with another event, or another config
    When: An event is emitted
    Then: ValueError says the route is chosen before middleware runs, and no handler runs
    """
    handled: list[_Order] = []

    def reroute(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
        return next(call._replace(**changed))

    bus = DirectBus(middleware=Middleware(publish=(reroute,)))
    bus.handle(_placed, handled.append)

    with pytest.raises(ValueError, match="route"):
        bus.emit(_placed, _Order())
    assert handled == []


@pytest.mark.parametrize(
    "changed",
    [{"event": _other}, {"config": "other"}, {"headers": {"x-tenant": "acme"}}],
    ids=["event", "config", "headers"],
)
def test_handle_middleware_cannot_change_the_message(changed: dict[str, Any]) -> None:
    """
    Handle middleware passing on a call with another event, config or headers should raise.

    Given: Handle middleware passing on a call with one of them changed
    When: A request is emitted
    Then: ValueError says they describe the message being handled, and the handler doesn't run
    """
    handled: list[_Order] = []

    def relabel(call: HandleCall, next: Callable[[HandleCall], Any]) -> Any:
        return next(call._replace(**changed))

    def responder(order: _Order) -> str:
        handled.append(order)
        return "placed"

    bus = DirectBus(middleware=Middleware(handle=(relabel,)))
    bus.handle(_place, responder)

    with pytest.raises(ValueError, match="message being handled"):
        bus.emit(_place, _Order())
    assert handled == []


async def test_async_handle_middleware_cannot_change_the_message() -> None:
    """
    Async handle middleware passing on a call with another event should raise.

    Given: Async handle middleware passing on a call with another event
    When: A request is emitted on an async bus
    Then: ValueError says they describe the message being handled
    """

    async def relabel(call: HandleCall, next: Callable[[HandleCall], Awaitable[Any]]) -> Any:
        return await next(call._replace(event=_other))

    async def responder(order: _Order) -> str:
        await asyncio.sleep(0)
        return "placed"

    bus = AsyncDirectBus(middleware=Middleware(handle=(relabel,)))
    bus.handle(_place, responder)

    with pytest.raises(ValueError, match="message being handled"):
        await bus.emit(_place, _Order())


def test_handle_middleware_passes_its_payload_on() -> None:
    """
    The payload handle middleware passes to next should be the one the handler gets.

    Given: Handle middleware passing on a call with another payload
    When: A request is emitted
    Then: The handler gets the new payload
    """

    def swap(call: HandleCall, next: Callable[[HandleCall], Any]) -> Any:
        return next(call._replace(payload=_Order("desk")))

    bus = DirectBus(middleware=Middleware(handle=(swap,)))
    bus.handle(_place, lambda order: f"placed {order.item}")

    assert bus.emit(_place, _Order()) == "placed desk"


def test_skipping_next_keeps_the_emit_from_its_handlers() -> None:
    """
    A publish middleware that returns without next should keep the emit from its handlers.

    Given: Publish middleware that returns at once, and a handler recording calls
    When: An event is emitted
    Then: The handler isn't called
    """
    handled: list[_Order] = []

    def drop(call: PublishCall, next: Callable[[PublishCall], Any]) -> Any:
        return None

    bus = DirectBus(middleware=Middleware(publish=(drop,)))
    bus.handle(_placed, handled.append)

    bus.emit(_placed, _Order())

    assert handled == []


def test_catching_the_handlers_error_makes_the_emit_succeed() -> None:
    """
    A handle middleware that catches the handler's error should make the emit succeed.

    Given: A responder that raises, and handle middleware that returns a fallback instead
    When: The request is emitted
    Then: The emit returns the fallback
    """

    def fallback(call: HandleCall, next: Callable[[HandleCall], Any]) -> Any:
        try:
            return next(call)
        except LookupError:
            return "unavailable"

    def responder(order: _Order) -> str:
        raise LookupError(order.item)

    bus = DirectBus(middleware=Middleware(handle=(fallback,)))
    bus.handle(_place, responder)

    assert bus.emit(_place, _Order()) == "unavailable"


# endregion
