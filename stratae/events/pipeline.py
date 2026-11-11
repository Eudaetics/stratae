"""
Compose hooks and middleware around an adapter's emits and handlers.

Every adapter runs these, so hooks and middleware behave the same on every bus: middleware
wraps hooks, which wrap the call itself. An adapter of your own, such as one for another
broker, uses them the same way. Its `emit` runs the publish with {py:func}`run_emit` or
{py:func}`run_emit_async`, inside the message's envelope. Its `handle` registers the call
{py:func}`wrap_handler` builds, which takes the payload whatever the event, as a
{py:class}`Handler <stratae.events.handler.Handler>`, and calls it with
{py:func}`invoke <stratae.events.handler.invoke>`.

Headers travel with the message. An adapter gives its emit a
{py:class}`MessageHeaders <stratae.events.middleware.MessageHeaders>` for publish middleware
to add to, and sends them with the message. It handles a message inside
{py:func}`receiving`, with the headers it arrived with, so handle middleware can read them.
Only handle middleware reads them, so an adapter without any can skip `receiving`.
"""

from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar, Token
from types import MappingProxyType
from typing import Any

from stratae.callables import is_async_callable
from stratae.events.handler import WithoutPayload
from stratae.events.hooks import Hooks, run_hooks, run_hooks_async
from stratae.events.middleware import (
    HandleCall,
    MessageHeaders,
    PublishCall,
    run_middleware,
    run_middleware_async,
)
from stratae.events.protocols import AnyEvent

_NO_HEADERS: Mapping[str, str] = MappingProxyType({})

_received: ContextVar[Mapping[str, str]] = ContextVar("_received_headers")


class _Receiving:
    """
    The context manager {py:func}`receiving` returns.

    A class rather than a `contextlib.contextmanager` generator: a bus with handle middleware
    opens one per emit, and a generator costs about twice as much to enter and exit.
    """

    __slots__ = ("_headers", "_token")

    def __init__(self, headers: Mapping[str, str]) -> None:
        self._headers = headers
        self._token: Token[Mapping[str, str]]

    def __enter__(self) -> None:
        self._token = _received.set(MappingProxyType(self._headers))

    def __exit__(self, *_: object) -> None:
        _received.reset(self._token)


def receiving(headers: Mapping[str, str]) -> _Receiving:
    """
    Make `headers` the headers handle middleware sees, for a block that handles a message.

    Handle middleware outside any `receiving` block sees no headers.

    :param headers: The headers the message arrived with, as a mapping the caller doesn't
        change afterwards. Handle middleware reads it read-only, without a copy.
    :returns: A context manager for the block.
    """
    return _Receiving(headers)


def run_emit(
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[Callable[..., object], ...],
    event: Any,
    config: Any,
    payload: Any,
    headers: MessageHeaders,
    call: Callable[..., Any],
    *args: Any,
) -> Any:
    """
    Run `call(*args)` inside the emit's sync hooks, then the bus's sync publish middleware.

    :param hooks: The emit's hooks, or `None`.
    :param middleware: The bus's publish middleware, outermost first.
    :param event: The event being emitted.
    :param config: The emit's routing config, which middleware sees as `call.config`.
    :param payload: The payload being emitted.
    :param headers: The message's headers, which middleware adds to as `call.headers`. `call`
        sends them.
    :param call: The emit itself, such as a dispatch.
    :returns: What `call` returns.
    """
    if hooks is not None:
        call, args = run_hooks, (hooks, event, payload, call, *args)
    if middleware:
        seen = PublishCall(event, config, payload, headers)
        call, args = run_middleware, (middleware, seen, call, *args)
    return call(*args)


async def run_emit_async(
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[Callable[..., object], ...],
    event: Any,
    config: Any,
    payload: Any,
    headers: MessageHeaders,
    call: Callable[..., Awaitable[Any]],
    *args: Any,
) -> Any:
    """
    Await `call(*args)` inside the emit's hooks, then the bus's async publish middleware.

    :param hooks: The emit's hooks, or `None`.
    :param middleware: The bus's publish middleware, outermost first.
    :param event: The event being emitted.
    :param config: The emit's routing config, which middleware sees as `call.config`.
    :param payload: The payload being emitted.
    :param headers: The message's headers, which middleware adds to as `call.headers`. `call`
        sends them.
    :param call: The emit itself, such as a dispatch or a publish.
    :returns: What `call` resolves to.
    """
    if hooks is not None:
        call, args = run_hooks_async, (hooks, event, payload, call, *args)
    if middleware:
        seen = PublishCall(event, config, payload, headers)
        call, args = run_middleware_async, (middleware, seen, call, *args)
    return await call(*args)


def _hooked(
    hooks: Hooks[Any, Any], event: AnyEvent, call: Callable[[Any], Any]
) -> Callable[[Any], Any]:
    """Return `call` run inside sync hooks."""

    def hooked(payload: Any, /) -> Any:
        return run_hooks(hooks, event, payload, call, payload)

    return hooked


def _hooked_async(
    hooks: Hooks[Any, Any], event: AnyEvent, call: Callable[[Any], Awaitable[Any]]
) -> Callable[[Any], Awaitable[Any]]:
    """Return `call` run inside hooks, sync or async, as an awaitable."""

    def hooked(payload: Any, /) -> Awaitable[Any]:
        return run_hooks_async(hooks, event, payload, call, payload)

    return hooked


def _handled(
    middleware: tuple[Callable[..., object], ...],
    event: AnyEvent,
    config: Any,
    call: Callable[[Any], Any],
) -> Callable[[Any], Any]:
    """Return `call` run inside sync handle middleware, which sees the received headers."""

    def handled(payload: Any, /) -> Any:
        seen = HandleCall(event, config, payload, _received.get(_NO_HEADERS))
        return run_middleware(middleware, seen, call, payload)

    return handled


def _handled_async(
    middleware: tuple[Callable[..., object], ...],
    event: AnyEvent,
    config: Any,
    call: Callable[[Any], Awaitable[Any]],
) -> Callable[[Any], Awaitable[Any]]:
    """Return `call` run inside async handle middleware, which sees the received headers."""

    def handled(payload: Any, /) -> Awaitable[Any]:
        seen = HandleCall(event, config, payload, _received.get(_NO_HEADERS))
        return run_middleware_async(middleware, seen, call, payload)

    return handled


class _Awaitable:
    """
    A sync handler call, made awaitable for an async runner. It runs when awaited.

    A class, since an awaitable call with nothing to await is a method: a function would be an
    `async def` that never awaits.
    """

    __slots__ = ("call",)

    def __init__(self, call: Callable[[Any], Any]) -> None:
        self.call = call

    async def __call__(self, payload: Any, /) -> Any:
        return self.call(payload)


def wrap_handler(
    event: AnyEvent,
    config: Any,
    fn: Callable[..., Any],
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[Callable[..., object], ...],
    asynchronous: bool,
) -> tuple[Callable[[Any], Any], bool]:
    """
    Build the call an adapter registers for a handler: the handler, its hooks, then middleware.

    :param event: The event the handler is for, which hooks and middleware see.
    :param config: The registration's routing config, which middleware sees as `call.config`.
    :param fn: The handler, taking the payload, or nothing for a payload-less event.
    :param hooks: The handler's hooks, or `None`.
    :param middleware: The adapter's handle middleware, outermost first.
    :param asynchronous: Whether the adapter runs middleware asynchronously.
    :returns: The call to register, taking the payload, and whether it's async.
    """
    call: Callable[[Any], Any] = WithoutPayload(fn) if event.is_signal else fn
    is_async = is_async_callable(fn)
    if hooks is not None:
        if is_async or hooks.has_async:
            call = _hooked_async(hooks, event, call if is_async else _Awaitable(call))
            is_async = True
        else:
            call = _hooked(hooks, event, call)
    if middleware:
        if asynchronous:
            call = _handled_async(middleware, event, config, call if is_async else _Awaitable(call))
            is_async = True
        else:
            call = _handled(middleware, event, config, call)
    return call, is_async
