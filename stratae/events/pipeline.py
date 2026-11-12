"""
Compose hooks and middleware around an adapter's emits and handlers.

Every adapter runs these, so hooks and middleware behave the same on every bus: middleware
wraps hooks, which wrap the call itself. An adapter of your own, such as one for another
broker, uses them the same way.

Its `emit` calls {py:func}`publish` or {py:func}`publish_async` with its `send`. That opens
the message's envelope, and builds the headers publish middleware can add to. It runs the
middleware and hooks, then calls `send` with the {py:class}`Outgoing` message. The adapter
writes only `send`: it delivers the message, with its headers, however its transport does.

Its `handle` registers the call {py:func}`wrap_handler` builds, which takes the payload
whatever the event, as a {py:class}`Handler <stratae.events.handler.Handler>`, and calls it
with {py:func}`invoke <stratae.events.handler.invoke>`. It handles a message inside
{py:func}`receiving`, with the headers it arrived with, so handle middleware can read them.
Only handle middleware reads them, so an adapter without any can skip `receiving`.
"""

from collections.abc import Awaitable, Callable, Collection, Mapping
from contextlib import AbstractContextManager, nullcontext
from contextvars import ContextVar, Token
from types import MappingProxyType
from typing import Any, NamedTuple, cast
from uuid import UUID

from stratae.callables import is_async_callable
from stratae.events.envelope import ENVELOPE_HEADERS, Envelope
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

_UNCHANGED: AbstractContextManager[None] = nullcontext()
"""What `receiving` returns for no headers outside any message: it would change nothing."""


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


def receiving(headers: Mapping[str, str]) -> AbstractContextManager[None]:
    """
    Make `headers` the headers handle middleware sees, for a block that handles a message.

    Handle middleware outside any `receiving` block sees no headers.

    :param headers: The headers the message arrived with, as a mapping the caller doesn't
        change afterwards. Handle middleware reads it read-only, without a copy.
    :returns: A context manager for the block.
    """
    if not headers and _received.get(None) is None:
        return _UNCHANGED
    return _Receiving(headers)


class Outgoing(NamedTuple):
    """One message on its way out: what an adapter's `send` receives."""

    event: AnyEvent
    """The event being emitted."""
    config: Any
    """The config of the route it's sent on, or `None` on a bus that doesn't route."""
    payload: Any
    """The payload being emitted."""
    envelope: Envelope | None
    """The envelope it's sent under, or `None` on a bus without envelopes."""
    headers: Mapping[str, str]
    """The headers to send with it: those given, plus any publish middleware added."""


type Send = Callable[[Outgoing], Any]
"""Sends one message: the only part of an emit an adapter writes."""

_message: Callable[[type[Outgoing], tuple[Any, ...]], Outgoing] = tuple[Any, ...].__new__
"""Builds an `Outgoing` from a tuple of its fields."""


def run_emit(
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[Callable[..., object], ...],
    message: Outgoing,
    send: Send,
) -> Any:
    """
    Send `message` inside the emit's sync hooks, then the bus's sync publish middleware.

    :param hooks: The emit's hooks, or `None`.
    :param middleware: The bus's publish middleware, outermost first.
    :param message: The message, which middleware sees as a {py:class}`PublishCall
        <stratae.events.middleware.PublishCall>`.
    :param send: Sends the message.
    :returns: What `send` returns.
    """
    if middleware:
        seen = _publish_call(message)
        return run_middleware(middleware, seen, _hooked_send, hooks, send, message)
    if hooks is not None:
        return run_hooks(hooks, message.event, message.payload, send, message)
    return send(message)


def _hooked_send(hooks: Hooks[Any, Any] | None, send: Send, message: Outgoing) -> Any:
    """Send `message` inside the emit's sync hooks, if it has any."""
    if hooks is None:
        return send(message)
    return run_hooks(hooks, message.event, message.payload, send, message)


def run_emit_async(
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[Callable[..., object], ...],
    message: Outgoing,
    send: Callable[[Outgoing], Awaitable[Any]],
) -> Awaitable[Any]:
    """Return `send(message)` inside the emit's hooks, then async publish middleware, to await."""
    if middleware:
        seen = _publish_call(message)
        return run_middleware_async(middleware, seen, _hooked_send_async, hooks, send, message)
    if hooks is not None:
        return run_hooks_async(hooks, message.event, message.payload, send, message)
    return send(message)


def _hooked_send_async(
    hooks: Hooks[Any, Any] | None, send: Callable[[Outgoing], Awaitable[Any]], message: Outgoing
) -> Awaitable[Any]:
    """Send `message` inside the emit's hooks, sync or async, if it has any."""
    if hooks is None:
        return send(message)
    return run_hooks_async(hooks, message.event, message.payload, send, message)


def _publish_call(message: Outgoing) -> PublishCall:
    """Describe the message for publish middleware, which can add to its headers."""
    headers = cast(MessageHeaders, message.headers)
    return PublishCall(message.event, message.config, message.payload, headers)


class Publishing:
    """How an adapter emits: its publish middleware, envelopes and reserved header names."""

    __slots__ = ("bare", "middleware", "new_id", "reserved")

    middleware: tuple[Callable[..., object], ...]
    """The adapter's publish middleware, outermost first."""
    new_id: Callable[[], UUID] | None
    """Makes the ids of each emit's new envelope; `None` opens none unless one is given."""
    reserved: Collection[str]
    """Header names the adapter sets itself, in lower case."""
    bare: bool
    """Whether an emit has nothing to open or stamp unless it's given an envelope or headers."""

    def __init__(
        self,
        middleware: tuple[Callable[..., object], ...] = (),
        new_id: Callable[[], UUID] | None = None,
        reserved: Collection[str] = ENVELOPE_HEADERS,
    ) -> None:
        """
        Describe how an adapter emits.

        :param middleware: The adapter's publish middleware, outermost first.
        :param new_id: Makes the ids of each emit's new envelope. `None` opens no envelope
            unless one is given.
        :param reserved: Header names the adapter sets itself, in lower case.
        """
        self.middleware = middleware
        self.new_id = new_id
        self.reserved = reserved
        self.bare = not middleware and new_id is None

    def opened(self, envelope: Envelope | None) -> AbstractContextManager[Envelope] | None:
        """
        Return the envelope scope an emit runs in, or `None` if it runs in none.

        It's `envelope` if one is given, else a new envelope if the adapter opens them.
        """
        if envelope is not None:
            return Envelope.scope(envelope)
        new_id = self.new_id
        if new_id is None:
            return None
        return Envelope.scope(new_id=new_id)

    def stamped(self, headers: Mapping[str, str] | None) -> Mapping[str, str]:
        """Return the headers an emit sends: writable only if something can write them."""
        if headers is None and not self.middleware:
            return _NO_HEADERS
        return MessageHeaders(headers or _NO_HEADERS, reserved=self.reserved)


def publish(
    publishing: Publishing,
    send: Send,
    event: AnyEvent,
    payload: Any,
    config: Any = None,
    hooks: Hooks[Any, Any] | None = None,
    envelope: Envelope | None = None,
    headers: Mapping[str, str] | None = None,
) -> Any:
    """
    Emit through a sync adapter: open the envelope, run middleware and hooks, then `send`.

    :param publishing: How the adapter emits.
    :param send: Sends the message.
    :param event: The event being emitted.
    :param payload: The payload being emitted.
    :param config: The config of the route the message is sent on.
    :param hooks: The emit's hooks, or `None`.
    :param envelope: Sends under this envelope, unchanged, instead of a new one.
    :param headers: Headers to send with the message, which publish middleware can add to.
    :returns: What `send` returns.
    :raises ValueError: If a given header is reserved.
    """
    if publishing.bare and hooks is None and envelope is None and headers is None:
        return send(_message(Outgoing, (event, config, payload, None, _NO_HEADERS)))
    middleware, stamped = publishing.middleware, publishing.stamped(headers)
    scope = publishing.opened(envelope)
    if scope is None:
        message = _message(Outgoing, (event, config, payload, None, stamped))
        return run_emit(hooks, middleware, message, send)
    with scope as opened:
        message = _message(Outgoing, (event, config, payload, opened, stamped))
        return run_emit(hooks, middleware, message, send)


async def publish_async(
    publishing: Publishing,
    send: Callable[[Outgoing], Awaitable[Any]],
    event: AnyEvent,
    payload: Any,
    config: Any = None,
    hooks: Hooks[Any, Any] | None = None,
    envelope: Envelope | None = None,
    headers: Mapping[str, str] | None = None,
) -> Any:
    """Emit through an async adapter, as {py:func}`publish` does, awaiting `send`."""
    if publishing.bare and hooks is None and envelope is None and headers is None:
        return await send(_message(Outgoing, (event, config, payload, None, _NO_HEADERS)))
    middleware, stamped = publishing.middleware, publishing.stamped(headers)
    scope = publishing.opened(envelope)
    if scope is None:
        message = _message(Outgoing, (event, config, payload, None, stamped))
        return await run_emit_async(hooks, middleware, message, send)
    with scope as opened:
        message = _message(Outgoing, (event, config, payload, opened, stamped))
        return await run_emit_async(hooks, middleware, message, send)


def received(headers: Mapping[str, str]) -> Mapping[str, str]:
    """Return a sent message's headers as handle middleware reads them: fixed from now on."""
    return headers if headers is _NO_HEADERS else cast(MessageHeaders, headers).to_dict()


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
