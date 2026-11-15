"""
Compose hooks and middleware around an adapter's emits and handlers.

Every adapter runs these, so hooks and middleware behave the same on every bus: middleware
wraps hooks, which wrap the call itself. An adapter of your own, such as one for another
broker, uses them the same way.

Its `emit` calls {py:func}`publish` or {py:func}`publish_async` with its `send`. That opens
the message's envelope, and builds the headers publish middleware can add to. It runs the
middleware and hooks, then calls `send` with the {py:class}`Outgoing` message. The adapter
writes only `send`: it delivers the message, with its headers, however its transport does.

Its `handle` registers the call {py:func}`wrap_handler` builds as a
{py:class}`Handler <stratae.events.handler.Handler>`. That call takes the message being
handled, a {py:class}`HandleCall <stratae.events.middleware.HandleCall>` holding its event,
config, payload and headers, whatever the event. Handle middleware sees that message, hooks
see its payload, and the handler is called with its payload, or with nothing for a
payload-less event. An adapter builds the message once for each one it handles, and calls the
handler with {py:func}`invoke <stratae.events.handler.invoke>`.
"""

from collections.abc import Awaitable, Callable, Collection, Mapping
from contextlib import AbstractContextManager
from types import MappingProxyType
from typing import Any, NamedTuple
from uuid import UUID

from stratae.callables import is_async_callable
from stratae.events.envelope import ENVELOPE_HEADERS, Envelope
from stratae.events.hooks import Hooks, run_hooks, run_hooks_async
from stratae.events.middleware import (
    NO_HEADERS,
    HandleCall,
    HandleMiddleware,
    PublishCall,
    PublishMiddleware,
    from_fields,
)
from stratae.events.protocols import AnyEvent
from stratae.middleware import chain

_RESERVED = "{!r} is a header the bus sets itself, so it can't be given or added"

_REROUTED = (
    "publish middleware passed on a call with another event or config, but they name the "
    "route, which is chosen before middleware runs"
)

_RELABELLED = (
    "handle middleware passed on a call with another event, config or headers, but they "
    "describe the message being handled"
)


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
    headers: Mapping[str, object]
    """The headers to send with it: those given, plus any publish middleware added."""


type Send = Callable[[Outgoing], Any]
"""Sends one message: the only part of an emit an adapter writes."""


def _refuse_reserved(headers: Mapping[str, object], reserved: Collection[str]) -> None:
    """
    Refuse a header the adapter sets itself.

    :raises ValueError: If a name is in `reserved`, compared ignoring case.
    """
    for name in headers:
        if name.lower() in reserved:
            raise ValueError(_RESERVED.format(name))


type _Sending = tuple[Hooks[Any, Any] | None, Callable[[Outgoing], Any], Outgoing, Collection[str]]
"""
What a publish chain finishes an emit with: its hooks, how to send, the message, and the
header names the adapter sets itself.
"""


def _sent(call: PublishCall, message: Outgoing, reserved: Collection[str]) -> Outgoing:
    """
    Return `message` with the payload and headers publish middleware passed on.

    :raises ValueError: If the middleware changed the event or config, or changed the headers
        to hold a reserved one.
    """
    event, config, payload, headers = call
    if event is not message.event or config is not message.config:
        raise ValueError(_REROUTED)
    if headers is not message.headers:
        _refuse_reserved(headers, reserved)
    elif payload is message.payload:
        return message
    return from_fields(Outgoing, (event, config, payload, message.envelope, headers))


def _finish_send(call: PublishCall, sending: _Sending) -> Any:
    """Send what publish middleware passed on, inside the emit's hooks."""
    hooks, send, message, reserved = sending
    return _hooked_send(hooks, send, _sent(call, message, reserved))


def _finish_send_async(call: PublishCall, sending: _Sending) -> Awaitable[Any]:
    """Send what publish middleware passed on, as {py:func}`_finish_send` does, to await."""
    hooks, send, message, reserved = sending
    return _hooked_send_async(hooks, send, _sent(call, message, reserved))


def _hooked_send(hooks: Hooks[Any, Any] | None, send: Send, message: Outgoing) -> Any:
    """Send `message` inside the emit's sync hooks, if it has any."""
    if hooks is None:
        return send(message)
    return run_hooks(hooks, message.event, message.payload, send, message)


def _hooked_send_async(
    hooks: Hooks[Any, Any] | None, send: Callable[[Outgoing], Awaitable[Any]], message: Outgoing
) -> Awaitable[Any]:
    """Send `message` inside the emit's hooks, sync or async, if it has any."""
    if hooks is None:
        return send(message)
    return run_hooks_async(hooks, message.event, message.payload, send, message)


class Publishing:
    """How an adapter emits: its publish middleware, envelopes and reserved header names."""

    __slots__ = ("bare", "chained", "middleware", "new_id", "reserved")

    middleware: tuple[PublishMiddleware, ...]
    """The adapter's publish middleware, outermost first."""
    chained: Callable[[PublishCall, _Sending], Any]
    """The middleware composed around sending, built once."""
    new_id: Callable[[], UUID] | None
    """Makes the ids of each emit's new envelope; `None` opens none unless one is given."""
    reserved: Collection[str]
    """Header names the adapter sets itself, in lower case."""
    bare: bool
    """Whether an emit has nothing to open or stamp unless it's given an envelope or headers."""

    def __init__(
        self,
        middleware: tuple[PublishMiddleware, ...] = (),
        new_id: Callable[[], UUID] | None = None,
        reserved: Collection[str] = ENVELOPE_HEADERS,
        *,
        asynchronous: bool = False,
    ) -> None:
        """
        Describe how an adapter emits.

        :param middleware: The adapter's publish middleware, outermost first.
        :param new_id: Makes the ids of each emit's new envelope. `None` opens no envelope
            unless one is given.
        :param reserved: Header names the adapter sets itself, in lower case.
        :param asynchronous: Whether the adapter emits asynchronously.
        """
        self.middleware = middleware
        if asynchronous:
            self.chained = chain(middleware, _finish_send_async)
        else:
            self.chained = chain(middleware, _finish_send)
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

    def stamped(self, headers: Mapping[str, object] | None) -> Mapping[str, object]:
        """
        Return the headers an emit starts with, read-only.

        :raises ValueError: If a given header is one the adapter sets itself.
        """
        if not headers:
            return NO_HEADERS
        _refuse_reserved(headers, self.reserved)
        return MappingProxyType(dict(headers))


def _emit(
    publishing: Publishing, hooks: Hooks[Any, Any] | None, message: Outgoing, send: Send
) -> Any:
    """Send `message` inside the emit's sync hooks, then the adapter's sync publish middleware."""
    if publishing.middleware:
        call = from_fields(PublishCall, (*message[:3], message.headers))
        return publishing.chained(call, (hooks, send, message, publishing.reserved))
    return _hooked_send(hooks, send, message)


def _emit_async(
    publishing: Publishing,
    hooks: Hooks[Any, Any] | None,
    message: Outgoing,
    send: Callable[[Outgoing], Awaitable[Any]],
) -> Awaitable[Any]:
    """Return `send(message)` inside the emit's hooks, then async publish middleware, to await."""
    if publishing.middleware:
        call = from_fields(PublishCall, (*message[:3], message.headers))
        return publishing.chained(call, (hooks, send, message, publishing.reserved))
    return _hooked_send_async(hooks, send, message)


def publish(
    publishing: Publishing,
    send: Send,
    event: AnyEvent,
    payload: Any,
    config: Any = None,
    hooks: Hooks[Any, Any] | None = None,
    envelope: Envelope | None = None,
    headers: Mapping[str, object] | None = None,
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
        return send(from_fields(Outgoing, (event, config, payload, None, NO_HEADERS)))
    stamped = publishing.stamped(headers)
    scope = publishing.opened(envelope)
    if scope is None:
        message = from_fields(Outgoing, (event, config, payload, None, stamped))
        return _emit(publishing, hooks, message, send)
    with scope as opened:
        message = from_fields(Outgoing, (event, config, payload, opened, stamped))
        return _emit(publishing, hooks, message, send)


async def publish_async(
    publishing: Publishing,
    send: Callable[[Outgoing], Awaitable[Any]],
    event: AnyEvent,
    payload: Any,
    config: Any = None,
    hooks: Hooks[Any, Any] | None = None,
    envelope: Envelope | None = None,
    headers: Mapping[str, object] | None = None,
) -> Any:
    """Emit through an async adapter, as {py:func}`publish` does, awaiting `send`."""
    if publishing.bare and hooks is None and envelope is None and headers is None:
        return await send(from_fields(Outgoing, (event, config, payload, None, NO_HEADERS)))
    stamped = publishing.stamped(headers)
    scope = publishing.opened(envelope)
    if scope is None:
        message = from_fields(Outgoing, (event, config, payload, None, stamped))
        return await _emit_async(publishing, hooks, message, send)
    with scope as opened:
        message = from_fields(Outgoing, (event, config, payload, opened, stamped))
        return await _emit_async(publishing, hooks, message, send)


def _payload_of(fn: Callable[[Any], Any]) -> Callable[[HandleCall], Any]:
    """Return `fn`, called with the payload of the message it's given."""

    def unpacked(message: HandleCall, /) -> Any:
        return fn(message.payload)

    return unpacked


def _without_payload(fn: Callable[[], Any]) -> Callable[[HandleCall], Any]:
    """Return a payload-less event's `fn`, called with nothing for the message it's given."""

    def unpacked(_: HandleCall, /) -> Any:
        return fn()

    return unpacked


def _hooked(
    hooks: Hooks[Any, Any], event: AnyEvent, call: Callable[[HandleCall], Any]
) -> Callable[[HandleCall], Any]:
    """Return `call` run inside sync hooks, which see the message's payload."""

    def hooked(message: HandleCall, /) -> Any:
        return run_hooks(hooks, event, message.payload, call, message)

    return hooked


def _hooked_async(
    hooks: Hooks[Any, Any], event: AnyEvent, call: Callable[[HandleCall], Awaitable[Any]]
) -> Callable[[HandleCall], Awaitable[Any]]:
    """Return `call` run inside hooks, sync or async, as an awaitable."""

    def hooked(message: HandleCall, /) -> Awaitable[Any]:
        return run_hooks_async(hooks, event, message.payload, call, message)

    return hooked


def _refuse_relabelled(seen: HandleCall, received: HandleCall) -> None:
    """
    Refuse a call handle middleware passed on with another event, config or headers.

    :param seen: The call passed on.
    :param received: The call the chain was given.
    :raises ValueError: If any of them differs.
    """
    if (
        seen.event is not received.event
        or seen.config is not received.config
        or seen.headers is not received.headers
    ):
        raise ValueError(_RELABELLED)


def _handled(
    middleware: tuple[HandleMiddleware, ...], call: Callable[[HandleCall], Any]
) -> Callable[[HandleCall], Any]:
    """Return `call` run inside handle middleware, sync or async, which sees the message."""

    def finish(seen: HandleCall, received: HandleCall) -> Any:
        if seen is not received:
            _refuse_relabelled(seen, received)
        return call(seen)

    chained = chain(middleware, finish)

    def handled(message: HandleCall, /) -> Any:
        return chained(message, message)

    return handled


class _Awaitable:
    """
    A sync handler call, made awaitable for an async runner. It runs when awaited.

    A class, since an awaitable call with nothing to await is a method: a function would be an
    `async def` that never awaits.
    """

    __slots__ = ("call",)

    def __init__(self, call: Callable[[HandleCall], Any]) -> None:
        self.call = call

    async def __call__(self, message: HandleCall, /) -> Any:
        return self.call(message)


def wrap_handler(
    event: AnyEvent,
    fn: Callable[..., Any],
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[HandleMiddleware, ...],
    asynchronous: bool,
) -> tuple[Callable[[HandleCall], Any], bool]:
    """
    Build the call an adapter registers for a handler: the handler, its hooks, then middleware.

    :param event: The event the handler is for, which hooks see.
    :param fn: The handler, taking the payload, or nothing for a payload-less event.
    :param hooks: The handler's hooks, or `None`.
    :param middleware: The adapter's handle middleware, outermost first.
    :param asynchronous: Whether the adapter runs middleware asynchronously.
    :returns: The call to register, taking the message being handled, and whether it's async.
    """
    call = _without_payload(fn) if event.is_signal else _payload_of(fn)
    is_async = is_async_callable(fn)
    if hooks is not None:
        if is_async or hooks.has_async:
            call = _hooked_async(hooks, event, call if is_async else _Awaitable(call))
            is_async = True
        else:
            call = _hooked(hooks, event, call)
    if middleware:
        if asynchronous and not is_async:
            call = _Awaitable(call)
            is_async = True
        call = _handled(middleware, call)
    return call, is_async
