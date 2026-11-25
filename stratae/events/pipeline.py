"""
Compose hooks and middleware around an adapter's emits and handlers.

Every adapter runs these, so hooks and middleware behave the same on every bus: middleware
wraps hooks, which wrap the call itself. An adapter of your own, such as one for another
broker, uses them the same way.

Its `emit` calls one of four functions with its `send`. An adapter that opens an
{py:class}`Envelope <stratae.events.envelope.Envelope>` for each emit calls
{py:func}`publish_enveloped`, or {py:func}`publish_enveloped_async`, which opens it first. One
that opens none calls {py:func}`publish` or {py:func}`publish_async`. Each builds the headers
publish middleware can add to, runs the middleware and hooks, then calls `send` with the
{py:class}`Outgoing` message. The adapter writes only `send`: it delivers the message, with
its headers, however its transport does. The message is typed by the adapter's config and by
its envelope, so `send` reads both as they are.

With each emit, the adapter passes the header names its route reserves, which
{py:func}`reserved_for <stratae.events.protocols.reserved_for>` works out once for each route
from its config. A header with one of those names, given to `emit` or added by publish
middleware, is refused once, after the middleware runs and before the emit's hooks and `send`.

Its `handle` registers the call {py:func}`wrap_handler` builds as a
{py:class}`Handler <stratae.events.handler.Handler>`. That call takes the message being
handled, a {py:class}`HandleCall <stratae.events.middleware.HandleCall>` holding its event,
config, payload and headers, whatever the event. Handle middleware sees that message, hooks
see its payload, and the handler is called with its payload, or with nothing for a
payload-less event. An adapter builds the message once for each one it handles, and calls the
handler with {py:func}`invoke <stratae.events.handler.invoke>`.
"""

from collections.abc import Awaitable, Callable, Coroutine, Mapping
from types import MappingProxyType
from typing import Any, NamedTuple
from uuid import UUID

from stratae.callables import is_async_callable
from stratae.events.envelope import Envelope
from stratae.events.handler import AsyncHandler, Handler, SyncHandler
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


class Outgoing[Config, Enveloped: Envelope | None](NamedTuple):
    """
    One message on its way out: what an adapter's `send` receives.

    Typed by the adapter's config, and by its envelope: an
    {py:class}`Envelope <stratae.events.envelope.Envelope>` from an adapter that opens one for
    each emit, or `None` from one that opens none.
    """

    event: AnyEvent
    """The event being emitted."""
    config: Config
    """The config of the route it's sent on, or `None` on a bus that doesn't route."""
    payload: Any
    """The payload being emitted."""
    envelope: Enveloped
    """The envelope it's sent under, or `None` from an adapter that opens none."""
    headers: Mapping[str, object]
    """The headers to send with it: those given, plus any publish middleware added."""


type Send[Config, Enveloped: Envelope | None] = Callable[[Outgoing[Config, Enveloped]], Any]
"""Sends one message: the only part of an emit an adapter writes."""


def _refuse_reserved(headers: Mapping[str, object], reserved: frozenset[str]) -> None:
    """
    Refuse an emit whose headers hold a name its route reserves.

    The one place an emit's headers are checked: after publish middleware, before the emit's
    hooks and `send`.

    :raises ValueError: If a header's name is in `reserved`, compared ignoring case.
    """
    for name in headers:
        if name.lower() in reserved:
            raise ValueError(_RESERVED.format(name))


type _Sending[Config, Enveloped: Envelope | None] = tuple[
    Hooks[Any, Any] | None,
    Send[Config, Enveloped],
    Outgoing[Config, Enveloped],
    frozenset[str],
]
"""
What a publish chain finishes an emit with: its hooks, how to send, the message, and the
header names its route reserves.
"""


def _sent[Config, Enveloped: Envelope | None](
    call: PublishCall, message: Outgoing[Config, Enveloped]
) -> Outgoing[Config, Enveloped]:
    """
    Return `message` with the payload and headers publish middleware passed on.

    :raises ValueError: If the middleware changed the event or config.
    """
    event, config, payload, headers = call
    if event is not message.event or config is not message.config:
        raise ValueError(_REROUTED)
    if headers is message.headers and payload is message.payload:
        return message
    sent: Outgoing[Config, Enveloped] = from_fields(
        Outgoing, (event, message.config, payload, message.envelope, headers)
    )
    return sent


def _finish_send[Config, Enveloped: Envelope | None](
    call: PublishCall, sending: _Sending[Config, Enveloped]
) -> Any:
    """Send what publish middleware passed on, inside the emit's hooks."""
    hooks, send, message, reserved = sending
    return _hooked_send(hooks, send, _sent(call, message), reserved)


def _finish_send_async[Config, Enveloped: Envelope | None](
    call: PublishCall, sending: _Sending[Config, Enveloped]
) -> Awaitable[Any]:
    """Send what publish middleware passed on, as {py:func}`_finish_send` does, to await."""
    hooks, send, message, reserved = sending
    return _hooked_send_async(hooks, send, _sent(call, message), reserved)


def _hooked_send[Config, Enveloped: Envelope | None](
    hooks: Hooks[Any, Any] | None,
    send: Send[Config, Enveloped],
    message: Outgoing[Config, Enveloped],
    reserved: frozenset[str],
) -> Any:
    """Refuse `message` if a header's name is reserved, then send it inside any sync hooks."""
    if message.headers:
        _refuse_reserved(message.headers, reserved)
    if hooks is None:
        return send(message)
    return run_hooks(hooks, message.event, message.payload, send, message)


def _hooked_send_async[Config, Enveloped: Envelope | None](
    hooks: Hooks[Any, Any] | None,
    send: Send[Config, Enveloped],
    message: Outgoing[Config, Enveloped],
    reserved: frozenset[str],
) -> Awaitable[Any]:
    """Send `message` inside the emit's hooks, sync or async, as {py:func}`_hooked_send` does."""
    if message.headers:
        _refuse_reserved(message.headers, reserved)
    if hooks is None:
        return send(message)
    return run_hooks_async(hooks, message.event, message.payload, send, message)


class Publishing[Config, Enveloped: Envelope | None]:
    """
    How an adapter emits: its publish middleware.

    Typed by the adapter's config, and by the envelope it sends each message under: `Envelope`
    for an adapter that emits with {py:func}`publish_enveloped`, or `None` for one that emits
    with {py:func}`publish`.
    """

    __slots__ = ("chained", "middleware")

    middleware: tuple[PublishMiddleware, ...]
    """The adapter's publish middleware, outermost first."""
    chained: Callable[[PublishCall, _Sending[Config, Enveloped]], Any]
    """The middleware composed around sending, built once."""

    def __init__(
        self, middleware: tuple[PublishMiddleware, ...], *, asynchronous: bool = False
    ) -> None:
        """
        Describe how an adapter emits.

        :param middleware: The adapter's publish middleware, outermost first.
        :param asynchronous: Whether the adapter emits asynchronously.
        """
        self.middleware = middleware
        if asynchronous:
            self.chained = chain(middleware, _finish_send_async)
        else:
            self.chained = chain(middleware, _finish_send)


def _stamped(headers: Mapping[str, object] | None) -> Mapping[str, object]:
    """Return the headers an emit starts with, read-only."""
    if not headers:
        return NO_HEADERS
    return MappingProxyType(dict(headers))


def _emit[Config, Enveloped: Envelope | None](
    publishing: Publishing[Config, Enveloped],
    hooks: Hooks[Any, Any] | None,
    message: Outgoing[Config, Enveloped],
    send: Send[Config, Enveloped],
    reserved: frozenset[str],
) -> Any:
    """Send `message` inside the emit's sync hooks, then the adapter's sync publish middleware."""
    if publishing.middleware:
        call = from_fields(PublishCall, (*message[:3], message.headers))
        return publishing.chained(call, (hooks, send, message, reserved))
    return _hooked_send(hooks, send, message, reserved)


def _emit_async[Config, Enveloped: Envelope | None](
    publishing: Publishing[Config, Enveloped],
    hooks: Hooks[Any, Any] | None,
    message: Outgoing[Config, Enveloped],
    send: Send[Config, Enveloped],
    reserved: frozenset[str],
) -> Awaitable[Any]:
    """Return `send(message)` inside the emit's hooks, then async publish middleware, to await."""
    if publishing.middleware:
        call = from_fields(PublishCall, (*message[:3], message.headers))
        return publishing.chained(call, (hooks, send, message, reserved))
    return _hooked_send_async(hooks, send, message, reserved)


def publish[Config](
    publishing: Publishing[Config, None],
    send: Send[Config, None],
    reserved: frozenset[str],
    event: AnyEvent,
    payload: Any,
    config: Config,
    hooks: Hooks[Any, Any] | None = None,
    headers: Mapping[str, object] | None = None,
) -> Any:
    """
    Emit through a sync adapter that opens no envelope: run middleware and hooks, then `send`.

    :param publishing: How the adapter emits.
    :param send: Sends the message.
    :param reserved: The header names the route reserves, from
        {py:func}`reserved_for <stratae.events.protocols.reserved_for>`.
    :param event: The event being emitted.
    :param payload: The payload being emitted.
    :param config: The config of the route the message is sent on.
    :param hooks: The emit's hooks, or `None`.
    :param headers: Headers to send with the message, which publish middleware can add to.
    :returns: What `send` returns.
    :raises ValueError: If a header, given or added by publish middleware, is reserved. It's
        refused after publish middleware runs, before the emit's hooks and `send`.
    """
    if hooks is None and headers is None and not publishing.middleware:
        plain: Outgoing[Config, None] = from_fields(
            Outgoing, (event, config, payload, None, NO_HEADERS)
        )
        return send(plain)
    stamped = _stamped(headers)
    message: Outgoing[Config, None] = from_fields(Outgoing, (event, config, payload, None, stamped))
    return _emit(publishing, hooks, message, send, reserved)


async def publish_async[Config](
    publishing: Publishing[Config, None],
    send: Callable[[Outgoing[Config, None]], Awaitable[Any]],
    reserved: frozenset[str],
    event: AnyEvent,
    payload: Any,
    config: Config,
    hooks: Hooks[Any, Any] | None = None,
    headers: Mapping[str, object] | None = None,
) -> Any:
    """Emit through an async adapter that opens no envelope, as {py:func}`publish` does."""
    if hooks is None and headers is None and not publishing.middleware:
        plain: Outgoing[Config, None] = from_fields(
            Outgoing, (event, config, payload, None, NO_HEADERS)
        )
        return await send(plain)
    stamped = _stamped(headers)
    message: Outgoing[Config, None] = from_fields(Outgoing, (event, config, payload, None, stamped))
    return await _emit_async(publishing, hooks, message, send, reserved)


def publish_enveloped[Config](
    publishing: Publishing[Config, Envelope],
    new_id: Callable[[], UUID],
    send: Send[Config, Envelope],
    reserved: frozenset[str],
    event: AnyEvent,
    payload: Any,
    config: Config,
    hooks: Hooks[Any, Any] | None = None,
    envelope: Envelope | None = None,
    headers: Mapping[str, object] | None = None,
) -> Any:
    """
    Emit through a sync adapter inside an envelope, then run middleware and hooks, and `send`.

    The envelope is the one given, or a child of the active one, or a new root.

    :param publishing: How the adapter emits.
    :param new_id: Makes the ids of a new envelope.
    :param send: Sends the message.
    :param reserved: The header names the route reserves, from
        {py:func}`reserved_for <stratae.events.protocols.reserved_for>`.
    :param event: The event being emitted.
    :param payload: The payload being emitted.
    :param config: The config of the route the message is sent on.
    :param hooks: The emit's hooks, or `None`.
    :param envelope: Sends under this envelope, unchanged, instead of a new one.
    :param headers: Headers to send with the message, which publish middleware can add to.
    :returns: What `send` returns.
    :raises ValueError: If a header, given or added by publish middleware, is reserved. It's
        refused after publish middleware runs, before the emit's hooks and `send`.
    """
    stamped = _stamped(headers)
    with Envelope.scope(envelope, new_id=new_id) as opened:
        message: Outgoing[Config, Envelope] = from_fields(
            Outgoing, (event, config, payload, opened, stamped)
        )
        return _emit(publishing, hooks, message, send, reserved)


async def publish_enveloped_async[Config](
    publishing: Publishing[Config, Envelope],
    new_id: Callable[[], UUID],
    send: Callable[[Outgoing[Config, Envelope]], Awaitable[Any]],
    reserved: frozenset[str],
    event: AnyEvent,
    payload: Any,
    config: Config,
    hooks: Hooks[Any, Any] | None = None,
    envelope: Envelope | None = None,
    headers: Mapping[str, object] | None = None,
) -> Any:
    """Emit through an async adapter inside an envelope, as {py:func}`publish_enveloped` does."""
    stamped = _stamped(headers)
    with Envelope.scope(envelope, new_id=new_id) as opened:
        message: Outgoing[Config, Envelope] = from_fields(
            Outgoing, (event, config, payload, opened, stamped)
        )
        return await _emit_async(publishing, hooks, message, send, reserved)


def _payload_of[Result](fn: Callable[[Any], Result]) -> Callable[[HandleCall], Result]:
    """Return `fn`, called with the payload of the message it's given."""

    def unpacked(message: HandleCall, /) -> Result:
        return fn(message.payload)

    return unpacked


def _without_payload[Result](fn: Callable[[], Result]) -> Callable[[HandleCall], Result]:
    """Return a payload-less event's `fn`, called with nothing for the message it's given."""

    def unpacked(_: HandleCall, /) -> Result:
        return fn()

    return unpacked


def _unpacking[Result](
    event: AnyEvent, fn: Callable[..., Result]
) -> Callable[[HandleCall], Result]:
    """Return `fn`, called with the message's payload, or with nothing for a payload-less event."""
    return _without_payload(fn) if event.is_signal else _payload_of(fn)


def _hooked[Result](
    hooks: Hooks[Any, Result], event: AnyEvent, call: Callable[[HandleCall], Result]
) -> Callable[[HandleCall], Result]:
    """Return `call` run inside sync hooks, which see the message's payload."""

    def hooked(message: HandleCall, /) -> Result:
        return run_hooks(hooks, event, message.payload, call, message)

    return hooked


def _hooked_async[Result](
    hooks: Hooks[Any, Result],
    event: AnyEvent,
    call: Callable[[HandleCall], Coroutine[Any, Any, Result]],
) -> Callable[[HandleCall], Coroutine[Any, Any, Result]]:
    """Return `call` run inside hooks, sync or async, as a coroutine."""

    def hooked(message: HandleCall, /) -> Coroutine[Any, Any, Result]:
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


def _handled[Result](
    middleware: tuple[HandleMiddleware, ...], call: Callable[[HandleCall], Result]
) -> Callable[[HandleCall], Result]:
    """Return `call` run inside handle middleware, sync or async, which sees the message."""

    def finish(seen: HandleCall, received: HandleCall) -> Result:
        if seen is not received:
            _refuse_relabelled(seen, received)
        return call(seen)

    chained = chain(middleware, finish)

    def handled(message: HandleCall, /) -> Result:
        return chained(message, message)

    return handled


class _Awaitable[Result]:
    """
    A sync handler call, made awaitable for an async runner. It runs when awaited.

    A class, since an awaitable call with nothing to await is a method: a function would be an
    `async def` that never awaits.
    """

    __slots__ = ("call",)

    def __init__(self, call: Callable[[HandleCall], Result]) -> None:
        self.call = call

    async def __call__(self, message: HandleCall, /) -> Result:
        return self.call(message)


def wrap_handler[Config](
    event: AnyEvent,
    fn: Callable[..., Any],
    config: Config,
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[HandleMiddleware, ...],
    asynchronous: bool,
) -> Handler[Config, Any]:
    """
    Build the handler an adapter registers: the function, its hooks, then middleware.

    It's async if the function is, if any hook is, or if the adapter runs its middleware
    asynchronously. Otherwise it's sync.

    :param event: The event the handler is for, which hooks see.
    :param fn: The handler, taking the payload, or nothing for a payload-less event.
    :param config: The handler's routing config.
    :param hooks: The handler's hooks, or `None`.
    :param middleware: The adapter's handle middleware, outermost first.
    :param asynchronous: Whether the adapter runs middleware asynchronously.
    :returns: The handler to register, taking the message being handled.
    """
    if is_async_callable(fn):
        return _async_handler(event, _unpacking(event, fn), config, hooks, middleware)
    call = _unpacking(event, fn)
    if hooks is not None and hooks.has_async:
        return _async_handler(event, _Awaitable(call), config, hooks, middleware)
    if hooks is not None:
        call = _hooked(hooks, event, call)
    if middleware:
        if asynchronous:
            return AsyncHandler(_handled(middleware, _Awaitable(call)), config)
        call = _handled(middleware, call)
    return SyncHandler(call, config)


def _async_handler[Config](
    event: AnyEvent,
    call: Callable[[HandleCall], Coroutine[Any, Any, Any]],
    config: Config,
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[HandleMiddleware, ...],
) -> AsyncHandler[Config, Any]:
    """Return an async call as a handler, inside its hooks, then the adapter's middleware."""
    if hooks is not None:
        call = _hooked_async(hooks, event, call)
    if middleware:
        call = _handled(middleware, call)
    return AsyncHandler(call, config)
