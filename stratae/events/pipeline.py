"""
Compose hooks and middleware around an adapter's emits.

Every adapter runs these, so hooks and middleware behave the same on every bus: middleware
wraps hooks, which wrap the send itself. An adapter of your own, such as one for another
broker, uses them the same way. The handle side is in
{py:mod}`handling <stratae.events.handling>`.

Its `emit` calls one of four functions with its `send`. An adapter that opens an
{py:class}`Envelope <stratae.events.envelope.Envelope>` for each emit calls
{py:func}`publish_enveloped`, or {py:func}`publish_enveloped_async`, which opens it first, with
the adapter's {py:class}`EnvelopeFactory <stratae.events.envelope.EnvelopeFactory>`. One
that opens none calls {py:func}`publish` or {py:func}`publish_async`. Each builds the headers
publish middleware can add to, runs the middleware and hooks, then calls `send` with the
{py:class}`Outgoing` message. The adapter writes only `send`: it delivers the message, with
its headers, however its transport does. The message is typed by the adapter's config and by
its envelope, so `send` reads both as they are.

With each emit, the adapter passes the header names its route reserves, which
{py:func}`reserved_for <stratae.events.protocols.reserved_for>` works out once for each route
from its config. A header with one of those names, given to `emit` or added by publish
middleware, is refused once, after the middleware runs and before the emit's hooks and `send`.
"""

from collections.abc import Awaitable, Callable, Mapping
from types import MappingProxyType
from typing import Any, NamedTuple

from stratae.events.envelope import Envelope, EnvelopeFactory
from stratae.events.hooks import Hooks, run_hooks, run_hooks_async
from stratae.events.middleware import (
    NO_HEADERS,
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


class Outgoing[PublishConfig, Enveloped: Envelope | None](NamedTuple):
    """
    One message on its way out: what an adapter's `send` receives.

    Typed by the adapter's config, and by its envelope: an
    {py:class}`Envelope <stratae.events.envelope.Envelope>` from an adapter that opens one for
    each emit, or `None` from one that opens none.
    """

    event: AnyEvent
    """The event being emitted."""
    config: PublishConfig
    """The config of the route it's sent on, or `None` on a bus that doesn't route."""
    payload: Any
    """The payload being emitted."""
    envelope: Enveloped
    """The envelope it's sent under, or `None` from an adapter that opens none."""
    headers: Mapping[str, object]
    """The headers to send with it: those given, plus any publish middleware added."""


type Send[PublishConfig, Enveloped: Envelope | None] = Callable[
    [Outgoing[PublishConfig, Enveloped]], Any
]
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


type _Sending[PublishConfig, Enveloped: Envelope | None] = tuple[
    Hooks[Any, Any] | None,
    Send[PublishConfig, Enveloped],
    Outgoing[PublishConfig, Enveloped],
    frozenset[str],
]
"""
What a publish chain finishes an emit with: its hooks, how to send, the message, and the
header names its route reserves.
"""


def _sent[PublishConfig, Enveloped: Envelope | None](
    call: PublishCall, message: Outgoing[PublishConfig, Enveloped]
) -> Outgoing[PublishConfig, Enveloped]:
    """
    Return `message` with the payload and headers publish middleware passed on.

    :raises ValueError: If the middleware changed the event or config.
    """
    event, config, payload, headers = call
    if event is not message.event or config is not message.config:
        raise ValueError(_REROUTED)
    if headers is message.headers and payload is message.payload:
        return message
    sent: Outgoing[PublishConfig, Enveloped] = from_fields(
        Outgoing, (event, message.config, payload, message.envelope, headers)
    )
    return sent


def _finish_send[PublishConfig, Enveloped: Envelope | None](
    call: PublishCall, sending: _Sending[PublishConfig, Enveloped]
) -> Any:
    """Send what publish middleware passed on, inside the emit's hooks."""
    hooks, send, message, reserved = sending
    return _hooked_send(hooks, send, _sent(call, message), reserved)


def _finish_send_async[PublishConfig, Enveloped: Envelope | None](
    call: PublishCall, sending: _Sending[PublishConfig, Enveloped]
) -> Awaitable[Any]:
    """Send what publish middleware passed on, as {py:func}`_finish_send` does, to await."""
    hooks, send, message, reserved = sending
    return _hooked_send_async(hooks, send, _sent(call, message), reserved)


def _hooked_send[PublishConfig, Enveloped: Envelope | None](
    hooks: Hooks[Any, Any] | None,
    send: Send[PublishConfig, Enveloped],
    message: Outgoing[PublishConfig, Enveloped],
    reserved: frozenset[str],
) -> Any:
    """Refuse `message` if a header's name is reserved, then send it inside any sync hooks."""
    if message.headers:
        _refuse_reserved(message.headers, reserved)
    if hooks is None:
        return send(message)
    return run_hooks(hooks, message.event, message.payload, send, message)


def _hooked_send_async[PublishConfig, Enveloped: Envelope | None](
    hooks: Hooks[Any, Any] | None,
    send: Send[PublishConfig, Enveloped],
    message: Outgoing[PublishConfig, Enveloped],
    reserved: frozenset[str],
) -> Awaitable[Any]:
    """Send `message` inside the emit's hooks, sync or async, as {py:func}`_hooked_send` does."""
    if message.headers:
        _refuse_reserved(message.headers, reserved)
    if hooks is None:
        return send(message)
    return run_hooks_async(hooks, message.event, message.payload, send, message)


class Publishing[PublishConfig, Enveloped: Envelope | None]:
    """
    How an adapter emits: its publish middleware.

    Typed by the adapter's config, and by the envelope it sends each message under: `Envelope`
    for an adapter that emits with {py:func}`publish_enveloped`, or `None` for one that emits
    with {py:func}`publish`.
    """

    __slots__ = ("chained", "middleware")

    middleware: tuple[PublishMiddleware, ...]
    """The adapter's publish middleware, outermost first."""
    chained: Callable[[PublishCall, _Sending[PublishConfig, Enveloped]], Any]
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


def _emit[PublishConfig, Enveloped: Envelope | None](
    publishing: Publishing[PublishConfig, Enveloped],
    hooks: Hooks[Any, Any] | None,
    message: Outgoing[PublishConfig, Enveloped],
    send: Send[PublishConfig, Enveloped],
    reserved: frozenset[str],
) -> Any:
    """Send `message` inside the emit's sync hooks, then the adapter's sync publish middleware."""
    if publishing.middleware:
        call = from_fields(PublishCall, (*message[:3], message.headers))
        return publishing.chained(call, (hooks, send, message, reserved))
    return _hooked_send(hooks, send, message, reserved)


def _emit_async[PublishConfig, Enveloped: Envelope | None](
    publishing: Publishing[PublishConfig, Enveloped],
    hooks: Hooks[Any, Any] | None,
    message: Outgoing[PublishConfig, Enveloped],
    send: Send[PublishConfig, Enveloped],
    reserved: frozenset[str],
) -> Awaitable[Any]:
    """Return `send(message)` inside the emit's hooks, then async publish middleware, to await."""
    if publishing.middleware:
        call = from_fields(PublishCall, (*message[:3], message.headers))
        return publishing.chained(call, (hooks, send, message, reserved))
    return _hooked_send_async(hooks, send, message, reserved)


def publish[PublishConfig](
    publishing: Publishing[PublishConfig, None],
    send: Send[PublishConfig, None],
    reserved: frozenset[str],
    event: AnyEvent,
    payload: Any,
    config: PublishConfig,
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
        plain: Outgoing[PublishConfig, None] = from_fields(
            Outgoing, (event, config, payload, None, NO_HEADERS)
        )
        return send(plain)
    stamped = _stamped(headers)
    message: Outgoing[PublishConfig, None] = from_fields(
        Outgoing, (event, config, payload, None, stamped)
    )
    return _emit(publishing, hooks, message, send, reserved)


async def publish_async[PublishConfig](
    publishing: Publishing[PublishConfig, None],
    send: Callable[[Outgoing[PublishConfig, None]], Awaitable[Any]],
    reserved: frozenset[str],
    event: AnyEvent,
    payload: Any,
    config: PublishConfig,
    hooks: Hooks[Any, Any] | None = None,
    headers: Mapping[str, object] | None = None,
) -> Any:
    """Emit through an async adapter that opens no envelope, as {py:func}`publish` does."""
    if hooks is None and headers is None and not publishing.middleware:
        plain: Outgoing[PublishConfig, None] = from_fields(
            Outgoing, (event, config, payload, None, NO_HEADERS)
        )
        return await send(plain)
    stamped = _stamped(headers)
    message: Outgoing[PublishConfig, None] = from_fields(
        Outgoing, (event, config, payload, None, stamped)
    )
    return await _emit_async(publishing, hooks, message, send, reserved)


def publish_enveloped[PublishConfig](
    publishing: Publishing[PublishConfig, Envelope],
    factory: EnvelopeFactory,
    send: Send[PublishConfig, Envelope],
    reserved: frozenset[str],
    event: AnyEvent,
    payload: Any,
    config: PublishConfig,
    hooks: Hooks[Any, Any] | None = None,
    envelope: Envelope | None = None,
    headers: Mapping[str, object] | None = None,
) -> Any:
    """
    Emit through a sync adapter inside an envelope, then run middleware and hooks, and `send`.

    The envelope is the one given, or a child of the active one, or a new root.

    :param publishing: How the adapter emits.
    :param factory: Makes the envelope, as its `scope` does.
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
    with factory.scope(envelope) as opened:
        message: Outgoing[PublishConfig, Envelope] = from_fields(
            Outgoing, (event, config, payload, opened, stamped)
        )
        return _emit(publishing, hooks, message, send, reserved)


async def publish_enveloped_async[PublishConfig](
    publishing: Publishing[PublishConfig, Envelope],
    factory: EnvelopeFactory,
    send: Callable[[Outgoing[PublishConfig, Envelope]], Awaitable[Any]],
    reserved: frozenset[str],
    event: AnyEvent,
    payload: Any,
    config: PublishConfig,
    hooks: Hooks[Any, Any] | None = None,
    envelope: Envelope | None = None,
    headers: Mapping[str, object] | None = None,
) -> Any:
    """Emit through an async adapter inside an envelope, as {py:func}`publish_enveloped` does."""
    stamped = _stamped(headers)
    with factory.scope(envelope) as opened:
        message: Outgoing[PublishConfig, Envelope] = from_fields(
            Outgoing, (event, config, payload, opened, stamped)
        )
        return await _emit_async(publishing, hooks, message, send, reserved)
