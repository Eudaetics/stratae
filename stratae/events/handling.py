"""
Build the call an adapter registers for a handler: its function, inside hooks and middleware.

Every adapter runs this, so hooks and middleware behave the same on every bus: middleware
wraps hooks, which wrap the handler itself. An adapter of your own, such as one for another
broker, uses it the same way. The publish side is in
{py:mod}`pipeline <stratae.events.pipeline>`.

An adapter's `handle` registers the call {py:func}`wrap_handler` builds as a
{py:class}`Handler <stratae.events.handler.Handler>`. That call takes the message being
handled, a {py:class}`HandleCall <stratae.events.middleware.HandleCall>` holding its event,
config, payload and headers, whatever the event. Handle middleware sees that message, hooks
see its payload, and the handler is called with its payload, or with nothing for a
payload-less event. An adapter builds the message once for each one it handles, and calls the
handler with {py:func}`invoke <stratae.events.handler.invoke>`.
"""

from collections.abc import Callable, Coroutine
from typing import Any

from stratae.callables import is_async_callable
from stratae.events.handler import AsyncHandler, Handler, SyncHandler
from stratae.events.hooks import Hooks, run_hooks, run_hooks_async
from stratae.events.middleware import HandleCall, HandleMiddleware
from stratae.events.protocols import AnyEvent
from stratae.middleware import chain

_RELABELLED = (
    "handle middleware passed on a call with another event, config or headers, but they "
    "describe the message being handled"
)

_NO_HOOKS: Hooks[Any, Any] = Hooks()
"""Empty hooks, which a handler registers as no hooks."""


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


def wrap_handler[HandlerConfig](
    event: AnyEvent,
    fn: Callable[..., Any],
    config: HandlerConfig,
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[HandleMiddleware, ...],
    asynchronous: bool,
) -> Handler[HandlerConfig, Any, ..., Any]:
    """
    Build the handler an adapter registers: the function, its hooks, then middleware.

    It's async if the function is, if any hook is, or if the adapter runs its middleware
    asynchronously. Otherwise it's sync. Empty hooks are no hooks, so the handler is the same
    as one given `None`.

    :param event: The event the handler is for, which hooks see.
    :param fn: The handler, taking the payload, or nothing for a payload-less event.
    :param config: The handler's routing config.
    :param hooks: The handler's hooks, or `None`.
    :param middleware: The adapter's handle middleware, outermost first.
    :param asynchronous: Whether the adapter runs middleware asynchronously.
    :returns: The handler to register, taking the message being handled.
    :raises TypeError: If `fn`, a hook or `config` is unhashable.
    """
    if hooks == _NO_HOOKS:
        hooks = None
    if is_async_callable(fn):
        return _async_handler(event, fn, _unpacking(event, fn), config, hooks, middleware)
    call = _unpacking(event, fn)
    if hooks is not None and hooks.has_async:
        return _async_handler(event, fn, _Awaitable(call), config, hooks, middleware)
    if hooks is not None:
        call = _hooked(hooks, event, call)
    if middleware:
        if asynchronous:
            return AsyncHandler(fn, hooks, _handled(middleware, _Awaitable(call)), config)
        call = _handled(middleware, call)
    return SyncHandler(fn, hooks, call, config)


def _async_handler[HandlerConfig](
    event: AnyEvent,
    fn: Callable[..., Any],
    call: Callable[[HandleCall], Coroutine[Any, Any, Any]],
    config: HandlerConfig,
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[HandleMiddleware, ...],
) -> AsyncHandler[HandlerConfig, Any, ..., Any]:
    """Return an async call as a handler, inside its hooks, then the adapter's middleware."""
    if hooks is not None:
        call = _hooked_async(hooks, event, call)
    if middleware:
        call = _handled(middleware, call)
    return AsyncHandler(fn, hooks, call, config)
