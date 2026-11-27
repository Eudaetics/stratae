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

The call is composed from steps that each wrap a call. The function, called with the payload,
runs inside its hooks, from {py:mod}`calls <stratae.events.calls>`. Then
{py:func}`unpacking` makes that a call of the message, and {py:func}`handled` runs it inside
the adapter's handle middleware.
"""

from collections.abc import Callable
from typing import Any

from stratae.callables import is_async_callable
from stratae.events.calls import deferred, hooked, hooked_async, payload_call
from stratae.events.handler import AsyncHandler, Handler, SyncHandler
from stratae.events.hooks import Hooks
from stratae.events.kind import Kind
from stratae.events.middleware import HandleCall, HandleMiddleware
from stratae.events.protocols import AnyEvent
from stratae.middleware import chain

_RELABELLED = (
    "handle middleware passed on a call with another event, config or headers, but they "
    "describe the message being handled"
)

_NO_HOOKS: Hooks[Any, Any] = Hooks()
"""Empty hooks, which a handler registers as no hooks."""


def unpacking[Result](call: Callable[[Any], Result]) -> Callable[[HandleCall], Result]:
    """Return `call`, made with the payload of the message it's given."""

    def unpacked(message: HandleCall, /) -> Result:
        return call(message.payload)

    return unpacked


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


def handled[Result](
    middleware: tuple[HandleMiddleware, ...], call: Callable[[HandleCall], Result]
) -> Callable[[HandleCall], Result]:
    """
    Return `call` run inside handle middleware, sync or async, which sees the message.

    :raises ValueError: When called, if middleware passes on another event, config or headers.
    """

    def finish(seen: HandleCall, received: HandleCall) -> Result:
        if seen is not received:
            _refuse_relabelled(seen, received)
        return call(seen)

    chained = chain(middleware, finish)

    def run(message: HandleCall, /) -> Result:
        return chained(message, message)

    return run


def _payload_call_in(
    hooks: Hooks[Any, Any] | None, event: AnyEvent, fn: Callable[..., Any]
) -> tuple[Callable[[Any], Any], bool]:
    """Return `fn` as a call of the payload inside its hooks, and whether that call is async."""
    call = payload_call(event, fn)
    is_async = is_async_callable(fn)
    if hooks is None:
        return call, is_async
    if is_async or hooks.has_async:
        return hooked_async(hooks, event, call if is_async else deferred(call)), True
    return hooked(hooks, event, call), False


def wrap_handler[HandlerConfig](
    event: AnyEvent,
    fn: Callable[..., Any],
    config: HandlerConfig,
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[HandleMiddleware, ...],
    kind: Kind,
) -> Handler[HandlerConfig, Any, ..., Any]:
    """
    Build the handler an adapter registers: the function, its hooks, then middleware.

    It's async if the function is, if any hook is, or if the adapter's kind awaits its
    middleware. Otherwise it's sync. Empty hooks are no hooks, so the handler is the same
    as one given `None`.

    :param event: The event the handler is for, which hooks see.
    :param fn: The handler, taking the payload, or nothing for a payload-less event.
    :param config: The handler's routing config.
    :param hooks: The handler's hooks, or `None`.
    :param middleware: The adapter's handle middleware, outermost first.
    :param kind: The adapter's {py:class}`Kind <stratae.events.kind.Kind>`, which fits the call
        to its middleware.
    :returns: The handler to register, taking the message being handled.
    :raises TypeError: If `fn`, a hook or `config` is unhashable.
    """
    if hooks == _NO_HOOKS:
        hooks = None
    call, is_async = _payload_call_in(hooks, event, fn)
    message_call = unpacking(call)
    if middleware:
        message_call, is_async = kind.fitted(message_call, is_async)
        message_call = handled(middleware, message_call)
    if is_async:
        return AsyncHandler(fn, hooks, message_call, config)
    return SyncHandler(fn, hooks, message_call, config)
