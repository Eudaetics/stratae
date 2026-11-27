"""
Build the call made for a function, from steps that each wrap a call of the payload.

Each step takes a call of the payload and returns one, so they compose wherever functions are
called with a payload. {py:func}`wrap_handler <stratae.events.handling.wrap_handler>` builds a
handler's call from them, then unpacks the message and runs handle middleware around it.

- {py:func}`payload_call`: the function itself, or for a payload-less event, the function
  called with nothing.
- {py:func}`hooked`: the call run inside sync hooks, which see the payload.
- {py:func}`hooked_async`: the call run inside hooks, sync or async, as a coroutine.
- {py:func}`deferred`: a sync call made awaitable, run only once it's awaited.
"""

from collections.abc import Callable, Coroutine
from typing import Any

from stratae.events.hooks import Hooks, run_hooks, run_hooks_async
from stratae.events.protocols import AnyEvent


def payload_call[Result](event: AnyEvent, fn: Callable[..., Result]) -> Callable[[Any], Result]:
    """
    Return `fn` as a call of the payload, which a payload-less event's `fn` is called without.

    :param event: The event `fn` is registered for.
    :param fn: The function, taking the payload, or nothing for a payload-less event.
    """
    if not event.is_signal:
        return fn
    return lambda _payload: fn()


def hooked[Payload, Result](
    hooks: Hooks[Payload, Result], event: AnyEvent, call: Callable[[Payload], Result]
) -> Callable[[Payload], Result]:
    """
    Return `call` run inside sync hooks, which see its payload.

    :param hooks: The hooks, all sync.
    :param event: The event, passed to every hook.
    :param call: The call of the payload the hooks observe.
    """

    def run(payload: Payload, /) -> Result:
        return run_hooks(hooks, event, payload, call, payload)

    return run


def hooked_async[Payload, Result](
    hooks: Hooks[Payload, Result],
    event: AnyEvent,
    call: Callable[[Payload], Coroutine[Any, Any, Result]],
) -> Callable[[Payload], Coroutine[Any, Any, Result]]:
    """
    Return `call` run inside hooks, sync or async, as a coroutine.

    :param hooks: The hooks.
    :param event: The event, passed to every hook.
    :param call: The async call of the payload the hooks observe.
    """

    def run(payload: Payload, /) -> Coroutine[Any, Any, Result]:
        return run_hooks_async(hooks, event, payload, call, payload)

    return run


class _Deferred[Arg, Result]:
    """
    A sync call, made awaitable. It runs when awaited.

    A class, since an awaitable call with nothing to await is a method: a function would be an
    `async def` that never awaits.
    """

    __slots__ = ("call",)

    def __init__(self, call: Callable[[Arg], Result]) -> None:
        self.call = call

    async def __call__(self, arg: Arg, /) -> Result:
        return self.call(arg)


def deferred[Arg, Result](
    call: Callable[[Arg], Result],
) -> Callable[[Arg], Coroutine[Any, Any, Result]]:
    """
    Return `call` made awaitable: calling it gives a coroutine that makes the call when awaited.

    :param call: The sync call.
    """
    return _Deferred(call)
