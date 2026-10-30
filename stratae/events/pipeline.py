"""
Compose hooks and middleware around an adapter's emits and handlers.

Every adapter runs these, so hooks and middleware behave the same on every bus: middleware
wraps hooks, which wrap the call itself. An adapter of your own, such as one for another
broker, uses them the same way. Its `emit` runs the publish with {py:func}`run_emit` or
{py:func}`run_emit_async`, inside the message's envelope. Its `handle` registers the call
{py:func}`wrap_handler` builds, which takes the payload whatever the event, as a
{py:class}`Handler <stratae.events.handler.Handler>`, and calls it with
{py:func}`invoke <stratae.events.handler.invoke>`.
"""

from typing import Any, Awaitable, Callable

from stratae._callables import is_async_callable
from stratae.events.handler import WithoutPayload
from stratae.events.hooks import Hooks, run_hooks, run_hooks_async
from stratae.events.middleware import (
    HandleCall,
    PublishCall,
    run_middleware,
    run_middleware_async,
)
from stratae.events.protocols import AnyEvent


def run_emit(
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[Callable[..., object], ...],
    event: Any,
    config: Any,
    payload: Any,
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
    :param call: The emit itself, such as a dispatch.
    :returns: What `call` returns.
    """
    if hooks is not None:
        call, args = run_hooks, (hooks, event, payload, call, *args)
    if middleware:
        call, args = (
            run_middleware,
            (middleware, PublishCall, event, config, payload, call, *args),
        )
    return call(*args)


async def run_emit_async(
    hooks: Hooks[Any, Any] | None,
    middleware: tuple[Callable[..., object], ...],
    event: Any,
    config: Any,
    payload: Any,
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
    :param call: The emit itself, such as a dispatch or a publish.
    :returns: What `call` resolves to.
    """
    if hooks is not None:
        call, args = run_hooks_async, (hooks, event, payload, call, *args)
    if middleware:
        call, args = (
            run_middleware_async,
            (middleware, PublishCall, event, config, payload, call, *args),
        )
    return await call(*args)


class _Wrapped:
    """
    A sync handler call wrapped by a runner, such as hooks or handle middleware.

    The runner is called as `runner(*context, payload, call, payload)`, the shape
    {py:func}`run_hooks <stratae.events.hooks.run_hooks>` and
    {py:func}`run_middleware <stratae.events.middleware.run_middleware>` share.
    """

    __slots__ = ("call", "context", "runner")

    def __init__(
        self, runner: Callable[..., Any], context: tuple[Any, ...], call: Callable[[Any], Any]
    ) -> None:
        self.runner = runner
        self.context = context
        self.call = call

    def __call__(self, payload: Any, /) -> Any:
        return self.runner(*self.context, payload, self.call, payload)


class _AsyncWrapped:
    """A handler call, sync or async, wrapped by an async runner, such as hooks or middleware."""

    __slots__ = ("call", "context", "runner")

    def __init__(
        self,
        runner: Callable[..., Awaitable[Any]],
        context: tuple[Any, ...],
        call: Callable[[Any], Any],
        is_async: bool,
    ) -> None:
        self.runner = runner
        self.context = context
        # Adapted once here, so a delivery doesn't decide again whether to await.
        self.call = call if is_async else _Awaitable(call)

    def __call__(self, payload: Any, /) -> Awaitable[Any]:
        return self.runner(*self.context, payload, self.call, payload)


class _Awaitable:
    """A sync handler call, made awaitable for an async runner."""

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
            call, is_async = _AsyncWrapped(run_hooks_async, (hooks, event), call, is_async), True
        else:
            call = _Wrapped(run_hooks, (hooks, event), call)
    if middleware:
        context = (middleware, HandleCall, event, config)
        if asynchronous:
            call, is_async = _AsyncWrapped(run_middleware_async, context, call, is_async), True
        else:
            call = _Wrapped(run_middleware, context, call)
    return call, is_async
