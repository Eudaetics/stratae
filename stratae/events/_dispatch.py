"""
Dispatchers for the direct buses, and the per-bus dispatch function built from them.

A dispatcher takes an event, its handlers and a payload, and delivers the payload as the
event's pattern requires. Each is generic in the pattern's type parameters, so its body is
checked against what the pattern promises to emit. Each direct bus builds its own
`singledispatch` function over the event's class, so it supports the patterns it has a
dispatcher for, and their subclasses.
"""

import asyncio
from collections.abc import Awaitable, Callable, Collection, Coroutine, Generator, Iterable
from contextvars import Context, copy_context
from functools import singledispatch
from typing import Any

from stratae.events.exceptions import (
    BaseHandlerErrors,
    HandlerErrors,
    MultipleRespondersError,
    NoResponderError,
)
from stratae.events.handler import Handler, Handlers, invoke
from stratae.events.patterns import PubSub, Request
from stratae.events.protocols import AnyEvent


def no_dispatcher(event: AnyEvent, handlers: Collection[Any], payload: Any) -> Any:
    """
    Mark a pattern the bus has no dispatcher for; never called.

    A bus compares its dispatch function's choice against this, and refuses the event with
    `TypeError` in `handle`, `bind` and `emit`. `BaseDirectBus` routes every emit through
    that check, so nothing dispatches to this.
    """
    ...


def build_dispatcher(pairs: Iterable[tuple[type, Callable[..., Any]]]):
    """Build a bus's dispatch function from pattern and dispatcher pairs; later pairs win."""
    dispatch = singledispatch(no_dispatcher)
    for pattern, dispatcher in pairs:
        dispatch.register(pattern, dispatcher)
    return dispatch


def only_responder[H: Handler[Any, Any, Any]](event: AnyEvent, handlers: Collection[H]) -> H:
    """Return a request event's sole responder, or raise if there isn't exactly one."""
    if not handlers:
        raise NoResponderError(event.name)
    if len(handlers) > 1:
        raise MultipleRespondersError(event.name, len(handlers))
    (responder,) = handlers
    return responder


def dispatch_request[S, R](
    event: Request[S, R, Any], handlers: Collection[Handler[[S], Any, R]], payload: S
) -> R:
    """Call a request event's sole responder and return its reply."""
    return only_responder(event, handlers)(payload)


def dispatch_request_async[S, R](
    event: Request[S, R, Any],
    handlers: Collection[Handler[[S], Any, R | Awaitable[R]]],
    payload: S,
) -> Awaitable[R]:
    """Call a request event's sole responder, awaiting it if async."""
    responder = only_responder(event, handlers)
    return invoke(responder, payload)


def fan_out[S](
    _event: PubSub[S, Any], handlers: Handlers[Handler[[S], Any, None]], payload: S
) -> None:
    """Call every handler in turn, raising their failures together as `HandlerErrors`."""
    exceptions: list[Exception] = []
    for handler in handlers:
        try:
            handler(payload)
        except Exception as exc:
            exceptions.append(exc)
    if exceptions:
        raise HandlerErrors(exceptions)


class _InContext:
    """
    Await a coroutine with each of its steps run in `context`, as a task would.

    It gives one handler its own copy of the context without a task's cost. The coroutine runs
    in the awaiting task, so cancelling that task cancels it.
    """

    __slots__ = ("_context", "_coro")

    def __init__(self, coro: Coroutine[Any, Any, Any], context: Context) -> None:
        self._coro = coro
        self._context = context

    def __await__(self) -> Generator[Any, Any, Any]:
        run, coro = self._context.run, self._coro
        value: Any = None
        error: BaseException | None = None
        while True:
            try:
                step = run(coro.send, value) if error is None else run(coro.throw, error)
            except StopIteration as stop:
                return stop.value
            try:
                value, error = (yield step), None
            except GeneratorExit:
                run(coro.close)
                raise
            except BaseException as exc:
                value, error = None, exc


_HANDLER_FAILURES = (Exception, asyncio.CancelledError)
"""
What a handler raises that's collected with the other handlers' failures.

Anything else, such as `KeyboardInterrupt`, propagates at once.
"""


def _run_sync_handlers(
    handlers: Iterable[Handler[[Any], Any, Any]], payload: Any, failures: list[BaseException]
) -> None:
    """Call each sync handler in its own copy of the context, collecting its failure."""
    for handler in handlers:
        try:
            copy_context().run(handler, payload)
        except _HANDLER_FAILURES as failure:
            failures.append(failure)


async def _await_lone_handler(
    handler: Handler[[Any], Any, Any], payload: Any, failures: list[BaseException]
) -> None:
    """
    Await one async handler in its own copy of the context, collecting its failure.

    A `CancelledError` while the emitting task is being cancelled is the emit's own
    cancellation, so it propagates rather than being collected.
    """
    try:
        await _InContext(handler(payload), copy_context())
    except _HANDLER_FAILURES as failure:
        task = asyncio.current_task()
        if isinstance(failure, asyncio.CancelledError) and task is not None and task.cancelling():
            raise
        failures.append(failure)


async def _await_handlers(
    handlers: tuple[Handler[[Any], Any, Any], ...], payload: Any, failures: list[BaseException]
) -> None:
    """Await async handlers, a lone one in place and several concurrently as tasks."""
    if len(handlers) == 1:
        await _await_lone_handler(handlers[0], payload, failures)
    elif handlers:
        results = await asyncio.gather(
            *(handler(payload) for handler in handlers), return_exceptions=True
        )
        failures.extend(result for result in results if isinstance(result, BaseException))


def _raise_failures(failures: list[BaseException]) -> None:
    """Raise the handlers' failures together, if there are any."""
    errors = [failure for failure in failures if isinstance(failure, Exception)]
    if len(errors) < len(failures):
        raise BaseHandlerErrors(failures)
    if errors:
        raise HandlerErrors(errors)


async def fan_out_async[S](
    _event: PubSub[S, Any], handlers: Handlers[Handler[[S], Any, Any]], payload: S
) -> None:
    """
    Run every handler in its own copy of the context, raising their failures together.

    Context variables a handler sets don't reach the other handlers or the emitter. Sync
    handlers run first, one at a time. A lone async handler is awaited in place. Several run
    concurrently, each as a task.

    :raises BaseHandlerErrors: If any handler raises. {py:exc}`HandlerErrors
        <stratae.events.exceptions.HandlerErrors>` unless one raised a `BaseException` such as
        `CancelledError`.
    """
    failures: list[BaseException] = []
    _run_sync_handlers(handlers.sync, payload, failures)
    await _await_handlers(handlers.asynchronous, payload, failures)
    _raise_failures(failures)
