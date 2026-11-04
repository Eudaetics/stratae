"""
Dispatchers for the direct buses, and the per-bus dispatch function built from them.

A dispatcher takes an event, its handlers and a payload, and delivers the payload as the
event's pattern requires. Each is generic in the pattern's type parameters, so its body is
checked against what the pattern promises to emit. Each direct bus builds its own
`singledispatch` function over the event's class, so it supports the patterns it has a
dispatcher for, and their subclasses.
"""

import asyncio
from collections.abc import Awaitable, Callable, Collection, Iterable
from functools import singledispatch
from typing import Any

from stratae.events.exceptions import (
    BaseHandlerErrors,
    HandlerErrors,
    MultipleRespondersError,
    NoResponderError,
)
from stratae.events.handler import Handler, invoke
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
    _event: PubSub[S, Any], handlers: Collection[Handler[[S], Any, object]], payload: S
) -> None:
    """Call every handler in turn, raising their failures together as `HandlerErrors`."""
    exceptions: list[Exception] = []
    for handler in tuple(handlers):
        try:
            handler(payload)
        except Exception as exc:
            exceptions.append(exc)
    if exceptions:
        raise HandlerErrors(exceptions)


async def fan_out_async[S](
    _event: PubSub[S, Any], handlers: Collection[Handler[[S], Any, Any]], payload: S
) -> None:
    """
    Run every handler concurrently, raising their failures together.

    :raises BaseHandlerErrors: If any handler raises. {py:exc}`HandlerErrors
        <stratae.events.exceptions.HandlerErrors>` unless one raised a `BaseException` such as
        `CancelledError`.
    """
    results = await asyncio.gather(
        *(invoke(handler, payload) for handler in handlers),
        return_exceptions=True,
    )
    failures = [result for result in results if isinstance(result, BaseException)]
    errors = [failure for failure in failures if isinstance(failure, Exception)]
    if len(errors) < len(failures):
        raise BaseHandlerErrors(failures)
    if errors:
        raise HandlerErrors(errors)
