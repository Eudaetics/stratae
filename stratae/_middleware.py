"""
The checks behind the middleware contract, shared by lifecycle scopes and event buses.

See {py:mod}`stratae.middleware` for the contract. Scopes and buses run their middleware
differently: a bus enters it around one call, and a scope holds it as resources of an
activation. Both check it, classify it and refuse its mistakes here, so the rules can't drift.
"""

import asyncio
from collections.abc import Callable
from types import CoroutineType
from typing import Any, NoReturn

from stratae.middleware import (
    MiddlewareAsyncMethodError,
    MiddlewareError,
    MiddlewareNotEnterableError,
    MiddlewareResultError,
)

type_of: Callable[[object], Any] = type
"""`type`, typed to return `Any`: a context manager's methods are looked up on its type."""

_UNKNOWN: Any = object()

_ENTRIES_LIMIT = 256
"""How many context manager types async owners remember; beyond it, they check each time."""

_ENTRIES: dict[type, bool | None] = {}
"""
How an async owner enters each context manager type it has seen, so it checks each type once.

Sync owners don't classify: they look up `__enter__` and `__exit__`, which they need anyway,
and refuse the result if either is missing.
"""


def refuse_unenterable(middleware: tuple[Any, ...], owner: str, asynchronous: bool) -> None:
    """
    Refuse a middleware class whose instances `owner` can't enter.

    Only a class can be checked before it's called. Any other callable is checked by
    {py:func}`manager` each time.

    :raises MiddlewareNotEnterableError: If a class isn't a context manager `owner` enters.
    """
    for item in middleware:
        if isinstance(item, type) and _entry(item, asynchronous) is None:
            raise MiddlewareNotEnterableError(owner, item, asynchronous)


def _entry(kind: type, asynchronous: bool) -> bool | None:
    """
    Return how an owner enters a context manager of type `kind`.

    :returns: `True` to enter it asynchronously, `False` synchronously, or `None` if the owner
        can't enter it.
    """
    if asynchronous and hasattr(kind, "__aenter__") and hasattr(kind, "__aexit__"):
        return True
    if hasattr(kind, "__enter__") and hasattr(kind, "__exit__"):
        return False
    return None


def manager(middleware: Callable[[Any], Any], wrapped: object) -> tuple[Any, Any, bool]:
    """
    Call a middleware for an async owner, returning its context manager, type, and kind.

    The kind is whether to enter the context manager asynchronously.

    :param middleware: The middleware.
    :param wrapped: What it wraps: a scope, or a call.
    :raises MiddlewareResultError: If it returned something the owner can't enter. A coroutine
        returned, from an `async def` middleware, is closed first.
    """
    result = middleware(wrapped)
    kind = type_of(result)
    entry = _ENTRIES.get(kind, _UNKNOWN)
    if entry is _UNKNOWN:
        entry = _entry(kind, asynchronous=True)
        # Bounded, so middleware that makes a new class per call can't grow it forever.
        if len(_ENTRIES) < _ENTRIES_LIMIT:
            _ENTRIES[kind] = entry
    if entry is None:
        refuse_result(middleware, result, asynchronous=True)
    return result, kind, entry


def refuse_result(middleware: object, result: Any, asynchronous: bool) -> NoReturn:
    """
    Refuse what a middleware returned, which the owner can't enter.

    A sync owner calls this itself when looking up `__enter__` or `__exit__` fails, rather
    than classifying each result: the lookups are needed anyway, so a context manager it can
    enter costs nothing extra.

    :raises MiddlewareResultError: Always. A coroutine, from an `async def` middleware, is
        closed first, so it isn't left unawaited.
    """
    if result.__class__ is CoroutineType:
        result.close()
    raise MiddlewareResultError(middleware, result, asynchronous)


def refuse_coroutine(result: Any, middleware: object) -> NoReturn:
    """
    Refuse a sync `__enter__` or `__exit__` that's an `async def`.

    Its coroutine is closed, so it isn't left unawaited. Callers check for a coroutine
    themselves, keeping a call off each layer's normal path. Only a coroutine is refused,
    being unambiguously an `async def` mistake: anything else `__enter__` returns is the
    context manager's own, awaitable or not, and is left alone.

    :raises MiddlewareAsyncMethodError: Always.
    """
    result.close()
    raise MiddlewareAsyncMethodError(middleware)


def refuse_swallow(error: BaseException, middleware: object, cancelling: bool) -> NoReturn:
    """
    Raise in place of an exit that returned true, trying to swallow `error`.

    An ordinary error becomes MiddlewareError from it, so the work still fails as an error.
    Anything else, such as `KeyboardInterrupt`, is raised again as it is, so it still stops
    what it was meant to stop: turned into an ordinary error, `except Exception` would catch
    it. A cancellation is raised again only while the task is still being cancelled. One the
    middleware made itself, timing the work out, has been uncancelled by the time it swallows
    the timeout, and re-raising it would look like the task being cancelled.

    :param cancelling: Whether the task doing the work is still being cancelled. Sync owners
        pass `True`: they have no task to cancel, so any cancellation is raised again.
    """
    if isinstance(error, Exception) or (
        isinstance(error, asyncio.CancelledError) and not cancelling
    ):
        raise MiddlewareError(middleware, error) from error
    raise error


def task_cancelling() -> bool:
    """
    Return whether the current task is being cancelled, as a swallowed cancellation shows.

    The work runs in this task, so its count is the work's: `asyncio.timeout` uncancels the
    task when it turns its own cancellation into `TimeoutError`. Code earlier in the task that
    caught a cancellation without uncancelling leaves the count raised, and a swallowed
    timeout's cancellation is then raised again rather than reported. Outside a task, a
    cancellation can't be the task's.
    """
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0
