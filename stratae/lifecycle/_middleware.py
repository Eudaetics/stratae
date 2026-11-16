"""
A scope's middleware, entered as the first resources of each activation.

A scope's middleware is a generator function, given the scope. Its generator is entered and
exited as a resource's is, by {py:mod}`stratae.lifecycle._stack`, so it must yield exactly once
and raises the same errors when it doesn't. Only its exit differs: it can't swallow the
activation's error. Each middleware's exit is built once, with the scope.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable, Generator
from typing import Literal, NoReturn, TypeGuard

from stratae.callables import Gives, call_result
from stratae.lifecycle._stack import (
    AsyncExit,
    SyncExit,
    exit_async_generator,
    exit_generator,
)
from stratae.lifecycle.exceptions import MiddlewareError
from stratae.middleware import MiddlewareKindError

type SyncLayer[Scope] = tuple[
    Callable[[Scope], Generator[object, None, None]], Literal[False], SyncExit
]
"""A generator function middleware, flagged as sync, and its exit."""

type AsyncLayer[Scope] = tuple[
    Callable[[Scope], AsyncGenerator[object, None]], Literal[True], AsyncExit
]
"""An async generator function middleware, flagged as async, and its exit."""


def _gives_generator[Scope](
    item: Callable[[Scope], Generator[object, None, None] | AsyncGenerator[object, None]],
) -> TypeGuard[Callable[[Scope], Generator[object, None, None]]]:
    """Whether calling `item` gives a generator, as its code says."""
    return call_result(item).gives is Gives.GENERATOR


def _gives_async_generator[Scope](
    item: Callable[[Scope], Generator[object, None, None] | AsyncGenerator[object, None]],
) -> TypeGuard[Callable[[Scope], AsyncGenerator[object, None]]]:
    """Whether calling `item` gives an async generator, as its code says."""
    return call_result(item).gives is Gives.ASYNC_GENERATOR


def sync_layers[Scope](
    middleware: tuple[Callable[[Scope], Generator[object, None, None]], ...], owner: str
) -> tuple[SyncLayer[Scope], ...]:
    """
    Return each of a sync scope's middleware with its exit.

    :raises MiddlewareKindError: If a middleware isn't a generator function.
    """
    found: list[SyncLayer[Scope]] = []
    for item in middleware:
        if not _gives_generator(item):
            raise MiddlewareKindError(owner, item, "a generator function")
        found.append((item, False, _leaving(item, asynchronous=False)))
    return tuple(found)


def async_layers[Scope](
    middleware: tuple[
        Callable[[Scope], Generator[object, None, None] | AsyncGenerator[object, None]], ...
    ],
    owner: str,
) -> tuple[SyncLayer[Scope] | AsyncLayer[Scope], ...]:
    """
    Return each of an async scope's middleware with its kind and exit.

    :raises MiddlewareKindError: If a middleware isn't a generator function, sync or async.
    """
    found: list[SyncLayer[Scope] | AsyncLayer[Scope]] = []
    for item in middleware:
        if _gives_async_generator(item):
            found.append((item, True, _leaving_async(item)))
        elif _gives_generator(item):
            found.append((item, False, _leaving(item, asynchronous=True)))
        else:
            raise MiddlewareKindError(owner, item, "a generator function or an async one")
    return tuple(found)


def _refuse_swallow(error: BaseException, middleware: object, cancelling: bool) -> NoReturn:
    """
    Raise in place of a middleware that swallowed `error`.

    An ordinary error becomes MiddlewareError from it, so the activation still fails. Anything
    else, such as `KeyboardInterrupt`, is raised again as it is, so it still stops what it was
    meant to stop. A cancellation is raised again only while the task is still being cancelled.
    One the middleware made itself, timing the activation out, has been uncancelled by the time
    it swallows the timeout, and re-raising it would look like the task being cancelled.

    :param cancelling: Whether the task is still being cancelled. A sync scope passes `True`: it
        has no task to cancel, so any cancellation is raised again.
    """
    if isinstance(error, Exception) or (
        isinstance(error, asyncio.CancelledError) and not cancelling
    ):
        raise MiddlewareError(middleware, error) from error
    raise error


def _cancelling() -> bool:
    """Return whether the current task is being cancelled, as a swallowed cancellation shows."""
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _leaving(middleware: object, asynchronous: bool) -> SyncExit:
    """
    Return the exit for a middleware's generator, which refuses a swallowed error.

    :param asynchronous: Whether an async scope holds it, so a swallowed cancellation is
        checked against the task.
    """

    def leave(gen: Generator[object, None, object], exc: BaseException | None) -> bool:
        if exit_generator(gen, exc) and exc is not None:
            _refuse_swallow(exc, middleware, _cancelling() if asynchronous else True)
        return False

    return leave


def _leaving_async(middleware: object) -> AsyncExit:
    """Return the exit for a middleware's async generator, as {py:func}`_leaving` does."""

    async def leave(gen: AsyncGenerator[object, None], exc: BaseException | None) -> bool:
        if await exit_async_generator(gen, exc) and exc is not None:
            _refuse_swallow(exc, middleware, _cancelling())
        return False

    return leave
