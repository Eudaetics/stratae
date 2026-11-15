"""
A scope's middleware, entered as the first resources of each activation.

A scope's middleware is a generator function, given the scope. Its generator is entered and
exited as a resource's is, by {py:mod}`stratae.lifecycle._stack`, so it must yield exactly once
and raises the same errors when it doesn't. Only its exit differs: it can't swallow the
activation's error. Each middleware's exit is built once, with the scope.
"""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from typing import Any, NoReturn

from stratae.callables import Gives, call_result
from stratae.lifecycle._stack import exit_async_generator, exit_generator
from stratae.lifecycle.exceptions import MiddlewareError
from stratae.middleware import MiddlewareKindError

type Layer = tuple[Callable[[Any], Any], bool, Callable[[Any, BaseException | None], Any]]
"""A middleware, whether it's an async generator function, and its exit."""


def layers(middleware: tuple[Any, ...], owner: str, asynchronous: bool) -> tuple[Layer, ...]:
    """
    Return each middleware with its kind and exit, refusing one `owner` can't run.

    :raises MiddlewareKindError: If a middleware isn't a generator function, or is an async one
        given to a sync scope.
    """
    found: list[Layer] = []
    for item in middleware:
        gives = call_result(item).gives
        if gives is Gives.ASYNC_GENERATOR and asynchronous:
            found.append((item, True, _leaving_async(item)))
        elif gives is Gives.GENERATOR:
            found.append((item, False, _leaving(item, asynchronous)))
        else:
            wanted = "a generator function"
            raise MiddlewareKindError(
                owner, item, f"{wanted} or an async one" if asynchronous else wanted
            )
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


def _leaving(
    middleware: object, asynchronous: bool
) -> Callable[[Generator[object, None, None], BaseException | None], bool]:
    """
    Return the exit for a middleware's generator, which refuses a swallowed error.

    :param asynchronous: Whether an async scope holds it, so a swallowed cancellation is
        checked against the task.
    """

    def leave(gen: Generator[object, None, None], exc: BaseException | None) -> bool:
        if exit_generator(gen, exc) and exc is not None:
            _refuse_swallow(exc, middleware, _cancelling() if asynchronous else True)
        return False

    return leave


def _leaving_async(
    middleware: object,
) -> Callable[[AsyncGenerator[object, None], BaseException | None], Awaitable[bool]]:
    """Return the exit for a middleware's async generator, as {py:func}`_leaving` does."""

    async def leave(gen: AsyncGenerator[object, None], exc: BaseException | None) -> bool:
        if await exit_async_generator(gen, exc) and exc is not None:
            _refuse_swallow(exc, middleware, _cancelling())
        return False

    return leave
