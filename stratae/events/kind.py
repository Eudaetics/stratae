"""
Whether an owner runs functions synchronously or asynchronously, as a value.

A {py:class}`Kind` holds what differs between the two: the dispatchers an owner starts from,
which handlers it accepts, whether it awaits what it runs, and how a handler's call is made to
fit its middleware. {py:data}`SYNC` runs sync handlers, hooks and middleware only.
{py:data}`ASYNC` runs sync and async handlers and hooks, and awaits its middleware.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from stratae.callables import is_async_callable
from stratae.events.calls import deferred
from stratae.events.dispatch import ASYNC_DISPATCHERS, SYNC_DISPATCHERS, Dispatch

type _Call = Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class Kind:
    """What differs between running functions synchronously and asynchronously."""

    dispatchers: tuple[Dispatch, ...]
    """The patterns an owner of this kind delivers by default, and their dispatchers."""
    accepts: Callable[[Callable[..., Any]], bool]
    """Whether a handler can be registered."""
    awaits: bool
    """
    Whether it awaits what it runs. One that awaits runs async hooks, and takes only an
    `async def` as middleware; one that doesn't refuses async hooks, and takes only a sync
    function returning a value.
    """
    fitted: Callable[[_Call, bool], tuple[_Call, bool]]
    """
    Makes a handler's call, and whether it's async, fit this kind's middleware. A kind that
    awaits defers a sync call, so its middleware can await it.
    """


def _not_async(fn: Callable[..., Any], /) -> bool:
    """Accept a handler whose call doesn't give a coroutine."""
    return not is_async_callable(fn)


def _anything(_: object, /) -> bool:
    """Accept anything."""
    return True


def _as_is(call: _Call, is_async: bool, /) -> tuple[_Call, bool]:
    """Leave a call as it is: sync middleware calls it directly."""
    return call, is_async


def _awaitable(call: _Call, is_async: bool, /) -> tuple[_Call, bool]:
    """Defer a sync call, so async middleware can await it."""
    return (call, True) if is_async else (deferred(call), True)


SYNC = Kind(SYNC_DISPATCHERS, _not_async, awaits=False, fitted=_as_is)
"""Runs sync handlers, hooks and middleware only."""

ASYNC = Kind(ASYNC_DISPATCHERS, _anything, awaits=True, fitted=_awaitable)
"""Runs sync and async handlers and hooks, and awaits its middleware."""
