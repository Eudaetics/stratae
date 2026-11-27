"""
Whether an owner runs functions synchronously or asynchronously, as a value.

A {py:class}`Kind` holds what differs between the two: the dispatchers an owner starts from,
which handlers it accepts, whether it awaits what it runs, how a handler's call is made to fit
its middleware, and how an event is bound to an emitter's call. {py:data}`SYNC` runs sync
handlers, hooks and middleware only, and binds sync calls. {py:data}`ASYNC` runs sync and
async handlers and hooks, awaits its middleware, and binds async calls.

The functions a kind is made of are public, so a kind of your own can be composed from them.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from stratae.callables import is_async_callable
from stratae.events.binding import bind_async, bind_sync
from stratae.events.calls import deferred
from stratae.events.dispatch import ASYNC_DISPATCHERS, SYNC_DISPATCHERS, Dispatch
from stratae.events.hooks import Hooks
from stratae.events.protocols import AnyEvent

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
    bind: Callable[
        [AnyEvent, Callable[..., Any], Callable[..., Any] | None, Hooks[Any, Any] | None],
        Callable[..., Any],
    ]
    """
    Binds an event to an emitter's call of this kind, given a factory and hooks or `None`:
    {py:func}`bind <stratae.events.binding.bind>` hands it every {py:class}`Emitter
    <stratae.events.binding.Emitter>` of this kind.
    """


def accepts_sync(fn: Callable[..., Any], /) -> bool:
    """Accept a handler whose call doesn't give a coroutine."""
    return not is_async_callable(fn)


def accepts_any(_: object, /) -> bool:
    """Accept any handler."""
    return True


def as_is(call: _Call, is_async: bool, /) -> tuple[_Call, bool]:
    """Leave a call as it is: sync middleware calls it directly."""
    return call, is_async


def deferred_if_sync(call: _Call, is_async: bool, /) -> tuple[_Call, bool]:
    """Defer a sync call, so async middleware can await it."""
    return (call, True) if is_async else (deferred(call), True)


SYNC = Kind(SYNC_DISPATCHERS, accepts_sync, awaits=False, fitted=as_is, bind=bind_sync)
"""Runs sync handlers, hooks and middleware only, and binds sync calls."""

ASYNC = Kind(ASYNC_DISPATCHERS, accepts_any, awaits=True, fitted=deferred_if_sync, bind=bind_async)
"""Runs sync and async handlers and hooks, awaits its middleware, and binds async calls."""
