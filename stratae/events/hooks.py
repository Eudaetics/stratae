"""
Observe an event being published or handled, with hooks passed where it meets a bus.

{py:class}`Hooks` holds four tuples of hooks, run around one call: `before` it, then
`success` or `failure`, then `complete`. Pass them to a bus's `emit` or `bind` as `hooks=`,
around each emit, or to its `handle`, around that handler. Both run inside the emission's
envelope. Every hook takes the event first, so one hook can serve many events. Hooks are
typed by the event they're passed with: `S` is its payload and `R` the reply they see.

- `before(event, payload)` runs first, and may stop the call: if one raises, nothing is
  dispatched and the error propagates.
- `success(event, payload, reply)` runs if the call returned.
- `failure(event, payload, error)` runs if the call raised an `Exception`, not on
  cancellation.
- `complete(event, payload, error)` runs last, with the call's error, or `None`.

`success`, `failure` and `complete` only observe. Every one of them runs, and one that
raises is logged rather than changing the call's outcome: its reply, or its error, stands.
Use `before`, or bus middleware, for anything that should be able to fail the call.

On an async bus, the observing hooks in a slot run concurrently, so a slow one doesn't hold
up the others; `complete` still runs after `success` or `failure`. `before` hooks, and every
hook on a sync bus, run one after another, in order.

An adapter runs hooks with {py:func}`run_hooks` or {py:func}`run_hooks_async`, and wraps a
handler with {py:func}`with_handle_hooks`.

````{example} Auditing requests as they're published and handled
```{code-block} python
from stratae.events import DirectBus, Hooks, Request

class PlaceOrder:
    def __init__(self, item: str) -> None:
        self.item = item

class Order:
    def __init__(self, item: str) -> None:
        self.item = item

def audit_success(event, payload, reply) -> None:
    print(f"{event.name}: {payload.item} -> {type(reply).__name__}")

def audit_failure(event, payload, error) -> None:
    print(f"{event.name}: {payload.item} failed: {error}")

def check_stock(event, payload) -> None:
    print(f"checking stock for {payload.item}")

place_order_event = Request(PlaceOrder, reply=Order)
audit = Hooks(success=(audit_success,), failure=(audit_failure,))

bus = DirectBus()
place_order = bus.bind(place_order_event, factory=PlaceOrder, hooks=audit)

@bus.handle(place_order_event, hooks=Hooks(before=(check_stock,)))
def handle_place_order(command: PlaceOrder) -> Order:
    if command.item == "unicorn":
        raise LookupError("out of stock")
    return Order(command.item)

place_order(item="lamp")
try:
    place_order(item="unicorn")
except LookupError:
    pass
```
```{output}
checking stock for lamp
PlaceOrder: lamp -> Order
checking stock for unicorn
PlaceOrder: unicorn failed: out of stock
```
````
"""

import asyncio
import logging
from dataclasses import dataclass, field
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from stratae._callables import is_async_callable

if TYPE_CHECKING:
    from stratae.events.protocols import AnyEvent

_log = logging.getLogger(__name__)

_SLOTS = ("before", "success", "failure", "complete")

_ASYNC_HOOK_ON_SYNC_PATH = (
    "hook {!r} returned an awaitable, but these hooks run synchronously; use an async bus"
)


@dataclass(frozen=True, slots=True)
class Hooks[S, R]:
    """
    Hooks run around publishing or handling an event, sync or async.

    `S` is the event's payload type and `R` the reply the hooks see: what emitting returns
    for publish hooks, what the handler returns for handle hooks.
    """

    before: tuple[Callable[["AnyEvent", S], object], ...] = ()
    """Run before the call; one raising stops it."""
    success: tuple[Callable[["AnyEvent", S, R], object], ...] = ()
    """Run with the reply after the call returns; errors are logged."""
    failure: tuple[Callable[["AnyEvent", S, Exception], object], ...] = ()
    """Run with the error after the call raises an `Exception`; errors are logged."""
    complete: tuple[Callable[["AnyEvent", S, BaseException | None], object], ...] = ()
    """Run last, always, with the call's error or `None`; errors are logged."""
    has_async: bool = field(init=False, repr=False, compare=False)
    """Whether any hook is async, so a sync bus can refuse them."""

    def __post_init__(self) -> None:
        """Freeze each slot as a tuple, and note whether any hook is async."""
        for slot in _SLOTS:
            object.__setattr__(self, slot, tuple(getattr(self, slot)))
        is_async = any(is_async_callable(hook) for slot in _SLOTS for hook in getattr(self, slot))
        object.__setattr__(self, "has_async", is_async)


def _reject_awaitable(hook: Callable[..., object], result: object) -> None:
    """Raise TypeError if a hook on the sync path returned an awaitable, closing it."""
    if not isawaitable(result):
        return
    close = getattr(result, "close", None)
    if close is not None:
        close()
    raise TypeError(_ASYNC_HOOK_ON_SYNC_PATH.format(hook))


def _observe(hooks: tuple[Callable[..., object], ...], event: Any, *args: Any) -> None:
    """Run every observing hook, logging any that raises instead of propagating it."""
    for hook in hooks:
        try:
            _reject_awaitable(hook, hook(event, *args))
        except Exception:
            _log.exception("hook %r for event '%s' failed", hook, event.name)


def run_hooks[**P, T](
    hooks: Hooks[Any, Any],
    event: Any,
    payload: Any,
    call: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """
    Run `call(*args, **kwargs)` inside sync hooks, returning or raising what it does.

    :param hooks: The hooks to run. Must all be sync.
    :param event: The event, passed to every hook.
    :param payload: The payload, passed to every hook.
    :param call: The publish or handler call the hooks observe.
    :returns: What `call` returns.
    :raises TypeError: If a hook returns an awaitable.
    """
    for hook in hooks.before:
        _reject_awaitable(hook, hook(event, payload))
    try:
        reply = call(*args, **kwargs)
    except Exception as exc:
        _observe(hooks.failure, event, payload, exc)
        _observe(hooks.complete, event, payload, exc)
        raise
    except BaseException as exc:
        _observe(hooks.complete, event, payload, exc)
        raise
    _observe(hooks.success, event, payload, reply)
    _observe(hooks.complete, event, payload, None)
    return reply


async def _observe_one(hook: Callable[..., object], event: Any, *args: Any) -> None:
    """Run one observing hook, sync or async, logging it if it raises."""
    try:
        result = hook(event, *args)
        if isawaitable(result):
            await result
    except Exception:
        _log.exception("hook %r for event '%s' failed", hook, event.name)


async def _observe_async(hooks: tuple[Callable[..., object], ...], event: Any, *args: Any) -> None:
    """Run every observing hook concurrently, so a slow one doesn't hold up the rest."""
    if len(hooks) == 1:
        await _observe_one(hooks[0], event, *args)
    elif hooks:
        await asyncio.gather(*(_observe_one(hook, event, *args) for hook in hooks))


async def run_hooks_async[**P, T](
    hooks: Hooks[Any, Any],
    event: Any,
    payload: Any,
    call: Callable[P, Awaitable[T]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    """
    Await `call(*args, **kwargs)` inside hooks, sync or async, returning or raising its result.

    :param hooks: The hooks to run.
    :param event: The event, passed to every hook.
    :param payload: The payload, passed to every hook.
    :param call: The publish or handler call the hooks observe.
    :returns: What `call` resolves to.
    """
    for hook in hooks.before:
        result = hook(event, payload)
        if isawaitable(result):
            await result
    try:
        reply = await call(*args, **kwargs)
    except Exception as exc:
        await _observe_async(hooks.failure, event, payload, exc)
        await _observe_async(hooks.complete, event, payload, exc)
        raise
    except BaseException as exc:
        await _observe_async(hooks.complete, event, payload, exc)
        raise
    await _observe_async(hooks.success, event, payload, reply)
    await _observe_async(hooks.complete, event, payload, None)
    return reply


class _HookedCall:
    """A sync handler call wrapped in hooks."""

    __slots__ = ("call", "event", "hooks")

    def __init__(self, call: Callable[[Any], Any], event: Any, hooks: Hooks[Any, Any]) -> None:
        self.call = call
        self.event = event
        self.hooks = hooks

    def __call__(self, payload: Any, /) -> Any:
        return run_hooks(self.hooks, self.event, payload, self.call, payload)


class _AsyncHookedCall:
    """A handler call, sync or async, wrapped in hooks."""

    __slots__ = ("call", "event", "hooks", "is_async")

    def __init__(
        self, call: Callable[[Any], Any], event: Any, hooks: Hooks[Any, Any], is_async: bool
    ) -> None:
        self.call = call
        self.event = event
        self.hooks = hooks
        self.is_async = is_async

    async def __call__(self, payload: Any, /) -> Any:
        return await run_hooks_async(self.hooks, self.event, payload, self._invoke, payload)

    async def _invoke(self, payload: Any) -> Any:
        """Call the handler, awaiting it if it's async."""
        if self.is_async:
            return await self.call(payload)
        return self.call(payload)


def with_handle_hooks(
    event: "AnyEvent",
    hooks: Hooks[Any, Any] | None,
    call: Callable[[Any], Any],
    is_async: bool,
) -> tuple[Callable[[Any], Any], bool]:
    """
    Wrap a handler call in hooks, if there are any.

    :param event: The event the handler is registered for, passed to every hook.
    :param hooks: The handler's hooks, or `None`.
    :param call: The handler call, taking the payload.
    :param is_async: Whether `call` is async.
    :returns: The call to register, and whether it's async: the call itself without hooks;
        an async wrapper if the call or any hook is async; a sync wrapper otherwise.
    """
    if hooks is None:
        return call, is_async
    if is_async or hooks.has_async:
        return _AsyncHookedCall(call, event, hooks, is_async), True
    return _HookedCall(call, event, hooks), False
