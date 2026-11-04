"""
Wrap every emit or handler call on a bus with middleware, for cross-cutting concerns.

{py:class}`Middleware` holds the middleware a bus runs: `publish` around each emit, and
`handle` around each handler call. Give it to the bus when it's built, with `middleware=`;
it's fixed from then on. The first middleware listed is outermost, and middleware wraps the
event's {py:class}`Hooks <stratae.events.hooks.Hooks>`, which wrap the call itself. Both run
inside the emission's envelope when the bus opens one: always on a broker adapter, and on a
direct bus built with `use_envelope=True`.

A middleware is a callable taking a {py:class}`PublishCall` or {py:class}`HandleCall`, which
carries the event and payload, and returning a context manager: an instance of a class with
`__enter__` and `__exit__`, or what a `contextlib.contextmanager` function returns. The bus
enters each middleware's context manager, outermost first, runs the call once, then exits
them innermost first, like nested `with` statements. An exit sees the call's error as a `with`
statement would, and after a successful call, `call.reply` holds the reply. Middleware sees
every event, so unlike hooks it isn't typed per event.

A sync bus enters sync context managers. An async bus enters async ones, with `__aenter__`
and `__aexit__` or from `contextlib.asynccontextmanager`, and sync ones too, in the task
running the call. So `lambda call: asyncio.timeout(5)` is a middleware giving each call five
seconds. A middleware that's a class is checked when the bus is built, and any other callable
on each call: one returning something the bus can't enter raises `TypeError`. A coroutine
returned, from an `async def` middleware, is closed; any other awaitable, such as a task, isn't
the bus's to cancel, so it's left as it is.

Middleware wraps the call, but can't change its reply or swallow its error. An exit that
returns true, trying to swallow an ordinary error, raises
{py:exc}`MiddlewareError <stratae.events.exceptions.MiddlewareError>` from it instead, so the
call still fails, and outer middleware sees that. Trying to swallow anything else, such as
`KeyboardInterrupt` or the task's cancellation, raises it again as it is, so it still stops
what it was meant to stop. A cancellation the middleware made itself, by timing the call out,
isn't the task's: swallowing the timeout raises `MiddlewareError`. A middleware can raise an
error of its own. Raised on entry, it refuses the call, and the middleware already entered see
it on exit. Raised on exit, it replaces the call's reply or error, as in nested `with`
statements; that's how `asyncio.timeout` turns its cancellation into `TimeoutError`. The call
runs exactly once, in the bus's own task, so a retry belongs to the handler, or to the adapter.

Middleware can send context with a message in its headers, such as a trace or a tenant. Publish
middleware sets them on `call.headers`, a {py:class}`MessageHeaders`. Handle middleware reads
them from its own `call.headers`, which it can't change. Over a broker, the headers travel in
the message. On a direct bus, they're passed to the handlers of the same emit. Names and values
are text. The envelope's headers are the bus's own, so setting one raises `ValueError`, and the
message isn't sent.

````{example} Tracing every request a bus handles
```{code-block} python
from contextlib import contextmanager

from stratae.events import DirectBus, Middleware, Request

class PlaceOrder:
    def __init__(self, item: str) -> None:
        self.item = item

class Order:
    def __init__(self, item: str) -> None:
        self.item = item

@contextmanager
def trace(call):
    print(f"-> {call.event.name}")
    try:
        yield
    except Exception as error:
        print(f"<- {call.event.name} failed: {error}")
        raise
    print(f"<- {call.event.name}: {type(call.reply).__name__}")

place_order_event = Request(PlaceOrder, reply=Order)
bus = DirectBus(middleware=Middleware(publish=(trace,)))
place_order = bus.bind(place_order_event, factory=PlaceOrder)

@bus.handle(place_order_event)
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
-> PlaceOrder
<- PlaceOrder: Order
-> PlaceOrder
<- PlaceOrder failed: out of stock
```
````

````{example} Sending a tenant header from publish middleware to handle middleware
```{code-block} python
from contextlib import nullcontext

from stratae.events import DirectBus, Middleware, PubSub

class OrderPlaced:
    def __init__(self, item: str) -> None:
        self.item = item

def send_tenant(call):
    call.headers["x-tenant"] = "acme"
    return nullcontext()

def log_tenant(call):
    print(f"handling {call.payload.item} for {call.headers['x-tenant']}")
    return nullcontext()

order_placed_event = PubSub(OrderPlaced)
bus = DirectBus(middleware=Middleware(publish=(send_tenant,), handle=(log_tenant,)))
place_order = bus.bind(order_placed_event, factory=OrderPlaced)
bus.handle(order_placed_event, lambda order: None)

place_order(item="lamp")
```
```{output}
handling lamp for acme
```
````
"""

import asyncio
from collections.abc import Awaitable, Callable, Collection, Iterator, Mapping, MutableMapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from types import CoroutineType, MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    NoReturn,
)

from stratae.events._frozen import freeze_tuples
from stratae.events.envelope import ENVELOPE_HEADERS
from stratae.events.exceptions import MiddlewareError

if TYPE_CHECKING:
    from stratae.events.protocols import AnyEvent

type _Manager = AbstractContextManager[object] | AbstractAsyncContextManager[object]

_SYNC_BUS_REJECTED = "{} enters middleware synchronously; {!r} isn't a sync context manager"

_REJECTED = "{} can't enter {!r}: it isn't a context manager"

_NOT_SYNC = (
    "middleware {!r} returned {!r}, which a sync bus can't enter: it isn't a sync context manager"
)

_NOT_A_MANAGER = "middleware {!r} returned {!r}, which isn't a context manager"

_SWALLOWED = "middleware {!r} tried to swallow {!r}, but middleware can't swallow errors"

_NO_HEADERS: Mapping[str, str] = MappingProxyType({})

_RESERVED = "{!r} is a header the bus sets itself, so middleware can't set it"

_ASYNC_METHOD = (
    "middleware {!r} has an async __enter__ or __exit__; an async context manager defines"
    " __aenter__ and __aexit__"
)

_type_of: Callable[[object], Any] = type
"""`type`, typed to return `Any`: a context manager's methods are looked up on its type."""

type _Entered = list[tuple[Any, Any, Callable[[Any], Any]]]
"""
The context managers entered so far, innermost last: each with its type, which its methods
are looked up on, as `with` does, and the middleware it came from.
"""

type _EnteredAsync = list[tuple[Any, Any, Callable[[Any], Any], bool]]
"""The same, with whether each one was entered asynchronously."""


@dataclass(frozen=True, slots=True)
class Middleware:
    """
    The middleware a bus runs: `publish` around each emit, `handle` around each handler.

    A broker adapter runs one half: a publisher its `publish` middleware, a consumer its
    `handle` middleware. So one `Middleware` can be shared by both.
    """

    publish: tuple[Callable[["PublishCall"], _Manager], ...] = ()
    """Run around each emit, outermost first."""
    handle: tuple[Callable[["HandleCall"], _Manager], ...] = ()
    """Run around each handler call, outermost first."""

    def __post_init__(self) -> None:
        """Freeze each slot as a tuple, refusing a lone middleware given in place of one."""
        freeze_tuples(self, ("publish", "handle"), "middleware", "trace")

    def require(
        self, bus: str, *, asynchronous: bool, publish: bool = True, handle: bool = True
    ) -> None:
        """
        Raise TypeError if any middleware in the selected halves is a class a bus can't enter.

        Only a class can be checked before it's called; the bus checks other callables on each
        call.

        :param bus: The bus's name, for the error message.
        :param asynchronous: Whether the bus runs middleware asynchronously.
        :param publish: Whether to check the `publish` half; a consumer doesn't run it.
        :param handle: Whether to check the `handle` half; a publisher doesn't run it.
        """
        selected = (*(self.publish if publish else ()), *(self.handle if handle else ()))
        message = _REJECTED if asynchronous else _SYNC_BUS_REJECTED
        for item in selected:
            if isinstance(item, type) and _entry(item, asynchronous) is None:
                raise TypeError(message.format(bus, item))


class MessageHeaders(MutableMapping[str, str]):
    """
    The headers a publish middleware adds to the message being published.

    Names and values are text. A name the bus sets itself, such as an
    {py:class}`Envelope <stratae.events.envelope.Envelope>` header, is reserved, compared
    ignoring case: setting one raises `ValueError`, so the message isn't sent.
    """

    __slots__ = ("_headers", "_reserved")

    def __init__(
        self,
        headers: Mapping[str, str] = _NO_HEADERS,
        *,
        reserved: Collection[str] = ENVELOPE_HEADERS,
    ) -> None:
        """
        Create the headers for one message.

        :param headers: Headers the message already carries, such as a re-published
            message's.
        :param reserved: The names the bus sets itself, in lower case.
        :raises ValueError: If a name is reserved.
        """
        self._reserved = reserved
        self._headers: dict[str, str] = {}
        self.update(headers)

    def __setitem__(self, name: str, value: str) -> None:
        """Set a header, refusing a reserved name."""
        if name.lower() in self._reserved:
            raise ValueError(_RESERVED.format(name))
        self._headers[name] = value

    def __getitem__(self, name: str) -> str:
        """Return a header's value."""
        return self._headers[name]

    def __delitem__(self, name: str) -> None:
        """Remove a header."""
        del self._headers[name]

    def __iter__(self) -> Iterator[str]:
        """Iterate over the header names."""
        return iter(self._headers)

    def __len__(self) -> int:
        """Return how many headers are set."""
        return len(self._headers)

    def __repr__(self) -> str:
        """Show the headers."""
        return f"MessageHeaders({self._headers!r})"


class _Call:
    """What a middleware sees of one call."""

    __slots__ = ("config", "event", "headers", "payload", "reply")

    def __init__(self, event: "AnyEvent", config: Any, payload: Any) -> None:
        """Describe one call for a middleware; buses build these."""
        self.event = event
        """The event being emitted or handled."""
        self.config = config
        """
        The adapter's routing config for the call: an emit's `config`, or the registration's
        on a consumer, such as RabbitMQ's exchange and routing key. `None` on a direct bus.
        """
        self.payload = payload
        """The payload being emitted or handled."""
        self.reply: Any = None
        """The call's reply once it has returned, for the exits to see; `None` until then."""


class PublishCall(_Call):
    """One emit, as a publish middleware sees it."""

    __slots__ = ()

    def __init__(
        self, event: "AnyEvent", config: Any, payload: Any, headers: MessageHeaders
    ) -> None:
        """Describe one emit for a publish middleware; buses build these."""
        super().__init__(event, config, payload)
        self.headers = headers
        """
        The headers the message is published with. Set one to send it along, for handle
        middleware to read.
        """


class HandleCall(_Call):
    """One handler call, as a handle middleware sees it."""

    __slots__ = ()

    def __init__(
        self, event: "AnyEvent", config: Any, payload: Any, headers: Mapping[str, str]
    ) -> None:
        """Describe one handler call for a handle middleware; buses build these."""
        super().__init__(event, config, payload)
        self.headers = headers
        """
        The headers of the message being handled, read-only. On a direct bus, they're the ones
        its publish middleware set. Over a broker, they're the message's text headers, which
        include its envelope's.
        """


def _entry(kind: type, asynchronous: bool) -> bool | None:
    """
    Return how a bus enters a context manager of type `kind`.

    :returns: `True` to enter it asynchronously, `False` synchronously, or `None` if the bus
        can't enter it.
    """
    if asynchronous and hasattr(kind, "__aenter__") and hasattr(kind, "__aexit__"):
        return True
    if hasattr(kind, "__enter__") and hasattr(kind, "__exit__"):
        return False
    return None


def _manager(
    middleware: Callable[[Any], Any], call: _Call, asynchronous: bool
) -> tuple[Any, Any, bool]:
    """
    Call a middleware, returning its context manager, the manager's type, and if it's async.

    :raises TypeError: If it returned something the bus can't enter.
    """
    manager = middleware(call)
    kind = _type_of(manager)
    entry = _entry(kind, asynchronous)
    if entry is None:
        if manager.__class__ is CoroutineType:
            manager.close()  # an async def middleware's, closed so it isn't left unawaited
        message = _NOT_A_MANAGER if asynchronous else _NOT_SYNC
        raise TypeError(message.format(middleware, manager))
    return manager, kind, entry


def _refuse_coroutine(result: Any, middleware: object) -> NoReturn:
    """
    Raise TypeError for a sync `__enter__` or `__exit__` that's an `async def`.

    Its coroutine is closed, so it isn't left unawaited. Callers check for a coroutine
    themselves, keeping a call off each layer's normal path. Only a coroutine is refused,
    being unambiguously an `async def` mistake: anything else `__enter__` returns is the
    context manager's own, awaitable or not, and is left alone.
    """
    result.close()
    raise TypeError(_ASYNC_METHOD.format(middleware))


def _refuse_swallow(error: BaseException, middleware: object, cancelling: bool) -> NoReturn:
    """
    Raise in place of an exit that returned true, trying to swallow `error`.

    An ordinary error becomes MiddlewareError from it, so the call still fails as an error.
    Anything else, such as `KeyboardInterrupt`, is raised again as it is, so it still stops
    what it was meant to stop: turned into an ordinary error, `except Exception` would catch
    it. A cancellation is raised again only while the task is still being cancelled. One the
    middleware made itself, timing the call out, has been uncancelled by the time it swallows
    the timeout, and re-raising it would look like the task being cancelled.

    :param cancelling: Whether the task running the call is still being cancelled.
    """
    if isinstance(error, Exception) or (
        isinstance(error, asyncio.CancelledError) and not cancelling
    ):
        raise MiddlewareError(_SWALLOWED.format(middleware, error)) from error
    raise error


def _task_cancelling() -> bool:
    """
    Return whether the current task is being cancelled, as a swallowed cancellation shows.

    The call runs in this task, so its count is the call's: `asyncio.timeout` uncancels the
    task when it turns its own cancellation into `TimeoutError`. Code earlier in the task that
    caught a cancellation without uncancelling leaves the count raised, and a swallowed
    timeout's cancellation is then raised again rather than reported. Outside a task, a
    cancellation can't be the task's.
    """
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _exit(entered: _Entered, call: _Call) -> None:
    """
    Exit the entered context managers innermost first, after the call succeeded.

    An exit that raises fails the call, so the rest exit with that error, and no reply.
    """
    while entered:
        manager, kind, middleware = entered.pop()
        try:
            result = kind.__exit__(manager, None, None, None)
            if result.__class__ is CoroutineType:
                _refuse_coroutine(result, middleware)
        except BaseException as error:
            call.reply = None
            _unwind(entered, error)


def _unwind(entered: _Entered, error: BaseException) -> NoReturn:
    """
    Exit the entered context managers innermost first with `error`, then raise it.

    Like nested `with` statements, except that an exit can't swallow the error. An exit
    raising a new error replaces it, and the rest exit with that one. It's called while
    `error` is being handled, so a new error chains from it, as in a `with`.
    """
    while entered:
        manager, kind, middleware = entered.pop()
        try:
            exc_info = (type(error), error, error.__traceback__)
            suppressed = kind.__exit__(manager, *exc_info)
            if suppressed.__class__ is CoroutineType:
                _refuse_coroutine(suppressed, middleware)
            if suppressed:
                # A sync bus has no task to cancel, so any cancellation is raised again.
                _refuse_swallow(error, middleware, cancelling=True)
        except BaseException as raised:
            if raised is not error:
                _unwind(entered, raised)
    raise error


def run_middleware[T](
    middleware: tuple[Callable[..., Any], ...],
    seen: _Call,
    call: Callable[..., T],
    *args: Any,
) -> T:
    """
    Run `call(*args)` inside sync middleware, returning or raising what it does.

    :param middleware: The middleware, outermost first.
    :param seen: What each middleware is given: a {py:class}`PublishCall` around an emit, a
        {py:class}`HandleCall` around a handler call.
    :param call: The emit or handler call the middleware wraps.
    :returns: What `call` returns.
    :raises TypeError: If a middleware doesn't return a sync context manager.
    """
    entered: _Entered = []
    try:
        for item in middleware:
            manager, kind, _ = _manager(item, seen, asynchronous=False)
            result = kind.__enter__(manager)
            if result.__class__ is CoroutineType:
                _refuse_coroutine(result, item)
            entered.append((manager, kind, item))
        reply = call(*args)
    except BaseException as error:
        _unwind(entered, error)
    seen.reply = reply
    _exit(entered, seen)
    return reply


async def _exit_async(entered: _EnteredAsync, call: _Call) -> None:
    """Exit the entered context managers, sync or async, the way {py:func}`_exit` does."""
    while entered:
        manager, kind, middleware, is_async = entered.pop()
        try:
            if is_async:
                await kind.__aexit__(manager, None, None, None)
            elif (result := kind.__exit__(manager, None, None, None)).__class__ is CoroutineType:
                _refuse_coroutine(result, middleware)
        except BaseException as error:
            call.reply = None
            await _unwind_async(entered, error)


async def _unwind_async(entered: _EnteredAsync, error: BaseException) -> NoReturn:
    """Exit the entered context managers, sync or async, the way {py:func}`_unwind` does."""
    while entered:
        manager, kind, middleware, is_async = entered.pop()
        try:
            exc_info = (type(error), error, error.__traceback__)
            if is_async:
                suppressed = await kind.__aexit__(manager, *exc_info)
            elif (suppressed := kind.__exit__(manager, *exc_info)).__class__ is CoroutineType:
                _refuse_coroutine(suppressed, middleware)
            if suppressed:
                _refuse_swallow(error, middleware, cancelling=_task_cancelling())
        except BaseException as raised:
            if raised is not error:
                await _unwind_async(entered, raised)
    raise error


async def run_middleware_async[T](
    middleware: tuple[Callable[..., Any], ...],
    seen: _Call,
    call: Callable[..., Awaitable[T]],
    *args: Any,
) -> T:
    """
    Await `call(*args)` inside async middleware, returning or raising its result.

    :param middleware: The middleware, outermost first.
    :param seen: What each middleware is given: a {py:class}`PublishCall` around an emit, a
        {py:class}`HandleCall` around a handler call.
    :param call: The emit or handler call the middleware wraps.
    :returns: What `call` resolves to.
    :raises TypeError: If a middleware doesn't return a context manager.
    """
    entered: _EnteredAsync = []
    try:
        for item in middleware:
            manager, kind, is_async = _manager(item, seen, asynchronous=True)
            if is_async:
                await kind.__aenter__(manager)
            elif (result := kind.__enter__(manager)).__class__ is CoroutineType:
                _refuse_coroutine(result, item)
            entered.append((manager, kind, item, is_async))
        reply = await call(*args)
    except BaseException as error:
        await _unwind_async(entered, error)
    seen.reply = reply
    await _exit_async(entered, seen)
    return reply
