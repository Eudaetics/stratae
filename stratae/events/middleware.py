"""
Wrap every emit or handler call on a bus with middleware, for cross-cutting concerns.

{py:class}`Middleware` holds the middleware a bus runs: `publish` around each emit, and
`handle` around each handler call. Give it to the bus when it's built, with `middleware=`;
it's fixed from then on. The first middleware listed is outermost, and middleware wraps the
event's {py:class}`Hooks <stratae.events.hooks.Hooks>`, which wrap the call itself. Both run
inside the emission's envelope when the bus opens one: always on a broker adapter, and on a
direct bus built with an `envelope`.

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
on each call: one returning something the bus can't enter raises
{py:exc}`MiddlewareResultError <stratae.middleware.MiddlewareResultError>`. A coroutine
returned, from an `async def` middleware, is closed; any other awaitable, such as a task, isn't
the bus's to cancel, so it's left as it is.

Middleware wraps the call, but can't change its reply or swallow its error. An exit that
returns true, trying to swallow an ordinary error, raises
{py:exc}`MiddlewareError <stratae.middleware.MiddlewareError>` from it instead, so the
call still fails, and outer middleware sees that. Trying to swallow anything else, such as
`KeyboardInterrupt` or the task's cancellation, raises it again as it is, so it still stops
what it was meant to stop. A cancellation the middleware made itself, by timing the call out,
isn't the task's: swallowing the timeout raises `MiddlewareError`. A middleware can raise an
error of its own. Raised on entry, it refuses the call, and the middleware already entered see
it on exit. Raised on exit, it replaces the call's reply or error, as in nested `with`
statements; that's how `asyncio.timeout` turns its cancellation into `TimeoutError`. The call
runs exactly once, in the bus's own task, so a retry belongs to the handler, or to the adapter.
These rules are the middleware contract in {py:mod}`stratae.middleware`, which lifecycle scopes
follow too.

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

from collections.abc import Awaitable, Callable, Collection, Iterator, Mapping, MutableMapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from types import CoroutineType, MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    NoReturn,
)

from stratae._checked import checked_callables
from stratae._middleware import (
    manager,
    refuse_coroutine,
    refuse_result,
    refuse_swallow,
    refuse_unenterable,
    task_cancelling,
    type_of,
)
from stratae.events.envelope import ENVELOPE_HEADERS

if TYPE_CHECKING:
    from stratae.events.protocols import AnyEvent

type _Manager = AbstractContextManager[object] | AbstractAsyncContextManager[object]

_NO_HEADERS: Mapping[str, str] = MappingProxyType({})

_RESERVED = "{!r} is a header the bus sets itself, so middleware can't set it"

type _Entered = list[tuple[Any, Any, Callable[[Any], Any]]]
"""
The context managers entered so far, innermost last: each with its `__exit__`, looked up on
its type as `with` does, and the middleware it came from.
"""

type _EnteredAsync = list[tuple[Any, Any, Callable[[Any], Any], bool]]
"""
The context managers an async bus has entered so far, innermost last: each with its type,
which its methods are looked up on, the middleware it came from, and whether it was entered
asynchronously.
"""


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
        """
        Freeze each half as a tuple, refusing a lone middleware and anything uncallable.

        :raises TypeError: If a half is a single callable or a string, or holds something that
            isn't callable.
        """
        for field in ("publish", "handle"):
            items = checked_callables(getattr(self, field), f"Middleware {field}=")
            object.__setattr__(self, field, items)

    def require(
        self, bus: str, *, asynchronous: bool, publish: bool = True, handle: bool = True
    ) -> None:
        """
        Refuse any middleware in the selected halves that's a class the bus can't enter.

        Only a class can be checked before it's called; the bus checks other callables on each
        call.

        :param bus: The bus's name, for the error message.
        :param asynchronous: Whether the bus runs middleware asynchronously.
        :param publish: Whether to check the `publish` half; a consumer doesn't run it.
        :param handle: Whether to check the `handle` half; a publisher doesn't run it.
        :raises MiddlewareNotEnterableError: If a selected middleware is such a class.
        """
        selected = (*(self.publish if publish else ()), *(self.handle if handle else ()))
        refuse_unenterable(selected, bus, asynchronous)


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
        if headers:
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

    def to_dict(self) -> dict[str, str]:
        """
        Return the headers as a new plain `dict`, for sending them with the message.

        Faster than `dict(headers)`, which reads a mapping one header at a time.
        """
        return self._headers.copy()

    def __repr__(self) -> str:
        """Show the headers."""
        return f"MessageHeaders({self._headers!r})"


class _Call:
    """What a middleware sees of one call."""

    __slots__ = ("config", "event", "headers", "payload", "reply")

    def __init__(self, event: "AnyEvent", config: Any, payload: Any) -> None:
        """Describe one call for a middleware; buses build these."""
        self.event: AnyEvent = event
        """The event being emitted or handled."""
        self.config: Any = config
        """
        The adapter's routing config for the call, such as RabbitMQ's exchange and routing key.
        On a publisher, it's the config of the event's route. A route that makes each message's
        routing key from its payload gives one with that message's key. On a consumer, it's the
        registration's. It's `None` on a direct bus.
        """
        self.payload: Any = payload
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


def _exit(entered: _Entered, call: _Call) -> None:
    """
    Exit the entered context managers innermost first, after the call succeeded.

    An exit that raises fails the call, so the rest exit with that error, and no reply.
    """
    while entered:
        context, leave, middleware = entered.pop()
        try:
            result = leave(context, None, None, None)
            if result.__class__ is CoroutineType:
                refuse_coroutine(result, middleware)
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
        context, leave, middleware = entered.pop()
        try:
            exc_info = (type(error), error, error.__traceback__)
            suppressed = leave(context, *exc_info)
            if suppressed.__class__ is CoroutineType:
                refuse_coroutine(suppressed, middleware)
            if suppressed:
                # A sync bus has no task to cancel, so any cancellation is raised again.
                refuse_swallow(error, middleware, cancelling=True)
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
    :raises MiddlewareResultError: If a middleware doesn't return a sync context manager.
    """
    entered: _Entered = []
    try:
        for item in middleware:
            context = item(seen)
            kind = type_of(context)
            try:
                enter, leave = kind.__enter__, kind.__exit__
            except AttributeError:
                refuse_result(item, context, False)
            if (result := enter(context)).__class__ is CoroutineType:
                refuse_coroutine(result, item)
            entered.append((context, leave, item))
        reply = call(*args)
    except BaseException as error:
        _unwind(entered, error)
    seen.reply = reply
    _exit(entered, seen)
    return reply


async def _exit_async(entered: _EnteredAsync, call: _Call) -> None:
    """Exit the entered context managers, sync or async, the way {py:func}`_exit` does."""
    while entered:
        context, kind, middleware, is_async = entered.pop()
        try:
            if is_async:
                await kind.__aexit__(context, None, None, None)
            elif (result := kind.__exit__(context, None, None, None)).__class__ is CoroutineType:
                refuse_coroutine(result, middleware)
        except BaseException as error:
            call.reply = None
            await _unwind_async(entered, error)


async def _unwind_async(entered: _EnteredAsync, error: BaseException) -> NoReturn:
    """Exit the entered context managers, sync or async, the way {py:func}`_unwind` does."""
    while entered:
        context, kind, middleware, is_async = entered.pop()
        try:
            exc_info = (type(error), error, error.__traceback__)
            if is_async:
                suppressed = await kind.__aexit__(context, *exc_info)
            elif (suppressed := kind.__exit__(context, *exc_info)).__class__ is CoroutineType:
                refuse_coroutine(suppressed, middleware)
            if suppressed:
                refuse_swallow(error, middleware, cancelling=task_cancelling())
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
    :raises MiddlewareResultError: If a middleware doesn't return a context manager.
    """
    entered: _EnteredAsync = []
    try:
        for item in middleware:
            context, kind, is_async = manager(item, seen)
            if is_async:
                await kind.__aenter__(context)
            elif (result := kind.__enter__(context)).__class__ is CoroutineType:
                refuse_coroutine(result, item)
            entered.append((context, kind, item, is_async))
        reply = await call(*args)
    except BaseException as error:
        await _unwind_async(entered, error)
    seen.reply = reply
    await _exit_async(entered, seen)
    return reply
