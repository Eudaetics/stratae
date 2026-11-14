"""
Wrap every emit or handler call on a bus with middleware, for cross-cutting concerns.

{py:class}`Middleware` holds the middleware a bus runs: `publish` around each emit, and
`handle` around each handler call. Give it to the bus when it's built, with `middleware=`;
it's fixed from then on. The first middleware listed is outermost, and middleware wraps the
event's {py:class}`Hooks <stratae.events.hooks.Hooks>`, which wrap the call itself. Both run
inside the emission's envelope when the bus opens one: always on a broker adapter, and on a
direct bus built with an `envelope`.

A middleware is a function taking the call and `next`, the rest of the chain. It calls `next`
with the call, or with a new call carrying added headers, and returns what `next` returns:
`def middleware(call, next): return next(call)`. The call is a value, a {py:class}`PublishCall`
or a {py:class}`HandleCall`, which nothing changes. On an async bus, a middleware is an `async
def`, and awaits `next`: `async def middleware(call, next): return await next(call)`. So a
timeout is `async with asyncio.timeout(5): return await next(call)`. A sync bus refuses an
`async def` middleware, and an async bus refuses any other, when the bus is built.

Middleware wraps the call, but can't change its reply or swallow its error. It must call `next`
exactly once and return its reply. A middleware that skips `next`, calls it twice, or returns
something else raises
{py:exc}`MiddlewareNextError <stratae.middleware.MiddlewareNextError>`. One that catches the
call's error and returns raises {py:exc}`MiddlewareError <stratae.middleware.MiddlewareError>`
from it instead, so the call still fails. Trying to swallow anything else, such as
`KeyboardInterrupt` or the task's cancellation, raises it again as it is. A middleware can
raise an error of its own, before calling `next` to refuse the call, or after it to replace the
call's reply or error. That's how `asyncio.timeout` turns its cancellation into
`TimeoutError`.

Publish middleware sends context with a message, such as a trace or a tenant, by passing `next`
the call {py:meth}`PublishCall.with_headers` returns. Handle middleware reads them from its own
`call.headers`. Over a broker, the headers travel in the message. On a direct bus, they're
passed to the handlers of the same emit. Names and values are text. The envelope's headers are
the bus's own, so adding one raises `ValueError`, and the message isn't sent.

````{example} Tracing every request a bus handles
```{code-block} python
from stratae.events import DirectBus, Middleware, Request

class PlaceOrder:
    def __init__(self, item: str) -> None:
        self.item = item

class Order:
    def __init__(self, item: str) -> None:
        self.item = item

def trace(call, next):
    print(f"-> {call.event.name}")
    try:
        reply = next(call)
    except Exception as error:
        print(f"<- {call.event.name} failed: {error}")
        raise
    print(f"<- {call.event.name}: {type(reply).__name__}")
    return reply

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
from stratae.events import DirectBus, Middleware, PubSub

class OrderPlaced:
    def __init__(self, item: str) -> None:
        self.item = item

def send_tenant(call, next):
    return next(call.with_headers({"x-tenant": "acme"}))

def log_tenant(call, next):
    print(f"handling {call.payload.item} for {call.headers['x-tenant']}")
    return next(call)

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

from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, NamedTuple

from stratae._checked import checked_callables
from stratae._middleware import refuse_swallow, task_cancelling
from stratae.callables import is_async_callable
from stratae.events.envelope import ENVELOPE_HEADERS
from stratae.middleware import MiddlewareKindError, MiddlewareNextError

if TYPE_CHECKING:
    from stratae.events.protocols import AnyEvent

NO_HEADERS: Mapping[str, str] = MappingProxyType({})
"""A message's headers when it has none."""

_RESERVED = "{!r} is a header the bus sets itself, so middleware can't set it"


class PublishCall(NamedTuple):
    """
    One emit, as a publish middleware sees it.

    Its `headers` are the ones the message is published with. To send another along, for handle
    middleware to read, pass `next` the call {py:meth}`with_headers` returns.
    """

    event: "AnyEvent"
    """The event being emitted."""
    config: Any
    """
    The adapter's routing config for the call, such as RabbitMQ's exchange and routing key.
    It's the config of the event's route, with a message's own routing key when the route
    makes one from its payload. It's `None` on a direct bus.
    """
    payload: Any
    """The payload being emitted."""
    headers: Mapping[str, str] = NO_HEADERS
    """The headers the message is published with, read-only."""
    reserved: Collection[str] = ENVELOPE_HEADERS
    """The header names the bus sets itself, in lower case, which middleware can't add."""

    def with_headers(self, headers: Mapping[str, str]) -> "PublishCall":
        """
        Return this call, publishing `headers` as well.

        :raises ValueError: If a name is one the bus sets itself, compared ignoring case.
        """
        for name in headers:
            if name.lower() in self.reserved:
                raise ValueError(_RESERVED.format(name))
        return self._replace(headers=MappingProxyType({**self.headers, **headers}))


class HandleCall(NamedTuple):
    """
    One handler call, as a handle middleware sees it.

    Its `headers` are the ones of the message being handled. On a direct bus, they're the ones
    its publish middleware added. Over a broker, they're the message's text headers, which
    include its envelope's.
    """

    event: "AnyEvent"
    """The event being handled."""
    config: Any
    """The registration's routing config, or `None` on a direct bus."""
    payload: Any
    """The payload being handled."""
    headers: Mapping[str, str] = NO_HEADERS
    """The headers of the message being handled, read-only."""


type Next[C, R] = Callable[[C], R]
"""The rest of the chain: the next middleware, or else the call itself."""


@dataclass(frozen=True, slots=True)
class Middleware:
    """
    The middleware a bus runs: `publish` around each emit, `handle` around each handler.

    A broker adapter runs one half: a publisher its `publish` middleware, a consumer its
    `handle` middleware. So one `Middleware` can be shared by both.
    """

    publish: tuple[Callable[[PublishCall, Next[PublishCall, Any]], Any], ...] = ()
    """Run around each emit, outermost first."""
    handle: tuple[Callable[[HandleCall, Next[HandleCall, Any]], Any], ...] = ()
    """Run around each handler call, outermost first."""

    def __post_init__(self) -> None:
        """
        Freeze each half as a tuple, refusing a lone middleware and anything uncallable.

        :raises TypeError: If a half is a single callable or a string, or holds something that
            isn't callable.
        """
        for half in ("publish", "handle"):
            items = checked_callables(getattr(self, half), f"Middleware {half}=")
            object.__setattr__(self, half, items)

    def require(
        self, bus: str, *, asynchronous: bool, publish: bool = True, handle: bool = True
    ) -> None:
        """
        Refuse any middleware in the selected halves of the wrong kind for the bus.

        :param bus: The bus's name, for the error message.
        :param asynchronous: Whether the bus runs middleware asynchronously.
        :param publish: Whether to check the `publish` half; a consumer doesn't run it.
        :param handle: Whether to check the `handle` half; a publisher doesn't run it.
        :raises MiddlewareKindError: If an async bus is given a middleware that isn't an
            `async def`, or a sync bus one that is.
        """
        selected: tuple[Callable[..., Any], ...] = (
            *(self.publish if publish else ()),
            *(self.handle if handle else ()),
        )
        for item in selected:
            if is_async_callable(item) is not asynchronous:
                raise MiddlewareKindError(bus, item, asynchronous)


type Finish[C, X] = Callable[[C, X], Any]
"""
The innermost call of a chain: given the call value the last middleware passed on, and what
the chain was run with for this call, such as the emit's message and how to send it.
"""


class _Next:
    """The `next` one middleware is given for one call: runs the rest of the chain once."""

    __slots__ = ("called", "context", "error", "inner", "middleware", "reply")

    def __init__(self, inner: Finish[Any, Any], context: Any, middleware: object) -> None:
        self.inner = inner
        self.context = context
        self.middleware = middleware
        self.called = False
        self.error: BaseException | None = None
        self.reply: Any = None

    def __call__(self, call: Any) -> Any:
        if self.called:
            raise MiddlewareNextError(self.middleware, "called next twice")
        self.called = True
        try:
            reply = self.inner(call, self.context)
        except BaseException as error:
            self.error = error
            raise
        self.reply = reply
        return reply


class _AsyncNext(_Next):
    """The `next` one async middleware is given for one call: awaits the rest once."""

    __slots__ = ()

    async def __call__(self, call: Any) -> Any:
        if self.called:
            raise MiddlewareNextError(self.middleware, "called next twice")
        self.called = True
        try:
            reply = await self.inner(call, self.context)
        except BaseException as error:
            self.error = error
            raise
        self.reply = reply
        return reply


def _settled(middleware: object, nxt: _Next, returned: Any, cancelling: bool) -> Any:
    """
    Return what a middleware returned, if it kept the contract.

    :raises MiddlewareError: If it caught the call's error and returned.
    :raises MiddlewareNextError: If it skipped `next`, or returned something else.
    """
    if nxt.error is not None:
        refuse_swallow(nxt.error, middleware, cancelling)
    if not nxt.called:
        raise MiddlewareNextError(middleware, "never called next")
    if returned is not nxt.reply:
        raise MiddlewareNextError(middleware, "returned something other than next's reply")
    return returned


def chain[C, X](middleware: tuple[Callable[..., Any], ...], finish: Finish[C, X]) -> Finish[C, X]:
    """
    Compose sync middleware around `finish`, outermost first, checking each keeps the contract.

    The chain is built once, and run per call with that call's value and context.

    :param middleware: The middleware, outermost first.
    :param finish: The innermost call.
    :returns: The composed chain, taking the call value and the context to finish it with.
    """
    inner: Finish[Any, Any] = finish
    for item in reversed(middleware):
        inner = _step(item, inner)
    return inner


def _step(item: Callable[..., Any], inner: Finish[Any, Any]) -> Finish[Any, Any]:
    def step(call: Any, context: Any) -> Any:
        nxt = _Next(inner, context, item)
        return _settled(item, nxt, item(call, nxt), True)

    return step


def chain_async[C, X](
    middleware: tuple[Callable[..., Any], ...], finish: Finish[C, X]
) -> Finish[C, X]:
    """Compose async middleware around `finish`, as {py:func}`chain` does, awaiting each layer."""
    inner: Finish[Any, Any] = finish
    for item in reversed(middleware):
        inner = _step_async(item, inner)
    return inner


def _step_async(item: Callable[..., Any], inner: Finish[Any, Any]) -> Finish[Any, Any]:
    async def step(call: Any, context: Any) -> Any:
        nxt = _AsyncNext(inner, context, item)
        return _settled(item, nxt, await item(call, nxt), task_cancelling())

    return step
