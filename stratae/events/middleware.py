"""
Wrap every emit or handler call on a bus with middleware, for cross-cutting concerns.

{py:class}`Middleware` holds the middleware a bus runs: `publish` around each emit, and
`handle` around each handler call. Give it to the bus when it's built, with `middleware=`;
it's fixed from then on. The first middleware listed is outermost, and middleware wraps the
event's {py:class}`Hooks <stratae.events.hooks.Hooks>`, which wrap the call itself. Both run
inside the emission's envelope when the bus opens one: always on a broker adapter, and on a
direct bus built with an `envelope`.

A middleware is a function taking the call and `next`, the rest of the chain. It calls `next`
with the call, or with a new one, and returns what `next` returns:
`def middleware(call, next): return next(call)`. The call is a value, a {py:class}`PublishCall`
or a {py:class}`HandleCall`, which nothing changes. A new call can carry another payload, and
on publish, added headers. Its event and config must stay the same: the route they name is
chosen before middleware runs, so a call with others raises `ValueError`.

On an async bus, a middleware is an `async def`, and awaits `next`:
`async def middleware(call, next): return await next(call)`. So a timeout is
`async with asyncio.timeout(5): return await next(call)`. A sync bus takes only a plain function
returning a value, and an async bus only an `async def`. Anything else is refused when the bus
is built.

A middleware decides what happens to the call. It usually calls `next` once and returns its
reply, but it can do otherwise on purpose. It can raise before `next` to refuse the call,
return without calling `next` to skip it, call `next` again to retry, return a different reply,
or catch the call's error and return in its place. On a broker adapter, that decides what's
delivered. A publish middleware that returns without sending, or catches the send's error,
makes the emit succeed without a message, and an outbox's relay then marks the record sent. A
handle middleware that catches the handler's error makes the delivery succeed, so the consumer
acks it.

Publish middleware sends context with a message, such as a trace or a tenant, by passing `next`
the call {py:meth}`PublishCall.with_headers` returns. Handle middleware reads them from its own
`call.headers`. Over a broker, the headers travel in the message. On a direct bus, they're
passed to the handlers of the same emit. Names are text. Values are whatever the transport
carries: a direct bus passes any value on as it is, and a broker adapter refuses one its
transport can't hold when the message is sent. The envelope's headers are the bus's own, and
so are any the route's adapter sets itself, such as RabbitMQ's attempt count. Adding one
raises `ValueError` once the middleware returns, and the message isn't sent.

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

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, NamedTuple

from stratae._checked import checked_callables
from stratae.middleware import Next, refuse_kind

if TYPE_CHECKING:
    from stratae.events.kind import Kind
    from stratae.events.protocols import AnyEvent

NO_HEADERS: Mapping[str, object] = MappingProxyType({})
"""A message's headers when it has none."""

from_fields: Callable[[type[Any], tuple[Any, ...]], Any] = tuple[Any, ...].__new__
"""
Build a named tuple, such as a call, from a tuple of its fields.

It skips the generated `__new__` and `_replace`'s keyword handling, which a call made on every
emit would pay for.
"""


class PublishCall(NamedTuple):
    """
    One emit, as a publish middleware sees it.

    The call passed to `next` is what's sent: its payload, and its headers. To send another
    header along, for handle middleware to read, pass `next` the call {py:meth}`with_headers`
    returns. The event and config name the route, which is chosen before middleware runs, so
    passing on a call with others raises `ValueError`.
    """

    event: "AnyEvent"
    """The event being emitted."""
    config: Any
    """
    Where the adapter publishes the call, as the adapter describes it. On RabbitMQ, it's a
    {py:class}`RabbitMQPublish <stratae.integrations.rabbitmq.config.RabbitMQPublish>`: the
    event's route, and the message's routing key. On the outbox, it's the route's
    {py:class}`OutboxConfig <stratae.events.outbox.OutboxConfig>`. It's `None` on a direct bus.
    """
    payload: Any
    """The payload being emitted."""
    headers: Mapping[str, object] = NO_HEADERS
    """The headers the message is published with, read-only."""

    def with_headers(self, headers: Mapping[str, object]) -> "PublishCall":
        """
        Return this call, publishing `headers` as well.

        A name the bus sets itself, such as the envelope's, is refused with `ValueError` when
        the call is sent, compared ignoring case.
        """
        event, config, payload, current = self
        return from_fields(
            PublishCall, (event, config, payload, MappingProxyType({**current, **headers}))
        )


class HandleCall(NamedTuple):
    """
    A message being handled: what a dispatcher gives each handler, and handle middleware sees.

    An adapter builds one for each message it handles. Its `headers` are the message's,
    without its envelope's, on every bus. On a direct bus, they're the ones given to `emit` and
    the ones publish middleware added. Over a broker, they're the headers the message arrived
    with, as the transport delivered them, beside any the adapter sets itself, such as an
    attempt count. The envelope is read as a value, from
    {py:meth}`Envelope.current <stratae.events.envelope.Envelope.current>`. The handler is
    called with the payload of the call passed to `next`. The event, config and headers
    describe the message being handled, so passing on a call with others raises `ValueError`.
    """

    event: "AnyEvent"
    """The event being handled."""
    config: Any
    """The registration's routing config, or `None` on a direct bus."""
    payload: Any
    """The payload being handled."""
    headers: Mapping[str, object] = NO_HEADERS
    """The headers of the message being handled, read-only."""


type PublishMiddleware = Callable[[PublishCall, Next[PublishCall, Any]], Any]
"""A publish middleware: given the call and `next`, the rest of the chain."""

type HandleMiddleware = Callable[[HandleCall, Next[HandleCall, Any]], Any]
"""A handle middleware: given the message being handled and `next`, the rest of the chain."""


@dataclass(frozen=True, slots=True)
class Middleware:
    """
    The middleware a bus runs: `publish` around each emit, `handle` around each handler.

    A broker adapter runs one half: a publisher its `publish` middleware, a consumer its
    `handle` middleware. So one `Middleware` can be shared by both.
    """

    publish: tuple[PublishMiddleware, ...] = ()
    """Run around each emit, outermost first."""
    handle: tuple[HandleMiddleware, ...] = ()
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
        self, bus: str, kind: "Kind", /, *, publish: bool = True, handle: bool = True
    ) -> None:
        """
        Refuse any middleware in the selected halves of the wrong kind for the bus.

        :param bus: The bus's name, for the error message.
        :param kind: The bus's {py:class}`Kind <stratae.events.kind.Kind>`: one that awaits
            takes only an `async def`, and one that doesn't only a sync function.
        :param publish: Whether to check the `publish` half; a consumer doesn't run it.
        :param handle: Whether to check the `handle` half; a publisher doesn't run it.
        :raises MiddlewareKindError: If an async bus is given a middleware that isn't an
            `async def`, or a sync bus one that isn't a sync function returning a value.
        """
        selected: tuple[Callable[..., Any], ...] = (
            *(self.publish if publish else ()),
            *(self.handle if handle else ()),
        )
        refuse_kind(selected, bus, kind.awaits)
