"""
Event system combining event definitions, bound-event facades, buses, and dispatch protocols.

{py:func}`event <stratae.events.event.event>` takes a payload factory and an
{py:class}`EventType <stratae.events.event.EventType>` discriminant and returns
an {py:class}`EventConfig <stratae.events.event.EventConfig>` binding the two
(or an {py:class}`AsyncEventConfig <stratae.events.event.AsyncEventConfig>`
when the factory is async).
{py:class}`PubSub <stratae.events.event.PubSub>` marks fire-and-forget
dispatch. {py:class}`Request <stratae.events.event.Request>` marks
request/reply, where emit blocks until a responder returns.
{py:func}`is_request <stratae.events.event.is_request>` and
{py:func}`reply_type <stratae.events.event.reply_type>` inspect an
`EventConfig`'s discriminant.

{py:func}`bind <stratae.events.bound.bind>` and
{py:func}`abind <stratae.events.bound.abind>` attach an emitter and
adapter-specific routing config to an `EventConfig`. The result is a
callable facade: {py:class}`BoundEvent <stratae.events.bound.BoundEvent>`
for a sync emitter, {py:class}`AsyncBoundEvent <stratae.events.bound.AsyncBoundEvent>`
for an async one. Calling it constructs the payload and forwards it to the
emitter.

Emitters and handler registries are described structurally by the
{py:class}`Producer <stratae.events.protocols.Producer>` and
{py:class}`Consumer <stratae.events.protocols.Consumer>` protocols. A single
bound emit call is described by
{py:class}`EmitCallable <stratae.events.protocols.EmitCallable>`. Registered
callables are wrapped as {py:class}`Handler <stratae.events.handler.Handler>`
instances, which detect once whether the wrapped callable is async.

{py:class}`DirectBus <stratae.events.direct.DirectBus>` and
{py:class}`AsyncDirectBus <stratae.events.direct.AsyncDirectBus>` are
in-process bus adapters implementing both protocols. Pub/sub events fan out
to every registered handler. Request events route to a single responder.

{py:class}`Envelope <stratae.events.envelope.Envelope>` carries correlation
and causation identifiers across an emission's call chain, scoped via
{py:meth}`Envelope.scope <stratae.events.envelope.Envelope.scope>`. It
round-trips through message headers
({py:data}`CAUSATION_ID_HEADER <stratae.events.envelope.CAUSATION_ID_HEADER>`,
{py:data}`CORRELATION_ID_HEADER <stratae.events.envelope.CORRELATION_ID_HEADER>`,
{py:data}`MESSAGE_ID_HEADER <stratae.events.envelope.MESSAGE_ID_HEADER>`,
{py:data}`TIMESTAMP_HEADER <stratae.events.envelope.TIMESTAMP_HEADER>`) for
adapters that cross a real transport.

````{example} Create event triggering a logging event
```{code-block} python
from stratae.events import DirectBus, Envelope, PubSub, Request, event

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

class CreateUserSchema:
    def __init__(self, username: str) -> None:
        self.username = username

class User:
    def __init__(self, username: str) -> None:
        self.username = username

log_message = event(LogMessage, PubSub)
create_user = event(CreateUserSchema, Request[User])

bus = DirectBus(use_envelope=True)
log = bus.bind(log_message)
create = bus.bind(create_user)

@bus.handle(log_message)
def write_to_log(entry: LogMessage) -> None:
    print(f"log: {entry.text}")
    print(f"  {Envelope.current()}")

@bus.handle(create_user)
def handle_create_user(cmd: CreateUserSchema) -> User:
    print("create envelope:")
    print(f"  {Envelope.current()}")
    log(text=f"creating user {cmd.username}")
    return User(username=cmd.username)

created = create(username="ada")
print(f"created user: {created.username}")
```
```{output}
create envelope:
  Envelope(message_id=..b3e1, correlation_id=..a1c4, causation_id=None)
log: creating user ada
  Envelope(message_id=..f02d, correlation_id=..a1c4, causation_id=..b3e1)
created user: ada
```
````

See {py:class}`DirectBus <stratae.events.direct.DirectBus>`,
{py:class}`AsyncDirectBus <stratae.events.direct.AsyncDirectBus>`,
{py:func}`bind <stratae.events.bound.bind>`, and
{py:func}`abind <stratae.events.bound.abind>` for the rest of the module's API.

"""

from .bound import AsyncBoundEvent, BoundEvent, abind, bind
from .direct import AsyncDirectBus, DirectBus
from .envelope import (
    CAUSATION_ID_HEADER,
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    TIMESTAMP_HEADER,
    Envelope,
)
from .event import (
    AsyncEventConfig,
    EventConfig,
    EventType,
    PubSub,
    Request,
    event,
    is_request,
    reply_type,
)
from .handler import Handler
from .protocols import Consumer, EmitCallable, Producer

__all__ = [
    "AsyncBoundEvent",
    "AsyncDirectBus",
    "AsyncEventConfig",
    "BoundEvent",
    "CAUSATION_ID_HEADER",
    "CORRELATION_ID_HEADER",
    "Consumer",
    "DirectBus",
    "EmitCallable",
    "Envelope",
    "EventConfig",
    "EventType",
    "Handler",
    "MESSAGE_ID_HEADER",
    "Producer",
    "PubSub",
    "Request",
    "TIMESTAMP_HEADER",
    "abind",
    "bind",
    "event",
    "is_request",
    "reply_type",
]
