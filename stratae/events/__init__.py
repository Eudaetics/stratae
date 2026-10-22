"""
Event system combining event definitions, bound-event facades, buses, and dispatch protocols.

An {py:class}`Event <stratae.events.event.Event>` pairs a payload schema with a dispatch
pattern: {py:class}`PubSub <stratae.events.event.PubSub>` or
{py:class}`Request <stratae.events.event.Request>`.
{py:func}`bind <stratae.events.binding.bind>` and
{py:func}`abind <stratae.events.binding.abind>` turn an event into a callable that emits it.
{py:class}`DirectBus <stratae.events.direct.DirectBus>` and
{py:class}`AsyncDirectBus <stratae.events.direct.AsyncDirectBus>` dispatch in-process;
other adapters implement the {py:class}`Producer <stratae.events.protocols.Producer>` and
{py:class}`Consumer <stratae.events.protocols.Consumer>` protocols.
{py:class}`Envelope <stratae.events.envelope.Envelope>` carries correlation ids across a
chain of emissions.

````{example} Create event triggering a logging event
```{code-block} python
from stratae.events import DirectBus, Envelope, Event, PubSub, Request

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

class CreateUserSchema:
    def __init__(self, username: str) -> None:
        self.username = username

class User:
    def __init__(self, username: str) -> None:
        self.username = username

log_message_event = Event(PubSub, LogMessage)
create_user_event = Event(Request[User], CreateUserSchema)

bus = DirectBus(use_envelope=True)
log = bus.bind(log_message_event, factory=LogMessage)
create = bus.bind(create_user_event, factory=CreateUserSchema)

@bus.handle(log_message_event)
def write_to_log(entry: LogMessage) -> None:
    print(f"log: {entry.text}")
    print(f"  {Envelope.current()}")

@bus.handle(create_user_event)
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
"""

from .binding import abind, bind
from .direct import AsyncDirectBus, DirectBus
from .envelope import (
    CAUSATION_ID_HEADER,
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    TIMESTAMP_HEADER,
    Envelope,
)
from .event import (
    DispatchPattern,
    Event,
    NoPayload,
    PubSub,
    Request,
    is_request,
    reply_type,
)
from .handler import Handler
from .protocols import Consumer, EmitCallable, Producer

__all__ = [
    "AsyncDirectBus",
    "CAUSATION_ID_HEADER",
    "CORRELATION_ID_HEADER",
    "Consumer",
    "DirectBus",
    "DispatchPattern",
    "EmitCallable",
    "Envelope",
    "Event",
    "Handler",
    "MESSAGE_ID_HEADER",
    "NoPayload",
    "Producer",
    "PubSub",
    "Request",
    "TIMESTAMP_HEADER",
    "abind",
    "bind",
    "is_request",
    "reply_type",
]
