"""
Event system combining event definitions, bound-event facades, buses, and dispatch protocols.

An event pairs a payload schema with a dispatch pattern, one class per pattern:
{py:class}`PubSub <stratae.events.patterns.PubSub>` or
{py:class}`Request <stratae.events.patterns.Request>`. Each implements
{py:class}`EventType <stratae.events.protocols.EventType>`. Events are data: each bus decides
which patterns it delivers.
{py:func}`bind <stratae.events.binding.bind>` and
{py:func}`abind <stratae.events.binding.abind>` turn an event into a callable that emits it.
{py:class}`DirectBus <stratae.events.direct.DirectBus>` and
{py:class}`AsyncDirectBus <stratae.events.direct.AsyncDirectBus>` dispatch in-process;
other adapters implement the {py:class}`Producer <stratae.events.protocols.Producer>` and
{py:class}`Consumer <stratae.events.protocols.Consumer>` protocols.
A direct bus delivers each pattern with a function from
{py:mod}`dispatch <stratae.events.dispatch>`, or one of your own.
{py:class}`Envelope <stratae.events.envelope.Envelope>` carries correlation ids across a
chain of emissions. {py:class}`Hooks <stratae.events.hooks.Hooks>` observe one event where it
meets a bus, typed by that event; {py:class}`Middleware <stratae.events.middleware.Middleware>`
wraps every emit and handler call on a bus.
{py:mod}`failure <stratae.events.failure>` decides what a consumer does with a message whose
handling failed.

````{example} Create event triggering a logging event
```{code-block} python
from stratae.events import DirectBus, Envelope, EnvelopeConfig, PubSub, Request

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

class CreateUserSchema:
    def __init__(self, username: str) -> None:
        self.username = username

class User:
    def __init__(self, username: str) -> None:
        self.username = username

log_message_event = PubSub(LogMessage)
create_user_event = Request(CreateUserSchema, reply=User)

bus = DirectBus(envelope=EnvelopeConfig())
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
from .direct import AsyncDirectBus, DirectBus, Dispatch
from .dispatch import (
    dispatch_request,
    dispatch_request_async,
    fan_out,
    fan_out_async,
    fan_out_concurrent,
    only_responder,
)
from .envelope import (
    CAUSATION_ID_HEADER,
    CORRELATION_ID_HEADER,
    MESSAGE_ID_HEADER,
    TIMESTAMP_HEADER,
    Envelope,
    EnvelopeConfig,
)
from .handler import Handler, Handlers, WithoutPayload
from .hooks import Hooks
from .middleware import HandleCall, MessageHeaders, Middleware, PublishCall
from .patterns import Immutable, NoPayload, PubSub, Request
from .protocols import (
    Addressed,
    AnyEvent,
    Consumer,
    Deserializer,
    Destination,
    EmitCallable,
    Encoded,
    EventType,
    PayloadEvent,
    Producer,
    PubSubProducer,
    Routed,
    Serializer,
    Serializing,
    SignalEvent,
)

__all__ = [
    "CAUSATION_ID_HEADER",
    "CORRELATION_ID_HEADER",
    "MESSAGE_ID_HEADER",
    "TIMESTAMP_HEADER",
    "Addressed",
    "AnyEvent",
    "AsyncDirectBus",
    "Consumer",
    "Deserializer",
    "Destination",
    "DirectBus",
    "Dispatch",
    "EmitCallable",
    "Encoded",
    "Envelope",
    "EnvelopeConfig",
    "EventType",
    "HandleCall",
    "Handler",
    "Handlers",
    "Hooks",
    "Immutable",
    "MessageHeaders",
    "Middleware",
    "NoPayload",
    "PayloadEvent",
    "Producer",
    "PubSub",
    "PubSubProducer",
    "PublishCall",
    "Request",
    "Routed",
    "Serializer",
    "Serializing",
    "SignalEvent",
    "WithoutPayload",
    "abind",
    "bind",
    "dispatch_request",
    "dispatch_request_async",
    "fan_out",
    "fan_out_async",
    "fan_out_concurrent",
    "only_responder",
]
