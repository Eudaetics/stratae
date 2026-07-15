"""Event system: event definitions, bound-event facades, and dispatch protocols."""

from stratae.events.bound import AsyncBoundEvent, BoundEvent, abind, bind
from stratae.events.envelope import Envelope
from stratae.events.event import (
    AsyncEventConfig,
    EventConfig,
    EventType,
    PubSub,
    Request,
    event,
    is_request,
    reply_type,
)
from stratae.events.exceptions import (
    EventDispatchError,
    MultipleRespondersError,
    NoResponderError,
)
from stratae.events.handler import Handler
from stratae.events.protocols import Consumer, EmitCallable, Producer

__all__ = [
    "AsyncBoundEvent",
    "AsyncEventConfig",
    "BoundEvent",
    "Consumer",
    "EmitCallable",
    "Envelope",
    "EventConfig",
    "EventDispatchError",
    "EventType",
    "Handler",
    "MultipleRespondersError",
    "NoResponderError",
    "Producer",
    "PubSub",
    "Request",
    "abind",
    "bind",
    "event",
    "is_request",
    "reply_type",
]
