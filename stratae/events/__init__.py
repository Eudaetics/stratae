"""Event system: event definitions, bound-event facades, and dispatch protocols."""

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
