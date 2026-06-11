"""Event system: event base types, bound-event facades, and transports."""

from stratae.events.handler import Handler
from stratae.events.protocols import Consumer, Producer

__all__ = ["Consumer", "Handler", "Producer"]
