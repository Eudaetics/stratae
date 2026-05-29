"""Event system: event base types, bound-event facades, and transports."""

from stratae.events.event import EventMeta
from stratae.events.handler import Handler

__all__ = ["Handler", "EventMeta"]
