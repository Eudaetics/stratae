"""Event envelope carrying correlation and causation identifiers across the call chain."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Generator
from uuid import UUID, uuid4


@dataclass(frozen=True)
class EventEnvelope:
    """Message envelope for tracking events."""

    message_id: UUID = field(default_factory=uuid4)
    correlation_id: UUID = field(default_factory=uuid4)
    causation_id: UUID | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def child(self) -> EventEnvelope:
        """Create a child envelope with the same correlation id and this as the parent."""
        return EventEnvelope(
            correlation_id=self.correlation_id,
            causation_id=self.message_id,
        )

    @staticmethod
    def current() -> EventEnvelope | None:
        """Retrieve the current envelope, or ``None`` if no envelope is active."""
        return _current.get(None)

    @classmethod
    @contextmanager
    def scope(cls, envelope: EventEnvelope | None = None) -> Generator[EventEnvelope, None, None]:
        """
        Set the active envelope for the duration of the block, then restore the previous one.

        If no envelope is provided, inherits from the current context (creating a child) or
        creates a new root envelope if there is no current context.
        """
        if envelope is None:
            parent = _current.get(None)
            envelope = parent.child() if parent else cls()
        token = _current.set(envelope)
        try:
            yield envelope
        finally:
            _current.reset(token)


_current: ContextVar[EventEnvelope] = ContextVar("_current_envelope")
