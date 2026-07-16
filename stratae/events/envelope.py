"""Event envelope carrying correlation and causation identifiers across the call chain."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Generator, Mapping
from uuid import UUID, uuid4

MESSAGE_ID_HEADER = "x-message-id"
CORRELATION_ID_HEADER = "x-correlation-id"
CAUSATION_ID_HEADER = "x-causation-id"
TIMESTAMP_HEADER = "x-timestamp"


def _uuid(value: object | None) -> UUID | None:
    """Parse an optional header value as a UUID."""
    return None if value is None else UUID(str(value))


def _when(value: object | None) -> datetime:
    """Parse an optional header timestamp, defaulting to the current time."""
    return datetime.now(timezone.utc) if value is None else datetime.fromisoformat(str(value))


@dataclass(frozen=True)
class Envelope:
    """Message envelope for tracking events."""

    message_id: UUID = field(default_factory=uuid4)
    correlation_id: UUID = field(default_factory=uuid4)
    causation_id: UUID | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def child(self) -> Envelope:
        """Create a child envelope with the same correlation id and this as the parent."""
        return Envelope(
            correlation_id=self.correlation_id,
            causation_id=self.message_id,
        )

    def to_headers(self) -> dict[str, str]:
        """Serialize the envelope as portable ``x-`` message headers."""
        headers = {
            MESSAGE_ID_HEADER: str(self.message_id),
            CORRELATION_ID_HEADER: str(self.correlation_id),
            TIMESTAMP_HEADER: self.timestamp.isoformat(),
        }
        if self.causation_id is not None:
            headers[CAUSATION_ID_HEADER] = str(self.causation_id)
        return headers

    @classmethod
    def from_headers(cls, headers: Mapping[str, object]) -> Envelope | None:
        """
        Rebuild an envelope from message headers.

        Returns ``None`` when the identifying headers are absent, so
        transports can fall back to a fresh envelope for untraced
        messages.

        Raises:
            ValueError: When identifying headers are present but
                unparseable — corruption worth surfacing, unlike absence.

        """
        if headers.get(MESSAGE_ID_HEADER) is None or headers.get(CORRELATION_ID_HEADER) is None:
            return None
        return cls(
            message_id=UUID(str(headers[MESSAGE_ID_HEADER])),
            correlation_id=UUID(str(headers[CORRELATION_ID_HEADER])),
            causation_id=_uuid(headers.get(CAUSATION_ID_HEADER)),
            timestamp=_when(headers.get(TIMESTAMP_HEADER)),
        )

    @staticmethod
    def current() -> Envelope | None:
        """Retrieve the current envelope, or ``None`` if no envelope is active."""
        return _current.get(None)

    @classmethod
    @contextmanager
    def scope(cls, envelope: Envelope | None = None) -> Generator[Envelope, None, None]:
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


_current: ContextVar[Envelope] = ContextVar("_current_envelope")
