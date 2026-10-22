"""
Event envelope carrying correlation and causation identifiers across the call chain.

{py:class}`Envelope` carries a message id, a correlation id shared across its chain, and
a causation id pointing at its parent. {py:func}`Envelope.scope` sets the active envelope
for a block, creating a child of the current one by default. {py:func}`Envelope.to_headers`
and {py:func}`Envelope.from_headers` carry it across a transport.

````{example} Tracing a chained event through envelopes
```{code-block} python
from stratae.events import DirectBus, Event, PubSub
from stratae.events.envelope import Envelope

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

class ShipmentScheduled:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_placed_event = Event(PubSub, OrderPlaced)
shipment_scheduled_event = Event(PubSub, ShipmentScheduled)

bus = DirectBus(use_envelope=True)
place_order = bus.bind(order_placed_event, factory=OrderPlaced)
schedule_shipment = bus.bind(shipment_scheduled_event, factory=ShipmentScheduled)

@bus.handle(order_placed_event)
def on_order_placed(order: OrderPlaced) -> None:
    print(f"order placed:       {Envelope.current()}")
    # Opens a child envelope, caused by this one.
    schedule_shipment(order_id=order.order_id)

@bus.handle(shipment_scheduled_event)
def on_shipment_scheduled(shipment: ShipmentScheduled) -> None:
    print(f"shipment scheduled: {Envelope.current()}")

place_order(order_id=42)
```
```{output}
order placed:       Envelope(message_id=..f9fa, correlation_id=..740c, causation_id=None)
shipment scheduled: Envelope(message_id=..7d86, correlation_id=..740c, causation_id=..f9fa)
```
````

"""

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
    """
    Immutable correlation and causation ids for one message in a causal chain.

    `causation_id` is `None` on a root envelope.
    """

    message_id: UUID = field(default_factory=uuid4)
    correlation_id: UUID = field(default_factory=uuid4)
    causation_id: UUID | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def child(self) -> Envelope:
        """
        Derive the next envelope in this chain.

        :returns: An {py:class}`Envelope` with this `correlation_id`, caused by this message.
        """
        return Envelope(
            correlation_id=self.correlation_id,
            causation_id=self.message_id,
        )

    def to_headers(self) -> dict[str, str]:
        """
        Serialize the envelope as `x-` message headers.

        :returns: A `dict` of string header values.
        """
        headers = {
            MESSAGE_ID_HEADER: str(self.message_id),
            CORRELATION_ID_HEADER: str(self.correlation_id),
            TIMESTAMP_HEADER: self.timestamp.isoformat(),
        }
        if self.causation_id is not None:
            headers[CAUSATION_ID_HEADER] = str(self.causation_id)
        return headers

    @classmethod
    def from_headers(cls, headers: Mapping[str, object]) -> Envelope:
        """
        Rebuild an envelope from message headers, generating any missing fields.

        :param headers: The message headers.
        :returns: The rebuilt {py:class}`Envelope`.
        :raises ValueError: If a header is present but unparseable.
        """
        return cls(
            message_id=_uuid(headers.get(MESSAGE_ID_HEADER)) or uuid4(),
            correlation_id=_uuid(headers.get(CORRELATION_ID_HEADER)) or uuid4(),
            causation_id=_uuid(headers.get(CAUSATION_ID_HEADER)),
            timestamp=_when(headers.get(TIMESTAMP_HEADER)),
        )

    @staticmethod
    def current() -> Envelope | None:
        """Return the active envelope, or `None` outside any {py:func}`Envelope.scope` block."""
        return _current.get(None)

    @classmethod
    @contextmanager
    def scope(cls, envelope: Envelope | None = None) -> Generator[Envelope, None, None]:
        """
        Set the active envelope for a block, restoring the previous one on exit.

        :param envelope: The envelope to activate. Defaults to a child of the current
            envelope, or a new root.
        :returns: A context manager yielding the active {py:class}`Envelope`.
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
