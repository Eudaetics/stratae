"""
Event envelope carrying correlation and causation identifiers across the call chain.

{py:class}`Envelope` pairs a message id with a correlation id shared by every
envelope descended from the same root, and a causation id pointing at
whichever message produced it. {py:func}`Envelope.child` derives the next
envelope in the same chain. {py:func}`Envelope.to_headers` and
{py:func}`Envelope.from_headers` round-trip an envelope through the
{py:data}`MESSAGE_ID_HEADER`, {py:data}`CORRELATION_ID_HEADER`,
{py:data}`CAUSATION_ID_HEADER`, and {py:data}`TIMESTAMP_HEADER` message
headers, for adapters that cross a real transport.

{py:func}`Envelope.current` reads whichever envelope is active in the running
context, or `None` if none is. {py:func}`Envelope.scope` sets the active
envelope for the duration of a block, restoring the previous one on exit.
Called with no envelope, it creates a child of the current one if there is
one, or a fresh root envelope otherwise.

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
    # Emitting from inside a handler opens a child envelope: same
    # correlation_id, with causation_id set to this envelope's message_id.
    # The payoff is bigger once a chain like this crosses a real transport,
    # but it threads through in-process dispatch the same way.
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
    Immutable record of correlation and causation ids for one message in a causal chain.

    Every envelope carries a `message_id`, a `correlation_id` shared by every
    envelope descended from the same root, and a `causation_id` pointing at
    the `message_id` of whichever envelope produced it (`None` on a root
    envelope). {py:func}`Envelope.child` derives the next envelope in the
    chain. {py:func}`Envelope.scope` tracks the currently active envelope in
    a `contextvars.ContextVar`, readable back via {py:func}`Envelope.current`.

    """

    message_id: UUID = field(default_factory=uuid4)
    correlation_id: UUID = field(default_factory=uuid4)
    causation_id: UUID | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def child(self) -> Envelope:
        """
        Derive a child envelope that continues the same correlation chain.

        :returns: A new {py:class}`Envelope` with this envelope's `correlation_id`
            and `causation_id` set to this envelope's `message_id`.

        """
        return Envelope(
            correlation_id=self.correlation_id,
            causation_id=self.message_id,
        )

    def to_headers(self) -> dict[str, str]:
        """
        Serialize the envelope as portable `x-` message headers.

        :returns: A `dict` with string values for {py:data}`MESSAGE_ID_HEADER`,
            {py:data}`CORRELATION_ID_HEADER`, and {py:data}`TIMESTAMP_HEADER`,
            plus {py:data}`CAUSATION_ID_HEADER` if `causation_id` is set.

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
        Rebuild an envelope from message headers.

        Fields absent from `headers` are minted fresh. A partial set, for
        example a foreign message carrying only a message id, keeps what it
        declares and defaults the rest.

        :param headers: Header mapping to read, keyed by {py:data}`MESSAGE_ID_HEADER`,
            {py:data}`CORRELATION_ID_HEADER`, {py:data}`CAUSATION_ID_HEADER`, and
            {py:data}`TIMESTAMP_HEADER`. Missing keys fall back to their defaults.
        :returns: The reconstructed {py:class}`Envelope`.
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
        """
        Retrieve the current envelope, or `None` if no envelope is active.

        :returns: The {py:class}`Envelope` active in the calling context, or
            `None` outside any {py:func}`Envelope.scope` block.

        """
        return _current.get(None)

    @classmethod
    @contextmanager
    def scope(cls, envelope: Envelope | None = None) -> Generator[Envelope, None, None]:
        """
        Set the active envelope for the duration of the block, then restore the previous one.

        If `envelope` is omitted, inherits from the currently active envelope,
        creating a child of it via {py:func}`Envelope.child`, or creates a new
        root envelope if none is active.

        :param envelope: The envelope to make active for the block. When
            omitted, one is derived from the current context as described
            above.
        :returns: A context manager yielding the active `Envelope` for the
            duration of the block.

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
