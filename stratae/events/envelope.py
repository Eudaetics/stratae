"""
Event envelope carrying correlation and causation identifiers across the call chain.

{py:class}`Envelope` carries a message id, a correlation id shared across its chain, and
a causation id pointing at its parent. {py:func}`Envelope.scope` sets the active envelope
for a block, creating a child of the current one by default. {py:func}`Envelope.to_headers`
and {py:func}`Envelope.from_headers` carry it across a transport.

New ids are random version 4 UUIDs by default. With the `uuid` extra installed
(`pip install stratae[uuid]`), they come from `uuid_utils.compat`, about two and a half times
faster, and are still standard `uuid.UUID`s. A component that opens envelopes takes an
{py:class}`EnvelopeConfig`, whose `new_id` can be any function returning a `UUID`, such as a
time-ordered `uuid7`.

````{example} Tracing a chained event through envelopes
```{code-block} python
from stratae.events import DirectBus, PubSub
from stratae.events.envelope import Envelope, EnvelopeConfig

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

class ShipmentScheduled:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_placed_event = PubSub(OrderPlaced)
shipment_scheduled_event = PubSub(ShipmentScheduled)

bus = DirectBus(envelope=EnvelopeConfig())
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

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Self
from uuid import UUID

MESSAGE_ID_HEADER = "x-message-id"
CORRELATION_ID_HEADER = "x-correlation-id"
CAUSATION_ID_HEADER = "x-causation-id"
TIMESTAMP_HEADER = "x-timestamp"

ENVELOPE_HEADERS = frozenset(
    (MESSAGE_ID_HEADER, CORRELATION_ID_HEADER, CAUSATION_ID_HEADER, TIMESTAMP_HEADER)
)
"""The headers an envelope is carried in, which middleware can't set."""


type _NewId = Callable[[], UUID]
"""Makes a new id for an envelope."""


def _default_new_id() -> _NewId:
    """
    Return the fastest random id maker available, which every component uses by default.

    `uuid_utils.compat.uuid4` if the `uuid` extra is installed: about two and a half times
    faster than `uuid.uuid4`, and still returning a standard `uuid.UUID`. Otherwise
    `uuid.uuid4`.
    """
    try:
        from uuid_utils.compat import uuid4
    except ImportError:
        from uuid import uuid4
    return uuid4


_new_uuid4 = _default_new_id()


def _uuid(value: object | None) -> UUID | None:
    """Parse an optional header value as a UUID."""
    return None if value is None else UUID(str(value))


def _now() -> datetime:
    """Return the current time, timezone-aware in UTC."""
    return datetime.now(UTC)


def _when(value: object | None) -> datetime:
    """Parse an optional header timestamp, defaulting to the current time."""
    return _now() if value is None else datetime.fromisoformat(str(value))


_MADE: Any = object()
"""Stands in for a field not given to `Envelope`, which makes it."""


@dataclass(frozen=True, slots=True, init=False)
class Envelope:
    """
    Immutable correlation and causation ids for one message in a causal chain.

    `causation_id` is `None` on a root envelope.
    """

    message_id: UUID = field(default_factory=_new_uuid4)
    correlation_id: UUID = field(default_factory=_new_uuid4)
    causation_id: UUID | None = None
    timestamp: datetime = field(default_factory=_now)

    def __init__(
        self,
        message_id: UUID = _MADE,
        correlation_id: UUID = _MADE,
        causation_id: UUID | None = None,
        timestamp: datetime = _MADE,
    ) -> None:
        """
        Make an envelope, making any id or timestamp not given.

        Sets each field through its slot directly. A frozen dataclass's own constructor goes
        through `object.__setattr__` for each, which costs more on this per-emit path.

        :param message_id: This message's id. A new random one if omitted.
        :param correlation_id: The chain's id. A new random one if omitted.
        :param causation_id: The id of the message that caused this one, if any.
        :param timestamp: When the message was made. The current time if omitted.
        """
        _set_message_id(self, _new_uuid4() if message_id is _MADE else message_id)
        _set_correlation_id(self, _new_uuid4() if correlation_id is _MADE else correlation_id)
        _set_causation_id(self, causation_id)
        _set_timestamp(self, _now() if timestamp is _MADE else timestamp)

    @classmethod
    def root(cls, new_id: _NewId = _new_uuid4) -> Self:
        """
        Start a new chain.

        :param new_id: Makes the message and correlation ids.
        :returns: An {py:class}`Envelope` with new ids and no `causation_id`.
        """
        # Positional, in field order: cheaper than keywords on this per-emit path.
        return cls(new_id(), new_id())

    def child(self, new_id: _NewId = _new_uuid4) -> Self:
        """
        Derive the next envelope in this chain.

        :param new_id: Makes the child's message id.
        :returns: An {py:class}`Envelope` with this `correlation_id`, caused by this message.
        """
        # Positional, in field order: cheaper than keywords on this per-emit path.
        return type(self)(new_id(), self.correlation_id, self.message_id)

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
    def from_headers(cls, headers: Mapping[str, object], new_id: _NewId = _new_uuid4) -> Self:
        """
        Rebuild an envelope from message headers, generating any missing fields.

        :param headers: The message headers.
        :param new_id: Makes a missing message or correlation id.
        :returns: The rebuilt {py:class}`Envelope`.
        :raises ValueError: If a header is present but unparseable.
        """
        return cls(
            message_id=_uuid(headers.get(MESSAGE_ID_HEADER)) or new_id(),
            correlation_id=_uuid(headers.get(CORRELATION_ID_HEADER)) or new_id(),
            causation_id=_uuid(headers.get(CAUSATION_ID_HEADER)),
            timestamp=_when(headers.get(TIMESTAMP_HEADER)),
        )

    @staticmethod
    def current() -> Envelope | None:
        """Return the active envelope, or `None` outside any {py:func}`Envelope.scope` block."""
        return _current.get(None)

    @classmethod
    def scope(
        cls, envelope: Envelope | None = None, *, new_id: _NewId = _new_uuid4
    ) -> AbstractContextManager[Envelope]:
        """
        Set the active envelope for a block, restoring the previous one on exit.

        :param envelope: The envelope to activate. Defaults to a child of the envelope current
            when the block is entered, or a new root.
        :param new_id: Makes the new envelope's ids when `envelope` isn't given.
        :returns: A context manager yielding the active {py:class}`Envelope`.
        """
        return _Scope(cls, envelope, new_id)


_set_message_id = Envelope.__dict__["message_id"].__set__
_set_correlation_id = Envelope.__dict__["correlation_id"].__set__
_set_causation_id = Envelope.__dict__["causation_id"].__set__
_set_timestamp = Envelope.__dict__["timestamp"].__set__

_current: ContextVar[Envelope] = ContextVar("_current_envelope")


@dataclass(frozen=True, slots=True)
class EnvelopeConfig:
    """
    How a component makes the envelopes it opens: a bus, the outbox, or an adapter.

    Pass `EnvelopeConfig()` for random ids. Pass a function making ids as `new_id` for another
    kind, such as time-ordered ids, which sort in the order they were made and so suit
    anything stored or sorted by message id, such as outbox records. `uuid_utils.compat.uuid7`
    makes them, as does `uuid.uuid7` on Python 3.14 and later.

    ````{example} Time-ordered ids for one bus, random ids for another
    ```{code-block} python
    from uuid_utils.compat import uuid7
    from stratae.events import DirectBus, Envelope, EnvelopeConfig, PubSub

    class OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    order_placed = PubSub(OrderPlaced)
    orders = DirectBus(envelope=EnvelopeConfig(new_id=uuid7))
    audit = DirectBus(envelope=EnvelopeConfig())

    @orders.handle(order_placed)
    def on_order(order: OrderPlaced) -> None:
        print(f"orders: version {Envelope.current().message_id.version}")

    @audit.handle(order_placed)
    def on_audit(order: OrderPlaced) -> None:
        print(f"audit: version {Envelope.current().message_id.version}")

    orders.emit(order_placed, OrderPlaced(1))
    audit.emit(order_placed, OrderPlaced(1))
    ```
    ```{output}
    orders: version 7
    audit: version 4
    ```
    ````
    """

    new_id: _NewId = _new_uuid4
    """Makes the ids of each new envelope. Random by default."""


class _Scope:
    """
    The context manager {py:func}`Envelope.scope` returns.

    A class rather than a `contextlib.contextmanager` generator: each emit, outbox write and
    request opens one, and a generator costs about twice as much to enter and exit.
    """

    __slots__ = ("_envelope", "_kind", "_new_id", "_token")

    def __init__(self, kind: type[Envelope], envelope: Envelope | None, new_id: _NewId) -> None:
        self._kind = kind
        self._envelope = envelope
        self._new_id = new_id
        self._token: Token[Envelope]

    def __enter__(self) -> Envelope:
        envelope = self._envelope
        if envelope is None:
            new_id, parent = self._new_id, _current.get(None)
            # A root built here, positionally, saves a call to `root` on this per-emit path.
            envelope = parent.child(new_id) if parent else self._kind(new_id(), new_id())
        self._token = _current.set(envelope)
        return envelope

    def __exit__(self, *_: object) -> None:
        _current.reset(self._token)
