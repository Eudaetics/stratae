"""
Event envelope carrying correlation and causation identifiers across the call chain.

{py:class}`Envelope` carries a message id, a correlation id shared across its chain, and
a causation id pointing at its parent. {py:func}`Envelope.scope` sets the active envelope
for a block, creating a child of the current one by default. {py:class}`EnvelopeHeaders`
names the headers it travels in across a transport, and writes it to them and reads it back.

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
from typing import final
from uuid import UUID

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


_NO_SUBCLASSES = (
    "Envelope can't be subclassed; carry more with a message in its headers, set by middleware"
)


@final
@dataclass(frozen=True, slots=True, init=False)
class Envelope:
    """
    Immutable correlation and causation ids for one message in a causal chain.

    `causation_id` is `None` on a root envelope. Its four fields are fixed, and it can't be
    subclassed. Anything else a message carries, such as a tenant or a trace, travels in its
    headers, which publish middleware can add to and handle middleware reads.
    """

    message_id: UUID = field(default_factory=_new_uuid4)
    correlation_id: UUID = field(default_factory=_new_uuid4)
    causation_id: UUID | None = None
    timestamp: datetime = field(default_factory=_now)

    def __init__(
        self,
        message_id: UUID | None = None,
        correlation_id: UUID | None = None,
        causation_id: UUID | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        """
        Make an envelope, making any id or timestamp not given.

        Sets each field through its slot directly. A frozen dataclass's own constructor goes
        through `object.__setattr__` for each, which costs more on this per-emit path.

        :param message_id: This message's id. A new random one if `None`.
        :param correlation_id: The chain's id. A new random one if `None`.
        :param causation_id: The id of the message that caused this one, if any.
        :param timestamp: When the message was made. The current time if `None`.
        """
        _set_message_id(self, _new_uuid4() if message_id is None else message_id)
        _set_correlation_id(self, _new_uuid4() if correlation_id is None else correlation_id)
        _set_causation_id(self, causation_id)
        _set_timestamp(self, _now() if timestamp is None else timestamp)

    def __init_subclass__(cls, **kwargs: object) -> None:
        """
        Refuse a subclass when it's defined.

        :raises TypeError: Always.
        """
        raise TypeError(_NO_SUBCLASSES)

    @staticmethod
    def root(new_id: _NewId = _new_uuid4) -> Envelope:
        """
        Start a new chain.

        :param new_id: Makes the message and correlation ids.
        :returns: An {py:class}`Envelope` with new ids and no `causation_id`.
        """
        # Positional, in field order: cheaper than keywords on this per-emit path.
        return Envelope(new_id(), new_id())

    def child(self, new_id: _NewId = _new_uuid4) -> Envelope:
        """
        Derive the next envelope in this chain.

        :param new_id: Makes the child's message id.
        :returns: An {py:class}`Envelope` with this `correlation_id`, caused by this message.
        """
        # Positional, in field order: cheaper than keywords on this per-emit path.
        return Envelope(new_id(), self.correlation_id, self.message_id)

    @staticmethod
    def current() -> Envelope | None:
        """Return the active envelope, or `None` outside any {py:func}`Envelope.scope` block."""
        return _current.get(None)

    @staticmethod
    def scope(
        envelope: Envelope | None = None, *, new_id: _NewId = _new_uuid4
    ) -> AbstractContextManager[Envelope]:
        """
        Set the active envelope for a block, restoring the previous one on exit.

        :param envelope: The envelope to activate. Defaults to a child of the envelope current
            when the block is entered, or a new root.
        :param new_id: Makes the new envelope's ids when `envelope` isn't given.
        :returns: A context manager yielding the active {py:class}`Envelope`.
        """
        return _Scope(envelope, new_id)


@dataclass(frozen=True, slots=True)
class EnvelopeHeaders:
    """
    The names of the headers an envelope travels in, and how it's written to and read from them.

    An immutable value: values with the same names are equal. The default names are
    `x-message-id`, `x-correlation-id`, `x-causation-id` and `x-timestamp`. A transport writes an
    envelope to headers with `write`, and a receiver rebuilds it with `read`. Each value is text:
    the ids as UUID strings, and the timestamp in ISO 8601.
    """

    message_id: str = "x-message-id"
    """The header carrying the message id."""
    correlation_id: str = "x-correlation-id"
    """The header carrying the correlation id, shared across a chain."""
    causation_id: str = "x-causation-id"
    """The header carrying the causation id, left out of a root envelope's headers."""
    timestamp: str = "x-timestamp"
    """The header carrying when the envelope was made."""
    names: frozenset[str] = field(init=False, repr=False, compare=False)
    """The four header names, worked out once."""

    def __post_init__(self) -> None:
        """Work out the names once, since the value can't change."""
        names = frozenset((self.message_id, self.correlation_id, self.causation_id, self.timestamp))
        object.__setattr__(self, "names", names)

    def write(self, envelope: Envelope) -> dict[str, str]:
        """
        Return an envelope's headers, leaving out the causation id of a root envelope.

        :param envelope: The envelope to write.
        :returns: A `dict` of header names to text values.
        """
        headers = {
            self.message_id: str(envelope.message_id),
            self.correlation_id: str(envelope.correlation_id),
            self.timestamp: envelope.timestamp.isoformat(),
        }
        if envelope.causation_id is not None:
            headers[self.causation_id] = str(envelope.causation_id)
        return headers

    def read(self, headers: Mapping[str, object], new_id: _NewId = _new_uuid4) -> Envelope:
        """
        Rebuild an envelope from headers, making any id that's missing.

        A header that's absent, or `None`, is missing. A missing timestamp is the current time.

        :param headers: The headers to read.
        :param new_id: Makes a missing message or correlation id.
        :returns: The rebuilt {py:class}`Envelope`.
        :raises ValueError: If a header is present but unparseable.
        """
        return Envelope(
            _uuid(headers.get(self.message_id)) or new_id(),
            _uuid(headers.get(self.correlation_id)) or new_id(),
            _uuid(headers.get(self.causation_id)),
            _when(headers.get(self.timestamp)),
        )


_set_message_id = Envelope.__dict__["message_id"].__set__
_set_correlation_id = Envelope.__dict__["correlation_id"].__set__
_set_causation_id = Envelope.__dict__["causation_id"].__set__
_set_timestamp = Envelope.__dict__["timestamp"].__set__

_current: ContextVar[Envelope] = ContextVar("_current_envelope")


@dataclass(frozen=True, slots=True)
class EnvelopeConfig:
    """
    How a component makes the envelopes it opens, and the headers they travel in.

    A bus, the outbox, an adapter or the ASGI middleware takes one, as all its envelope setup.
    Pass `EnvelopeConfig()` for random ids and the default header names. Pass a function making
    ids as `new_id` for another kind, such as time-ordered ids, which sort in the order they
    were made and so suit anything stored or sorted by message id, such as outbox records.
    `uuid_utils.compat.uuid7` makes them, as does `uuid.uuid7` on Python 3.14 and later.

    Pass {py:class}`EnvelopeHeaders` with other names as `headers` when a tool the service
    works with already uses one of the defaults. Every component of a service, and every
    service a message passes between, needs the same names: a bus refuses its own names as
    headers, writes the envelope under them, and reads it back from them.

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
    headers: EnvelopeHeaders = EnvelopeHeaders()
    """The names of the headers the envelope travels in. The default names by default."""


class _Scope:
    """
    The context manager {py:func}`Envelope.scope` returns.

    A class rather than a `contextlib.contextmanager` generator: each emit, outbox write and
    request opens one, and a generator costs about twice as much to enter and exit.
    """

    __slots__ = ("_envelope", "_new_id", "_token")

    def __init__(self, envelope: Envelope | None, new_id: _NewId) -> None:
        self._envelope = envelope
        self._new_id = new_id
        self._token: Token[Envelope]

    def __enter__(self) -> Envelope:
        envelope = self._envelope
        if envelope is None:
            new_id, parent = self._new_id, _current.get(None)
            # A root built here, positionally, saves a call to `root` on this per-emit path.
            envelope = parent.child(new_id) if parent else Envelope(new_id(), new_id())
        self._token = _current.set(envelope)
        return envelope

    def __exit__(self, *_: object) -> None:
        _current.reset(self._token)
