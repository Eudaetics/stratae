"""
Event envelope carrying correlation and causation identifiers across the call chain.

{py:class}`Envelope` carries a message id, a correlation id shared across its chain, and
a causation id pointing at its parent. An {py:class}`EnvelopeFactory` makes envelopes: a new
chain, the next envelope in one, and one carried in headers. Its `scope` sets the active
envelope for a block, creating a child of the current one by default.
{py:class}`EnvelopeHeaders` names the headers an envelope travels in across a transport, and
writes it to them.

New ids are random version 4 UUIDs by default. With the `uuid` extra installed
(`pip install stratae[uuid]`), they come from `uuid_utils.compat`, about two and a half times
faster, and are still standard `uuid.UUID`s. A component that opens envelopes takes an
{py:class}`EnvelopeFactory` as `envelope_factory`, whose `new_id` can be any function
returning a `UUID`, such as a time-ordered `uuid7`.

````{example} Tracing a chained event through envelopes
```{code-block} python
from stratae.events import DirectBus, PubSub
from stratae.events.envelope import Envelope, EnvelopeFactory

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

class ShipmentScheduled:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_placed_event = PubSub(OrderPlaced)
shipment_scheduled_event = PubSub(ShipmentScheduled)

bus = DirectBus(envelope_factory=EnvelopeFactory())
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


_NO_SUBCLASSES = (
    "Envelope can't be subclassed; carry more with a message in its headers, set by middleware"
)


@final
@dataclass(frozen=True, slots=True, init=False)
class Envelope:
    """
    Immutable correlation and causation ids for one message in a causal chain.

    A value of the ids and time it's given: an {py:class}`EnvelopeFactory` makes new ones.
    `causation_id` is `None` on a root envelope. Its four fields are fixed, and it can't be
    subclassed. Anything else a message carries, such as a tenant or a trace, travels in its
    headers, which publish middleware can add to and handle middleware reads.
    """

    message_id: UUID
    correlation_id: UUID
    timestamp: datetime
    causation_id: UUID | None = None

    def __init__(
        self,
        message_id: UUID,
        correlation_id: UUID,
        timestamp: datetime,
        causation_id: UUID | None = None,
    ) -> None:
        """
        Make an envelope from its ids and time.

        Sets each field through its slot directly. A frozen dataclass's own constructor goes
        through `object.__setattr__` for each, which costs more on this per-emit path.

        :param message_id: This message's id.
        :param correlation_id: The chain's id.
        :param timestamp: When the message was made.
        :param causation_id: The id of the message that caused this one, or `None` for a root.
        """
        _set_message_id(self, message_id)
        _set_correlation_id(self, correlation_id)
        _set_timestamp(self, timestamp)
        _set_causation_id(self, causation_id)

    def __init_subclass__(cls, **kwargs: object) -> None:
        """
        Refuse a subclass when it's defined.

        :raises TypeError: Always.
        """
        raise TypeError(_NO_SUBCLASSES)

    @staticmethod
    def current() -> Envelope | None:
        """
        Return the active envelope.

        :returns: The envelope of the innermost {py:meth}`EnvelopeFactory.scope` block, or
            `None` outside any.
        """
        return _current.get(None)


@dataclass(frozen=True, slots=True)
class EnvelopeHeaders:
    """
    The names of the headers an envelope travels in, and how it's written to them.

    An immutable value: values with the same names are equal. The default names are
    `x-message-id`, `x-correlation-id`, `x-causation-id` and `x-timestamp`. A transport writes an
    envelope to headers with `write`, and a receiver rebuilds it with
    {py:meth}`EnvelopeFactory.read`. Each value is text: the ids as UUID strings, and the
    timestamp in ISO 8601.
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


_set_message_id = Envelope.__dict__["message_id"].__set__
_set_correlation_id = Envelope.__dict__["correlation_id"].__set__
_set_causation_id = Envelope.__dict__["causation_id"].__set__
_set_timestamp = Envelope.__dict__["timestamp"].__set__

_current: ContextVar[Envelope] = ContextVar("_current_envelope")


@dataclass(frozen=True, slots=True)
class EnvelopeFactory:
    """
    Makes envelopes, and names the headers they travel in.

    A bus, the outbox, an adapter or the ASGI middleware takes one as `envelope_factory`, as all
    its envelope setup, and makes every envelope it opens or reads through it: a new chain with
    `root`, the next envelope in one with `child`, the active one for a block with `scope`, and
    one carried in headers with `read`.

    Pass `EnvelopeFactory()` for random ids and the default header names. Pass a function making
    ids as `new_id` for another kind, such as time-ordered ids, which sort in the order they
    were made and so suit anything stored or sorted by message id, such as outbox records.
    `uuid_utils.compat.uuid7` makes them, as does `uuid.uuid7` on Python 3.14 and later.

    Pass a function returning a timezone-aware time as `now` to stamp envelopes from another
    clock, such as a fixed one in a test. Each envelope's timestamp comes from it, and its
    default is the current time in UTC.

    Pass {py:class}`EnvelopeHeaders` with other names as `headers` when a tool the service
    works with already uses one of the defaults. Every component of a service, and every
    service a message passes between, needs the same names: a bus refuses its own names as
    headers, writes the envelope under them, and reads it back from them.

    ````{example} Time-ordered ids for one bus, random ids for another
    ```{code-block} python
    from uuid_utils.compat import uuid7
    from stratae.events import DirectBus, Envelope, EnvelopeFactory, PubSub

    class OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    order_placed = PubSub(OrderPlaced)
    orders = DirectBus(envelope_factory=EnvelopeFactory(new_id=uuid7))
    audit = DirectBus(envelope_factory=EnvelopeFactory())

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
    now: Callable[[], datetime] = _now
    """Returns the timestamp of each new envelope. The current time in UTC by default."""

    def root(self, correlation_id: UUID | None = None) -> Envelope:
        """
        Start a new chain, or a new root in the chain whose correlation id is given.

        :param correlation_id: The chain to join, such as one a request brought, or `None` for
            a new chain.
        :returns: An {py:class}`Envelope` with a new message id and no `causation_id`.
        """
        new_id = self.new_id
        return Envelope(
            new_id(), new_id() if correlation_id is None else correlation_id, self.now()
        )

    def child(self, parent: Envelope) -> Envelope:
        """
        Make the next envelope in a parent's chain.

        :param parent: The envelope of the message that caused this one.
        :returns: An {py:class}`Envelope` with a new message id and the parent's
            `correlation_id`, caused by the parent.
        """
        return Envelope(self.new_id(), parent.correlation_id, self.now(), parent.message_id)

    def scope(self, envelope: Envelope | None = None) -> AbstractContextManager[Envelope]:
        """
        Set the active envelope for a block, restoring the previous one on exit.

        :param envelope: The envelope to activate. Defaults to a child of the envelope current
            when the block is entered, or a new root.
        :returns: A context manager yielding the active {py:class}`Envelope`.
        """
        return _Scope(envelope, self)

    def read(self, headers: Mapping[str, object]) -> Envelope:
        """
        Rebuild an envelope from headers named as `headers` names them, making what's missing.

        A header that's absent, or `None`, is missing. A missing message or correlation id is
        made with `new_id`, and a missing timestamp with `now`.

        :param headers: The headers to read.
        :returns: The rebuilt {py:class}`Envelope`.
        :raises ValueError: If a header is present but unparseable.
        """
        names, new_id = self.headers, self.new_id
        timestamp = headers.get(names.timestamp)
        return Envelope(
            _uuid(headers.get(names.message_id)) or new_id(),
            _uuid(headers.get(names.correlation_id)) or new_id(),
            self.now() if timestamp is None else datetime.fromisoformat(str(timestamp)),
            _uuid(headers.get(names.causation_id)),
        )


class _Scope:
    """
    The context manager {py:meth}`EnvelopeFactory.scope` returns.

    A class rather than a `contextlib.contextmanager` generator: each emit, outbox write and
    request opens one, and a generator costs about twice as much to enter and exit.
    """

    __slots__ = ("_envelope", "_factory", "_token")

    def __init__(self, envelope: Envelope | None, factory: EnvelopeFactory) -> None:
        self._envelope = envelope
        self._factory = factory
        self._token: Token[Envelope]

    def __enter__(self) -> Envelope:
        envelope = self._envelope
        if envelope is None:
            factory, parent = self._factory, _current.get(None)
            new_id = factory.new_id
            # Built here, positionally, to save a call to `child` or `root` on this per-emit path.
            envelope = (
                Envelope(new_id(), parent.correlation_id, factory.now(), parent.message_id)
                if parent
                else Envelope(new_id(), new_id(), factory.now())
            )
        self._token = _current.set(envelope)
        return envelope

    def __exit__(self, *_: object) -> None:
        _current.reset(self._token)
