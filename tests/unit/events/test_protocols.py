"""
Unit tests for the Producer and Consumer protocols.

This test suite verifies the following behaviors:

Producer:
- A sync implementation satisfies the Producer protocol.
- An async implementation satisfies the Producer protocol.

PubSubProducer:
- Every Producer, such as the direct buses, also satisfies PubSubProducer.

AsyncPubSubProducer:
- AsyncDirectBus satisfies it, and DirectBus, whose emit isn't awaited, is a type error.
- An event bound with bind through it, with a payload or without, gives an awaitable of
  `object`.

Consumer:
- An implementation satisfies the Consumer protocol.

Serializer and Deserializer:
- msgspec's json.encode and json.decode type-check as a Serializer and a Deserializer, and
  round-trip a payload.
- A codec of the wrong shape is a type error. Each `# pyright: ignore[...]` marks an expected
  one. The project enables `reportUnnecessaryTypeIgnoreComment`, so if one stops being an
  error, the type check fails.

destination_of and operation_of:
- A config's Destination and Operation are read from it.
- A config without them, or whose attributes of those names hold something else, has no
  destination and sends each message.

reserved_for:
- Gives the envelope names it's given for a config that reserves none, and adds a Reserving
  config's names, all in lower case.
"""

from collections.abc import Awaitable, Callable
from typing import Any, assert_type

import msgspec
import pytest

from stratae.events import (
    ASYNC,
    AsyncDirectBus,
    AsyncPubSubProducer,
    Consumer,
    Deserializer,
    Destination,
    DirectBus,
    EmitCallable,
    Emitter,
    EventType,
    Operation,
    Producer,
    PubSub,
    PubSubProducer,
    Serializer,
    bind,
    destination_of,
    operation_of,
    reserved_for,
)
from stratae.events.envelope import EnvelopeHeaders

_HEADERS = EnvelopeHeaders()
"""The default names an envelope travels under."""


class _SyncProducer:
    def emit[E](self, event: EventType[E, Any, Any, Any], config: Any, payload: E) -> None: ...


class _AsyncProducer:
    async def emit[E](
        self, event: EventType[E, Any, Any, Any], config: Any, payload: E
    ) -> None: ...


class _Consumer:
    def handle(
        self,
        config: Any,
        fn: Callable[[Any], Any] | None = None,
    ) -> Any: ...


def test_sync_producer_satisfies_protocol():
    """
    A sync Producer implementation should satisfy the Producer protocol.

    Given: A class with a sync emit method
    When: checked against the Producer protocol
    Then: isinstance should return True
    """
    assert isinstance(_SyncProducer(), Producer)


def test_async_producer_satisfies_protocol():
    """
    An async Producer implementation should satisfy the Producer protocol.

    Given: A class with an async emit method
    When: checked against the Producer protocol
    Then: isinstance should return True
    """
    assert isinstance(_AsyncProducer(), Producer)


def test_producer_emit_satisfies_emit_callable_protocol():
    """
    Producer.emit should satisfy the EmitCallable protocol.

    Given: a Producer implementation's bound emit method
    When: checked against the EmitCallable protocol
    Then: isinstance should return True
    """
    assert isinstance(_SyncProducer().emit, EmitCallable)


def test_consumer_satisfies_protocol():
    """
    A Consumer implementation should satisfy the Consumer protocol.

    Given: A class with a handle method
    When: checked against the Consumer protocol
    Then: isinstance should return True
    """
    assert isinstance(_Consumer(), Consumer)


class _Order(msgspec.Struct):
    order_id: int


def test_msgspec_codecs_satisfy_the_codec_protocols():
    """
    Msgspec's JSON codecs should type-check as a Serializer and a Deserializer.

    Given: msgspec.json.encode and msgspec.json.decode, typed as the protocols
    When: a payload is serialized and deserialized through them
    Then: the payload round-trips
    """
    # Arrange
    serializer: Serializer[object] = msgspec.json.encode
    deserializer: Deserializer = msgspec.json.decode

    # Act
    restored = deserializer(serializer(_Order(order_id=7)), type=_Order)

    # Assert
    assert restored == _Order(order_id=7)


def test_codecs_of_the_wrong_shape_are_refused():
    """
    A codec of the wrong shape should be a type error.

    Given: a serializer returning text, and a deserializer without a `type` keyword
    When: each is typed as its protocol
    Then: both are type errors
    """

    def to_text(obj: object) -> str:
        return str(obj)

    def untyped_decode(data: bytes) -> object:
        return data

    # Act / Assert
    _serializer: Serializer[object] = to_text  # pyright: ignore[reportAssignmentType]
    _deserializer: Deserializer = untyped_decode  # pyright: ignore[reportAssignmentType]


def test_every_producer_is_a_pubsub_producer():
    """
    A Producer emits any event, so it should match PubSubProducer too.

    Given: A DirectBus and an AsyncDirectBus, which emit any event
    When: Each is assigned to Producer, and to PubSubProducer
    Then: Both assignments type-check, and isinstance agrees
    """
    for bus in (DirectBus(), AsyncDirectBus()):
        # Act
        producer: Producer = bus
        pubsub_producer: PubSubProducer = bus

        # Assert
        assert isinstance(producer, Producer)
        assert isinstance(pubsub_producer, PubSubProducer)


def test_only_async_producers_are_async_pubsub_producers():
    """
    A producer whose emit is awaited should match AsyncPubSubProducer, and a sync one shouldn't.

    The `# pyright: ignore[...]` marks an expected type error.

    Given: An AsyncDirectBus and a DirectBus
    When: Each is assigned to AsyncPubSubProducer
    Then: The first type-checks, and the second is a type error
    """
    # Act
    async_producer: AsyncPubSubProducer = AsyncDirectBus()
    sync_producer: AsyncPubSubProducer = DirectBus()  # pyright: ignore[reportAssignmentType]

    # Assert
    assert isinstance(async_producer, AsyncDirectBus)
    assert isinstance(sync_producer, DirectBus)


async def test_events_bound_through_an_async_pubsub_producer_are_awaited():
    """
    An event bound with bind to an AsyncPubSubProducer's emit should give an awaitable.

    Given: A producer typed only as AsyncPubSubProducer, and a PubSub event with a payload
        and one without
    When: Each is bound with bind to its emit, and called
    Then: Each emitter is typed as giving `Awaitable[object]`, and its emit reaches the handler
    """
    # Arrange
    bus = AsyncDirectBus()
    placed = PubSub(int, name="placed")
    pinged = PubSub(name="pinged")
    seen: list[object] = []
    bus.handle(placed, seen.append)
    bus.handle(pinged, lambda: seen.append("ping"))

    async def emit_through(producer: AsyncPubSubProducer) -> None:
        place = bind(placed, Emitter(ASYNC, producer.emit))
        ping = bind(pinged, Emitter(ASYNC, producer.emit))
        assert_type(place, Callable[[int], Awaitable[object]])
        assert_type(ping, Callable[[], Awaitable[object]])
        await place(1)
        await ping()

    # Act
    await emit_through(bus)

    # Assert
    assert seen == [1, "ping"]


class _Describing:
    """A config with a messaging destination and operation, whatever they hold."""

    def __init__(self, destination: object, operation: object) -> None:
        self.messaging_destination = destination
        self.messaging_operation = operation


def test_config_description_read():
    """
    A config's destination and operation should be read from it.

    Given: A config with a Destination and an Operation
    When: destination_of and operation_of read it
    Then: They return its Destination and its Operation
    """
    destination = Destination("testmq", "orders")
    operation = Operation("create", "create")
    config = _Describing(destination, operation)

    assert destination_of(config) is destination
    assert operation_of(config) is operation


@pytest.mark.parametrize(
    "config", ["orders", _Describing("orders", "create")], ids=["undescribed", "unrelated"]
)
def test_undescribed_config_has_no_destination_and_sends(config: object):
    """
    A config that doesn't describe itself should have no destination, and send each message.

    Given: A config with no messaging attributes, and one whose attributes of those names hold
        a string
    When: destination_of and operation_of read it
    Then: It has no destination, and its operation is publish/send
    """
    assert destination_of(config) is None
    assert operation_of(config) == Operation("publish", "send")


class _Reserving:
    """A routing config whose adapter sets headers of its own."""

    @property
    def reserved_headers(self) -> frozenset[str]:
        return frozenset({"X-Attempts", "x-failure"})


def test_reserved_for_adds_a_configs_own_names() -> None:
    """
    reserved_for should give the envelope names given, plus a Reserving config's, in lower case.

    Given: No config, a config that reserves nothing, and one that reserves two names, with
        the default envelope names, and a config that reserves nothing with mixed-case names
    When: Each one's reserved names are worked out
    Then: The first two give the envelope names; the third adds its own; all in lower case
    """
    custom = EnvelopeHeaders("Acme-Msg", "acme-corr", "acme-cause", "acme-at")

    assert reserved_for(None, _HEADERS) == _HEADERS.names
    assert reserved_for("orders", _HEADERS) == _HEADERS.names
    assert reserved_for(_Reserving(), _HEADERS) == _HEADERS.names | {"x-attempts", "x-failure"}
    assert reserved_for(None, custom) == {"acme-msg", "acme-corr", "acme-cause", "acme-at"}
