"""
Unit tests for the Producer and Consumer protocols.

This test suite verifies the following behaviors:

Producer:
- A sync implementation satisfies the Producer protocol.
- An async implementation satisfies the Producer protocol.

Consumer:
- An implementation satisfies the Consumer protocol.

Serializer and Deserializer:
- msgspec's json.encode and json.decode type-check as a Serializer and a Deserializer, and
  round-trip a payload.
- A codec of the wrong shape is a type error. Each `# pyright: ignore[...]` marks an expected
  one. The project enables `reportUnnecessaryTypeIgnoreComment`, so if one stops being an
  error, the type check fails.
"""

from collections.abc import Callable
from typing import Any

import msgspec

from stratae.events import Consumer, Deserializer, EmitCallable, EventType, Producer, Serializer


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
