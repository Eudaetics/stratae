"""
Unit tests for the Producer and Consumer protocols.

This test suite verifies the following behaviors:

Producer:
- A sync implementation satisfies the Producer protocol.
- An async implementation satisfies the Producer protocol.

Consumer:
- An implementation satisfies the Consumer protocol.
"""

from typing import Any, Callable

from stratae.events import Consumer, DispatchPattern, EmitCallable, Event, Producer


class _SyncProducer:
    def emit[T: DispatchPattern[Any, Any], E, Signal: bool](
        self, event: Event[T, E, Signal], config: Any, payload: E
    ) -> None: ...


class _AsyncProducer:
    async def emit[T: DispatchPattern[Any, Any], E, Signal: bool](
        self, event: Event[T, E, Signal], config: Any, payload: E
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
