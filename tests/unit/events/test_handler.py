"""
Unit tests for the handler classes.

This test suite verifies the following behaviors:

SyncHandler and AsyncHandler:
- Each kind says whether calling it gives an awaitable.
- call stores the wrapped callable.
- config stores the supplied value.
- Calling a SyncHandler with a message invokes the wrapped callable.
- Calling an AsyncHandler with a message returns an awaitable of the wrapped callable's result.

Handlers:
- Holds its handlers as a tuple, equal to a plain tuple of them.
- Keeps the sync and async handlers apart.
- adding returns new handlers with the handler added, leaving the original unchanged.
- removing returns new handlers without the handler, leaving the original unchanged.
- removing a handler it doesn't hold returns the same handlers.
- removing takes out only the given registration of a callable registered twice.
"""

import asyncio

from stratae.events import AsyncHandler, HandleCall, Handlers, PubSub, SyncHandler

_ticked = PubSub(int)
_message = HandleCall(_ticked, None, 1)


def _sync_handler(message: HandleCall) -> int:
    """Define a simple sync handler for testing."""
    return 1


async def _async_handler(message: HandleCall) -> int:
    """Define a simple async handler for testing."""
    await asyncio.sleep(0)
    return 2


class _SyncCallable:
    def __call__(self, message: HandleCall) -> int:
        return 3


class _AsyncCallable:
    async def __call__(self, message: HandleCall) -> int:
        return 4


def test_each_kind_says_whether_it_is_async():
    """
    A handler's kind should say whether calling it gives an awaitable.

    Given: A sync and an async handler
    When: Each is asked whether it's async
    Then: The sync one says no, and the async one says yes
    """
    assert SyncHandler(_sync_handler, object()).is_async is False
    assert AsyncHandler(_async_handler, object()).is_async is True


def test_call_stores_wrapped_callable():
    """
    Call should reference the callable passed at construction.

    Given: A callable
    When: A Handler is constructed with it
    Then: call should be that callable
    """
    handler = SyncHandler(_sync_handler, object())

    assert handler.call is _sync_handler


def test_config_stores_supplied_value():
    """
    Config should store the value passed at construction.

    Given: A callable and a config value
    When: A Handler is constructed with both
    Then: config should reference the supplied value
    """
    config = {"test": 1}
    handler = SyncHandler(_sync_handler, config)

    assert handler.config is config


def test_calling_handler_invokes_sync_callable():
    """
    Calling a Handler wrapping a sync callable should return its result.

    Given: A Handler wrapping a sync callable
    When: The Handler is called with a message
    Then: The result should be the callable's return value
    """
    handler = SyncHandler(_sync_handler, object())

    assert handler(_message) == 1


async def test_calling_handler_wrapping_async_callable_returns_awaitable():
    """
    Calling a Handler wrapping an async callable should return an awaitable.

    Given: A Handler wrapping an async callable
    When: The Handler is called with a message
    Then: The result should be awaitable and resolve to the callable's return value
    """
    handler = AsyncHandler(_async_handler, object())

    result = await handler(_message)

    assert result == 2


def test_handlers_equal_plain_tuple():
    """
    Handlers should be a tuple of its handlers.

    Given: Two handlers
    When: Handlers is built from them
    Then: It should equal a plain tuple of the same handlers
    """
    # Arrange
    first, second = (
        SyncHandler(_sync_handler, None),
        AsyncHandler(_async_handler, None),
    )

    # Act
    handlers = Handlers((first, second))

    # Assert
    assert handlers == (first, second)


def test_handlers_keeps_sync_and_async_apart():
    """
    Handlers should keep its sync and async handlers apart.

    Given: Sync and async handlers, interleaved
    When: Handlers is built from them
    Then: sync should hold the sync ones and asynchronous the async ones, each in order
    """
    # Arrange
    sync_a, sync_b = (
        SyncHandler(_sync_handler, None),
        SyncHandler(_SyncCallable(), None),
    )
    async_a, async_b = (
        AsyncHandler(_async_handler, None),
        AsyncHandler(_AsyncCallable(), None),
    )

    # Act
    handlers = Handlers((sync_a, async_a, sync_b, async_b))

    # Assert
    assert handlers.sync == (sync_a, sync_b)
    assert handlers.asynchronous == (async_a, async_b)


def test_handlers_adding_leaves_original_unchanged():
    """
    ``adding`` should return new handlers, with the original unchanged.

    Given: Handlers holding one sync handler
    When: An async handler is added
    Then: The result should hold both, kept apart, and the original only the first
    """
    # Arrange
    first, added = (
        SyncHandler(_sync_handler, None),
        AsyncHandler(_async_handler, None),
    )
    original: Handlers[None, int] = Handlers((first,))

    # Act
    result = original.adding(added)

    # Assert
    assert result == (first, added)
    assert result.asynchronous == (added,)
    assert original == (first,)
    assert original.asynchronous == ()


def test_handlers_removing_leaves_original_unchanged():
    """
    ``removing`` should return new handlers without the handler, with the original unchanged.

    Given: Handlers holding a sync and an async handler
    When: The async handler is removed
    Then: The result should hold only the sync one, and the original both
    """
    # Arrange
    kept, removed = (
        SyncHandler(_sync_handler, None),
        AsyncHandler(_async_handler, None),
    )
    original = Handlers((kept, removed))

    # Act
    result = original.removing(removed)

    # Assert
    assert result == (kept,)
    assert result.asynchronous == ()
    assert original == (kept, removed)


def test_handlers_removing_absent_handler_returns_same_handlers():
    """
    ``removing`` a handler it doesn't hold should return the same handlers.

    Given: Handlers holding one handler, and another handler it doesn't hold
    When: The other handler is removed
    Then: The same Handlers should be returned
    """
    # Arrange
    original = Handlers((SyncHandler(_sync_handler, None),))

    # Act
    result = original.removing(SyncHandler(_sync_handler, None))

    # Assert
    assert result is original


def test_handlers_removing_takes_out_only_that_registration():
    """
    ``removing`` should take out only the given registration of a callable registered twice.

    Given: Two Handlers wrapping the same callable
    When: One of them is removed
    Then: The other should remain
    """
    # Arrange
    kept, removed = (
        SyncHandler(_sync_handler, None),
        SyncHandler(_sync_handler, None),
    )

    # Act
    result = Handlers((kept, removed)).removing(removed)

    # Assert
    assert result == (kept,)
