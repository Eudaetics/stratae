"""
Unit tests for the Handler class.

This test suite verifies the following behaviors:

Handler:
- is_async is False for a synchronous callable.
- is_async is True for an asynchronous callable, including an object with async __call__.
- call stores the wrapped callable.
- config stores the supplied value.
- Calling a Handler invokes the wrapped sync callable.
- Calling a Handler wrapping an async callable returns an awaitable.

Handlers:
- Holds its handlers as a tuple, equal to a plain tuple of them.
- Keeps the sync and async handlers apart.
- adding returns new handlers with the handler added, leaving the original unchanged.
- removing returns new handlers without the handler, leaving the original unchanged.
- removing a handler it doesn't hold returns the same handlers.
- removing takes out only the given registration of a callable registered twice.
"""

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from stratae.events import Handler, Handlers


def _sync_handler(payload: int) -> int:
    """Define a simple sync handler for testing."""
    return 1


async def _async_handler(payload: int) -> int:
    """Define a simple async handler for testing."""
    await asyncio.sleep(0)
    return 2


class _SyncCallable:
    def __call__(self, payload: int) -> int:
        return 3


class _AsyncCallable:
    async def __call__(self, payload: int) -> int:
        return 4


@pytest.mark.parametrize(
    ("call", "expected"),
    [
        (_sync_handler, False),
        (_async_handler, True),
        (_SyncCallable(), False),
        (_AsyncCallable(), True),
    ],
    ids=["sync_function", "async_function", "sync_callable", "async_callable"],
)
def test_is_async_reflects_callable_type(call: Callable[[Any], Any], expected: bool):
    """
    is_async should be False for sync callables and True for async callables.

    Given: A sync or async callable
    When: A Handler is constructed with it
    Then: is_async should match whether the callable is a coroutine function
    """
    assert Handler(call, object()).is_async is expected


def test_call_stores_wrapped_callable():
    """
    Call should reference the callable passed at construction.

    Given: A callable
    When: A Handler is constructed with it
    Then: call should be that callable
    """
    handler = Handler(_sync_handler, object())

    assert handler.call is _sync_handler


def test_config_stores_supplied_value():
    """
    Config should store the value passed at construction.

    Given: A callable and a config value
    When: A Handler is constructed with both
    Then: config should reference the supplied value
    """
    config = {"test": 1}
    handler = Handler(_sync_handler, config)

    assert handler.config is config


def test_calling_handler_invokes_sync_callable():
    """
    Calling a Handler wrapping a sync callable should return its result.

    Given: A Handler wrapping a sync callable
    When: The Handler is called with a payload
    Then: The result should be the callable's return value
    """
    payload = 1
    handler = Handler(_sync_handler, object())

    assert handler(payload) == 1


async def test_calling_handler_wrapping_async_callable_returns_awaitable():
    """
    Calling a Handler wrapping an async callable should return an awaitable.

    Given: A Handler wrapping an async callable
    When: The Handler is called with a payload
    Then: The result should be awaitable and resolve to the callable's return value
    """
    payload = 1
    handler = Handler(_async_handler, object())

    result = await handler(payload)

    assert result == 2


def test_handlers_equal_plain_tuple():
    """
    Handlers should be a tuple of its handlers.

    Given: Two handlers
    When: Handlers is built from them
    Then: It should equal a plain tuple of the same handlers
    """
    # Arrange
    first, second = Handler(_sync_handler, None), Handler(_async_handler, None)

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
    sync_a, sync_b = Handler(_sync_handler, None), Handler(_SyncCallable(), None)
    async_a, async_b = Handler(_async_handler, None), Handler(_AsyncCallable(), None)

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
    first, added = Handler(_sync_handler, None), Handler(_async_handler, None)
    original: Handlers[Handler[Any, Any, Any]] = Handlers((first,))

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
    kept, removed = Handler(_sync_handler, None), Handler(_async_handler, None)
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
    original = Handlers((Handler(_sync_handler, None),))

    # Act
    result = original.removing(Handler(_sync_handler, None))

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
    kept, removed = Handler(_sync_handler, None), Handler(_sync_handler, None)

    # Act
    result = Handlers((kept, removed)).removing(removed)

    # Assert
    assert result == (kept,)
