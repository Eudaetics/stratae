"""
Unit tests for the Handler class.

This test suite verifies the following behaviors:

Handler:
- is_async is False for a synchronous callable.
- is_async is True for an asynchronous callable.
- call stores the wrapped callable.
- Calling a Handler invokes the wrapped sync callable.
- Calling a Handler wrapping an async callable returns an awaitable.
- Two Handlers wrapping the same callable are equal.
- Two Handlers wrapping different callables are not equal.
- A Handler is equal to the raw callable it wraps.
- A raw callable is equal to a Handler wrapping it.
- A Handler is not equal to a non-callable, non-Handler object.
- Two Handlers wrapping the same callable have the same hash.
"""

import asyncio
from typing import Any, Callable

import pytest

from stratae.events.event import EventSchema
from stratae.events.handler import Handler


def _sync_handler(event: EventSchema) -> int:
    """Define a simple sync handler for testing."""
    return 1


async def _async_handler(event: EventSchema) -> int:
    """Define a simple async handler for testing."""
    await asyncio.sleep(0)
    return 2


@pytest.mark.parametrize(
    "call,expected",
    [
        (_sync_handler, False),
        (_async_handler, True),
    ],
)
def test_is_async_reflects_callable_type(call: Callable[[EventSchema], Any], expected: bool):
    """
    is_async should be False for sync callables and True for async callables.

    Given: A sync or async callable
    When: A Handler is constructed with it
    Then: is_async should match whether the callable is a coroutine function
    """
    assert Handler(call).is_async is expected


def test_call_stores_wrapped_callable():
    """
    Call should reference the callable passed at construction.

    Given: A callable
    When: A Handler is constructed with it
    Then: call should be that callable
    """
    handler = Handler(_sync_handler)

    assert handler.call is _sync_handler


def test_calling_handler_invokes_sync_callable():
    """
    Calling a Handler wrapping a sync callable should return its result.

    Given: A Handler wrapping a sync callable
    When: The Handler is called with an event
    Then: The result should be the callable's return value
    """
    event = EventSchema()
    handler = Handler(_sync_handler)

    assert handler(event) == 1


async def test_calling_handler_wrapping_async_callable_returns_awaitable():
    """
    Calling a Handler wrapping an async callable should return an awaitable.

    Given: A Handler wrapping an async callable
    When: The Handler is called with an event
    Then: The result should be awaitable and resolve to the callable's return value
    """
    event = EventSchema()
    handler = Handler(_async_handler)

    result = await handler(event)

    assert result == 2


def test_two_handlers_wrapping_same_callable_are_equal():
    """
    Two Handlers wrapping the same callable should be equal.

    Given: Two Handler instances constructed with the same callable
    When: They are compared with ==
    Then: They should be equal
    """
    assert Handler(_sync_handler) == Handler(_sync_handler)


def test_two_handlers_wrapping_different_callables_are_not_equal():
    """
    Two Handlers wrapping different callables should not be equal.

    Given: Two Handler instances constructed with different callables
    When: They are compared with ==
    Then: They should not be equal
    """
    assert Handler(_sync_handler) != Handler(_async_handler)


def test_handler_is_equal_to_its_raw_callable():
    """
    A Handler should be equal to the raw callable it wraps.

    Given: A Handler and the callable it wraps
    When: They are compared with ==
    Then: They should be equal
    """
    assert Handler(_sync_handler) == _sync_handler


def test_raw_callable_is_equal_to_handler_wrapping_it():
    """
    A raw callable should be equal to a Handler wrapping it.

    Given: A callable and a Handler wrapping it
    When: They are compared with ==
    Then: They should be equal (via reflected __eq__)
    """
    assert _sync_handler == Handler(_sync_handler)


def test_handler_is_not_equal_to_non_callable_non_handler():
    """
    A Handler should not be equal to a non-callable, non-Handler object.

    Given: A Handler instance and an arbitrary non-callable object
    When: They are compared with ==
    Then: They should not be equal
    """
    assert Handler(_sync_handler) != 42


def test_two_handlers_wrapping_same_callable_have_same_hash():
    """
    Two Handlers wrapping the same callable should have the same hash.

    Given: Two Handler instances constructed with the same callable
    When: Their hashes are compared
    Then: The hashes should be equal
    """
    assert hash(Handler(_sync_handler)) == hash(Handler(_sync_handler))
