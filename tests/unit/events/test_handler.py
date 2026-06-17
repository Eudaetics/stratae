"""
Unit tests for the Handler class.

This test suite verifies the following behaviors:

Handler:
- is_async is False for a synchronous callable.
- is_async is True for an asynchronous callable.
- call stores the wrapped callable.
- config stores the supplied value.
- Calling a Handler invokes the wrapped sync callable.
- Calling a Handler wrapping an async callable returns an awaitable.
"""

import asyncio
from typing import Any, Callable

import pytest

from stratae.events.event import Payload
from stratae.events.handler import Handler


def _sync_handler(payload: Payload) -> int:
    """Define a simple sync handler for testing."""
    return 1


async def _async_handler(payload: Payload) -> int:
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
def test_is_async_reflects_callable_type(call: Callable[[Payload], Any], expected: bool):
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
    payload = Payload()
    handler = Handler(_sync_handler, object())

    assert handler(payload) == 1


async def test_calling_handler_wrapping_async_callable_returns_awaitable():
    """
    Calling a Handler wrapping an async callable should return an awaitable.

    Given: A Handler wrapping an async callable
    When: The Handler is called with a payload
    Then: The result should be awaitable and resolve to the callable's return value
    """
    payload = Payload()
    handler = Handler(_async_handler, object())

    result = await handler(payload)

    assert result == 2
