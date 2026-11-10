"""
Tests for is_async_callable: whether calling something gives a coroutine.

This test suite verifies the following behaviors:

True for:
- Async functions, and async methods bound or not.
- Objects whose class defines `async def __call__`.
- Functions marked with `inspect.markcoroutinefunction`.
- `functools.partial` objects of any of these, nested or not.

False for:
- Sync functions, and sync methods bound or not.
- Objects whose class defines a sync `__call__`.
- `functools.partial` objects of any of these.
- Classes, including a class whose instances have `async def __call__`, and partials of one.
- Async generator functions.

Typing:
- It narrows its argument to the async form for type checkers.
"""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from functools import partial
from inspect import markcoroutinefunction
from typing import Any, assert_type

import pytest

from stratae.callables import is_async_callable


async def _async_function() -> int:
    await asyncio.sleep(0)
    return 1


def _sync_function() -> int:
    return 1


async def _async_generator() -> AsyncGenerator[int]:
    await asyncio.sleep(0)
    yield 1


def _marked() -> Awaitable[int]:
    return _async_function()


markcoroutinefunction(_marked)


class _Methods:
    async def async_method(self) -> int:
        await asyncio.sleep(0)
        return 1

    def sync_method(self) -> int:
        return 1


class _AsyncCall:
    async def __call__(self) -> int:
        await asyncio.sleep(0)
        return 1


class _SyncCall:
    def __call__(self) -> int:
        return 1


_ASYNC: dict[str, Callable[..., Any]] = {
    "async function": _async_function,
    "bound async method": _Methods().async_method,
    "unbound async method": _Methods.async_method,
    "object with async __call__": _AsyncCall(),
    "marked function": _marked,
    "partial of an async function": partial(_async_function),
    "partial of a bound async method": partial(_Methods().async_method),
    "partial of an object with async __call__": partial(_AsyncCall()),
    "partial of a partial of an object with async __call__": partial(partial(_AsyncCall())),
    "partial of a marked function": partial(_marked),
}

_SYNC: dict[str, Callable[..., Any]] = {
    "sync function": _sync_function,
    "bound sync method": _Methods().sync_method,
    "unbound sync method": _Methods.sync_method,
    "object with sync __call__": _SyncCall(),
    "partial of a sync function": partial(_sync_function),
    "partial of a bound sync method": partial(_Methods().sync_method),
    "partial of an object with sync __call__": partial(_SyncCall()),
    "class": _SyncCall,
    "class whose instances have async __call__": _AsyncCall,
    "partial of a class whose instances have async __call__": partial(_AsyncCall),
    "async generator function": _async_generator,
}


@pytest.mark.parametrize("fn", list(_ASYNC.values()), ids=list(_ASYNC))
def test_true_when_calling_gives_a_coroutine(fn: Callable[..., Any]):
    """
    is_async_callable should be true for anything whose call gives a coroutine.

    Given: An async callable of one kind
    When: It's inspected
    Then: It reads as async
    """
    assert is_async_callable(fn)


@pytest.mark.parametrize("fn", list(_SYNC.values()), ids=list(_SYNC))
def test_false_when_calling_gives_no_coroutine(fn: Callable[..., Any]):
    """
    is_async_callable should be false for anything whose call gives no coroutine.

    A class builds an instance, whatever its instances' __call__ is, and an async generator
    function gives an async generator.

    Given: A callable of one kind whose call gives no coroutine
    When: It's inspected
    Then: It reads as sync
    """
    assert not is_async_callable(fn)


def test_narrows_to_the_async_form():
    """
    is_async_callable should narrow its argument to the async form for type checkers.

    Given: A callable typed as giving an int or an awaitable of one
    When: is_async_callable is true for it
    Then: Type checkers see it as giving an awaitable of an int
    """
    # Arrange
    fn: Callable[[], int] | Callable[[], Awaitable[int]] = _async_function

    # Act / Assert
    if is_async_callable(fn):
        assert_type(fn, Callable[[], Awaitable[int]])
    else:
        pytest.fail("an async function read as sync")
