"""
Unit tests for Kind, SYNC and ASYNC.

This test suite verifies the following behaviors:

SYNC:
- Starts from the sync dispatchers.
- Accepts sync handlers, and refuses async functions and objects with an async __call__.
- Doesn't await, and binds with bind_sync.

ASYNC:
- Starts from the async dispatchers.
- Accepts sync and async handlers, awaits, and binds with bind_async.

Kind:
- fitted leaves a call as it is on SYNC; ASYNC leaves an async call, and defers a sync one.
- It's immutable.
"""

import asyncio
import dataclasses

import pytest

from stratae.events import ASYNC, SYNC, Kind, bind_async, bind_sync
from stratae.events.dispatch import ASYNC_DISPATCHERS, SYNC_DISPATCHERS


def _sync_handler(payload: object) -> None:
    return None


async def _async_handler(payload: object) -> None:
    await asyncio.sleep(0)


class _AsyncCallable:
    async def __call__(self, payload: object) -> None:
        await asyncio.sleep(0)


def test_sync_kind():
    """
    SYNC should start from the sync dispatchers and accept sync handlers and hooks only.

    Given: SYNC
    When: Its dispatchers and whether it awaits are read, and handlers of each kind are checked
    Then: It has the sync dispatchers, accepts sync handlers only, and doesn't await
    """
    # Assert
    assert SYNC.dispatchers == SYNC_DISPATCHERS
    assert SYNC.accepts(_sync_handler)
    assert not SYNC.accepts(_async_handler)
    assert not SYNC.accepts(_AsyncCallable())
    assert SYNC.awaits is False
    assert SYNC.bind is bind_sync


def test_async_kind():
    """
    ASYNC should start from the async dispatchers and accept sync and async handlers and hooks.

    Given: ASYNC
    When: Its dispatchers and whether it awaits are read, and handlers of each kind are checked
    Then: It has the async dispatchers, accepts every handler, and awaits
    """
    # Assert
    assert ASYNC.dispatchers == ASYNC_DISPATCHERS
    assert ASYNC.accepts(_sync_handler)
    assert ASYNC.accepts(_async_handler)
    assert ASYNC.awaits is True
    assert ASYNC.bind is bind_async


async def test_fitted_makes_a_call_fit_middleware():
    """
    A kind should fit a handler's call to its middleware: SYNC as it is, ASYNC awaitable.

    Given: A sync call and an async one
    When: SYNC and ASYNC fit each
    Then: SYNC leaves both as they are; ASYNC leaves the async one, and defers the sync one
        so awaiting it gives the sync call's result
    """

    # Arrange
    def call(payload: object) -> str:
        return "sync"

    async def acall(payload: object) -> str:
        await asyncio.sleep(0)
        return "async"

    # Act
    sync_fit = SYNC.fitted(call, False)
    async_fit_of_async = ASYNC.fitted(acall, True)
    deferred_call, deferred_async = ASYNC.fitted(call, False)

    # Assert
    assert sync_fit == (call, False)
    assert SYNC.fitted(acall, True) == (acall, True)
    assert async_fit_of_async == (acall, True)
    assert deferred_async is True
    assert await deferred_call(object()) == "sync"


def test_kind_is_immutable():
    """
    A Kind should be immutable.

    Given: SYNC
    When: One of its fields is assigned
    Then: FrozenInstanceError is raised
    """
    # Act / Assert
    with pytest.raises(dataclasses.FrozenInstanceError):
        SYNC.accepts = _sync_handler  # pyright: ignore[reportAttributeAccessIssue]
    assert isinstance(SYNC, Kind)
