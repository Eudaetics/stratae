"""
Unit tests for the handler classes.

This test suite verifies the following behaviors:

SyncHandler and AsyncHandler:
- Each kind says whether calling it gives an awaitable.
- fn, hooks, call and config store what they were made from.
- Calling a SyncHandler with a message invokes the wrapped callable.
- Calling an AsyncHandler with a message returns an awaitable of the wrapped callable's result.
- Handlers of one kind with equal functions, hooks and configs are equal and hash alike,
  whatever their calls.
- Handlers differing in function, hooks, config or kind are unequal.
- An unhashable function or hook is refused with TypeError.

Handlers:
- Has a length, and iterates over its handlers in order, but can't be indexed.
- Is equal to Handlers holding the same handlers in order, never to a plain tuple.
- Keeps the sync and async handlers apart.
- Finds a handler by value.
- adding returns new handlers with the handler added, leaving the original unchanged.
- removing returns new handlers without the handler, leaving the original unchanged.
- removing a handler it doesn't hold returns the same handlers.
- removing takes out a handler equal to the one given.
"""

import asyncio
from typing import Any

import pytest

from stratae.events import AsyncHandler, HandleCall, Handlers, Hooks, PubSub, SyncHandler

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


def _other_sync_handler(message: HandleCall) -> int:
    """Define a second sync handler, unequal to the first."""
    return 5


def _sync(fn: Any = _sync_handler, config: object = None) -> SyncHandler[object, Any, ..., int]:
    """Make a sync handler of `fn` with no hooks, calling `fn` itself."""
    return SyncHandler(fn, None, fn, config)


def _async(fn: Any = _async_handler, config: object = None) -> AsyncHandler[object, Any, ..., int]:
    """Make an async handler of `fn` with no hooks, calling `fn` itself."""
    return AsyncHandler(fn, None, fn, config)


def _before(event: object, payload: object) -> None:
    """Do nothing before a call."""


class _Unhashable:
    __hash__ = None  # pyright: ignore[reportAssignmentType]

    def __call__(self, message: HandleCall) -> int:
        return 6


class _UnhashableHook:
    __hash__ = None  # pyright: ignore[reportAssignmentType]

    def __call__(self, event: object, payload: object) -> None:
        """Do nothing, as a hook that can't be hashed."""


def test_each_kind_says_whether_it_is_async():
    """
    A handler's kind should say whether calling it gives an awaitable.

    Given: A sync and an async handler
    When: Each is asked whether it's async
    Then: The sync one says no, and the async one says yes
    """
    assert _sync().is_async is False
    assert _async().is_async is True


def test_handler_stores_its_parts():
    """
    A handler should keep the function, hooks, call and config it was made from.

    Given: A function, hooks, a separate call and a config
    When: A handler is made from them
    Then: Each field is what it was made from
    """
    # Arrange
    hooks: Hooks[Any, Any] = Hooks(before=(_before,))
    config = "config"

    # Act
    handler = SyncHandler(_sync_handler, hooks, _other_sync_handler, config)

    # Assert
    assert handler.fn is _sync_handler
    assert handler.hooks is hooks
    assert handler.call is _other_sync_handler
    assert handler.config is config


def test_handlers_with_equal_parts_are_equal_whatever_their_calls():
    """
    Handlers of one kind should be equal when their function, hooks and config are.

    Given: Two sync handlers of one function, equal hooks and one config, with different calls
    When: They're compared and hashed
    Then: They're equal and hash alike
    """
    # Arrange
    first = SyncHandler(_sync_handler, Hooks(before=(_before,)), _sync_handler, "config")
    second = SyncHandler(_sync_handler, Hooks(before=(_before,)), _other_sync_handler, "config")

    # Act / Assert
    assert first == second
    assert hash(first) == hash(second)


def test_handlers_differing_in_any_part_or_kind_are_unequal():
    """
    Handlers should be unequal when their function, hooks, config or kind differ.

    Given: A sync handler, and others differing from it in one part each, or in kind
    When: Each is compared with it
    Then: None is equal to it, nor to something that isn't a handler
    """
    # Arrange
    handler = _sync()
    others: list[object] = [
        _sync(_other_sync_handler),
        SyncHandler(_sync_handler, Hooks(before=(_before,)), _sync_handler, None),
        _sync(config="other"),
        AsyncHandler(_sync_handler, None, _async_handler, None),
    ]

    # Act / Assert
    assert all(other != handler for other in others)
    assert handler != "not a handler"


@pytest.mark.parametrize("kind", [SyncHandler, AsyncHandler])
def test_unhashable_function_or_hook_cannot_be_hashed(kind: type):
    """
    A handler with an unhashable function or hook should refuse to be hashed.

    Given: Handlers with an unhashable callable as their function, or as a hook
    When: Each is hashed
    Then: TypeError says the callable is unhashable
    """
    # Arrange
    by_function = kind(_Unhashable(), None, _sync_handler, None)
    by_hook = kind(_sync_handler, Hooks(before=(_UnhashableHook(),)), _sync_handler, None)

    # Act / Assert
    with pytest.raises(TypeError, match="unhashable"):
        hash(by_function)
    with pytest.raises(TypeError, match="unhashable"):
        hash(by_hook)


def test_calling_handler_invokes_sync_callable():
    """
    Calling a Handler wrapping a sync callable should return its result.

    Given: A Handler wrapping a sync callable
    When: The Handler is called with a message
    Then: The result should be the callable's return value
    """
    handler = _sync()

    assert handler(_message) == 1


async def test_calling_handler_wrapping_async_callable_returns_awaitable():
    """
    Calling a Handler wrapping an async callable should return an awaitable.

    Given: A Handler wrapping an async callable
    When: The Handler is called with a message
    Then: The result should be awaitable and resolve to the callable's return value
    """
    handler = _async()

    result = await handler(_message)

    assert result == 2


def test_handlers_is_a_collection():
    """
    Handlers should have a length, and iterate over its handlers in order.

    Given: Two handlers
    When: Handlers is built from them
    Then: Its length is two, it iterates over them in order, and it can't be indexed
    """
    # Arrange
    first, second = (
        _sync(),
        _async(),
    )

    # Act
    handlers = Handlers((first, second))

    # Assert
    assert len(handlers) == 2
    assert list(handlers) == [first, second]
    assert not hasattr(handlers, "__getitem__")


def test_handlers_equal_by_handlers_in_order():
    """
    Handlers should be equal when they hold the same handlers, in order, and only then.

    Given: Two handlers
    When: Handlers are built from them in the same order, in the other order, and a tuple
    Then: The same order is equal and hashes alike; the other order and the tuple are unequal
    """
    # Arrange
    first, second = (
        _sync(),
        _async(),
    )
    handlers = Handlers((first, second))

    # Act / Assert
    assert handlers == Handlers((first, second))
    assert hash(handlers) == hash(Handlers((first, second)))
    assert handlers != Handlers((second, first))
    assert handlers != (first, second)
    assert repr(handlers) == f"Handlers({(first, second)!r})"


def test_handlers_keeps_sync_and_async_apart():
    """
    Handlers should keep its sync and async handlers apart.

    Given: Sync and async handlers, interleaved
    When: Handlers is built from them
    Then: sync should hold the sync ones and asynchronous the async ones, each in order
    """
    # Arrange
    sync_a, sync_b = (
        _sync(),
        _sync(_SyncCallable()),
    )
    async_a, async_b = (
        _async(),
        _async(_AsyncCallable()),
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
        _sync(),
        _async(),
    )
    original: Handlers[object, Any, int] = Handlers((first,))

    # Act
    result = original.adding(added)

    # Assert
    assert tuple(result) == (first, added)
    assert result.asynchronous == (added,)
    assert tuple(original) == (first,)
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
        _sync(),
        _async(),
    )
    original = Handlers((kept, removed))

    # Act
    result = original.removing(removed)

    # Assert
    assert tuple(result) == (kept,)
    assert result.asynchronous == ()
    assert tuple(original) == (kept, removed)


def test_handlers_removing_absent_handler_returns_same_handlers():
    """
    ``removing`` a handler it doesn't hold should return the same handlers.

    Given: Handlers holding one handler, and another handler it doesn't hold
    When: The other handler is removed
    Then: The same Handlers should be returned
    """
    # Arrange
    original = Handlers((_sync(),))

    # Act
    result = original.removing(_sync(_other_sync_handler))

    # Assert
    assert result is original


def test_handlers_removing_takes_out_an_equal_handler():
    """
    ``removing`` should take out the handler equal to the one given.

    Given: Handlers holding two handlers, and a new handler equal to the second
    When: The new handler is removed
    Then: Only the first should remain
    """
    # Arrange
    kept, held = _sync(), _sync(_other_sync_handler)
    equal: SyncHandler[object, Any, ..., int] = SyncHandler(
        _other_sync_handler, None, _sync_handler, None
    )

    # Act
    result = Handlers((kept, held)).removing(equal)

    # Assert
    assert tuple(result) == (kept,)


def test_handlers_find_a_handler_by_value():
    """
    Handlers should hold a handler equal to one among them.

    Given: Handlers holding one handler
    When: An equal handler, and an unequal one, are looked for
    Then: The equal one is found, and the other isn't
    """
    # Arrange
    handlers = Handlers((_sync(),))

    # Act / Assert
    assert SyncHandler(_sync_handler, None, _other_sync_handler, None) in handlers
    assert _sync(_other_sync_handler) not in handlers
