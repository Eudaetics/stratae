"""
Unit tests for event hooks and the runners adapters use for them.

This test suite verifies the following behaviors:

Hooks:
- Every slot defaults to empty, and slots given as lists are frozen as tuples.
- has_async reports whether any hook is async, including an object with async __call__.

run_hooks:
- On success, runs before, the call, success with the reply, then complete with None.
- On failure, runs before, the call, failure with the error, then complete with the error,
  and re-raises the call's error.
- A before hook that raises prevents the call, and no other hook runs.
- The first before hook to raise stops the rest.
- A success or complete hook that raises is logged; every observing hook still runs, and
  the reply stands.
- A failure hook that raises is logged, and the call's error still propagates.
- A BaseException skips failure; complete still sees it.
- A before hook that returns an awaitable raises TypeError, and the awaitable is closed if
  it can be; an observing hook that does is logged like any other hook error.

run_hooks_async:
- Awaits async hooks and runs sync ones, around an awaited call.
- On failure, runs failure then complete, and re-raises the call's error.
- An async failure hook that raises is logged, and the call's error still propagates.
- A cancelled call skips failure; complete sees the CancelledError.
- Observing hooks in a slot run concurrently, so a slow one doesn't hold up the rest.

with_handle_hooks:
- Leaves a handler unwrapped without hooks.
- Wraps a sync handler with sync hooks in a sync call.
- Wraps a handler in an async call when the handler or any hook is async.
"""

import asyncio
from typing import Any, Generator, cast

import pytest

from stratae.events import Hooks, PubSub, Request
from stratae.events.hooks import run_hooks, run_hooks_async, with_handle_hooks


class _Order:
    def __init__(self, item: str) -> None:
        self.item = item


_order_placed = PubSub(_Order)


def _recording_hooks(log: list[str]) -> Hooks[Any, Any]:
    """Return hooks that log each slot as it runs, with what it was given."""

    def before(event: Any, payload: Any) -> None:
        log.append(f"before {payload.item}")

    def success(event: Any, payload: Any, reply: Any) -> None:
        log.append(f"success {reply}")

    def failure(event: Any, payload: Any, error: Exception) -> None:
        log.append(f"failure {error}")

    def complete(event: Any, payload: Any, error: BaseException | None) -> None:
        log.append(f"complete {type(error).__name__}")

    return Hooks(before=(before,), success=(success,), failure=(failure,), complete=(complete,))


def _raise(error: BaseException) -> Any:
    """Return a hook that raises the given error."""

    def hook(*_: Any) -> None:
        raise error

    return hook


# region: Hooks


def test_hooks_default_to_empty_and_freeze_lists() -> None:
    """
    Hooks should default every slot to empty, and store slots given as lists as tuples.

    Given: Hooks built with no slots, and Hooks built with a list for one slot
    When: Their slots are read
    Then: Empty slots are empty tuples, and the list is stored as a tuple
    """

    # Arrange
    def hook(event: Any, payload: Any) -> None: ...

    # Act
    empty: Hooks[Any, Any] = Hooks()
    listed: Hooks[Any, Any] = Hooks(before=cast(Any, [hook]))

    # Assert
    assert (empty.before, empty.success, empty.failure, empty.complete) == ((), (), (), ())
    assert listed.before == (hook,)


def test_hooks_report_whether_any_hook_is_async() -> None:
    """
    has_async should say whether any hook is async, wherever it is.

    Given: Sync hooks, an async function hook, and an object whose __call__ is async
    When: has_async is read
    Then: It's False for the sync hooks and True for either async kind
    """

    # Arrange
    def sync_hook(event: Any, payload: Any, error: BaseException | None) -> None: ...

    async def async_hook(event: Any, payload: Any, error: BaseException | None) -> None: ...

    class AsyncCallable:
        async def __call__(self, event: Any, payload: Any, error: BaseException | None) -> None: ...

    # Act
    sync_only: Hooks[Any, Any] = Hooks(complete=(sync_hook,))
    with_function: Hooks[Any, Any] = Hooks(complete=(sync_hook, async_hook))
    with_object: Hooks[Any, Any] = Hooks(complete=(AsyncCallable(),))

    # Assert
    assert sync_only.has_async is False
    assert with_function.has_async is True
    assert with_object.has_async is True


# endregion
# region: run_hooks


def test_run_hooks_order_on_success() -> None:
    """
    run_hooks should run before, the call, success, then complete.

    Given: Hooks recording each slot, and a call that returns a reply
    When: run_hooks runs the call
    Then: The slots run in order, success sees the reply, complete sees no error, and the
        call's reply is returned
    """
    # Arrange
    log: list[str] = []

    def call(order: _Order) -> str:
        log.append("call")
        return f"placed {order.item}"

    # Act
    reply = run_hooks(_recording_hooks(log), _order_placed, _Order("lamp"), call, _Order("lamp"))

    # Assert
    assert reply == "placed lamp"
    assert log == ["before lamp", "call", "success placed lamp", "complete NoneType"]


def test_run_hooks_order_on_failure() -> None:
    """
    run_hooks should run failure then complete when the call raises, and re-raise.

    Given: Hooks recording each slot, and a call that raises
    When: run_hooks runs the call
    Then: failure and complete see the error, success doesn't run, and the error propagates
    """
    # Arrange
    log: list[str] = []

    # Act
    with pytest.raises(LookupError, match="out of stock"):
        run_hooks(
            _recording_hooks(log),
            _order_placed,
            _Order("lamp"),
            _raise(LookupError("out of stock")),
        )

    # Assert
    assert log == ["before lamp", "failure out of stock", "complete LookupError"]


def test_run_hooks_before_raising_prevents_call() -> None:
    """
    A before hook that raises should prevent the call and every other hook.

    Given: A before hook that raises, and recording hooks in the other slots
    When: run_hooks runs a call
    Then: The before hook's error propagates, and neither the call nor any hook runs
    """
    # Arrange
    log: list[str] = []
    recording = _recording_hooks(log)
    hooks: Hooks[Any, Any] = Hooks(
        before=(_raise(PermissionError("denied")),),
        success=recording.success,
        failure=recording.failure,
        complete=recording.complete,
    )

    # Act
    with pytest.raises(PermissionError, match="denied"):
        run_hooks(hooks, _order_placed, _Order("lamp"), log.append, "call")

    # Assert
    assert log == []


def test_run_hooks_before_first_raising_stops_the_rest() -> None:
    """
    The first before hook to raise should stop the before hooks after it.

    Given: Two before hooks, the first raising
    When: run_hooks runs a call
    Then: The second before hook doesn't run
    """
    # Arrange
    log: list[str] = []

    def second(event: Any, payload: Any) -> None:
        log.append("second")

    hooks: Hooks[Any, Any] = Hooks(before=(_raise(PermissionError("denied")), second))

    # Act
    with pytest.raises(PermissionError):
        run_hooks(hooks, _order_placed, _Order("lamp"), str, "reply")

    # Assert
    assert log == []


def test_run_hooks_observing_hook_errors_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    """
    A success or complete hook that raises should be logged, not change the outcome.

    Given: A raising success hook before a recording one, a raising complete hook before a
        recording one, and a recording failure hook
    When: run_hooks runs a call that returns
    Then: The reply is returned, every observing hook runs, failure doesn't, complete sees
        no error, and both hook errors are logged
    """
    # Arrange
    log: list[str] = []
    recording = _recording_hooks(log)
    hooks: Hooks[Any, Any] = Hooks(
        success=(_raise(ValueError("audit down")), *recording.success),
        failure=recording.failure,
        complete=(_raise(RuntimeError("metrics down")), *recording.complete),
    )

    # Act
    reply = run_hooks(hooks, _order_placed, _Order("lamp"), str, "reply")

    # Assert
    assert reply == "reply"
    assert log == ["success reply", "complete NoneType"]
    logged = [str(record.exc_info[1]) for record in caplog.records if record.exc_info]
    assert logged == ["audit down", "metrics down"]


def test_run_hooks_failure_hook_error_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    """
    A failure hook that raises should be logged, and the call's error should still propagate.

    Given: A raising failure hook, and a recording complete hook, around a call that raises
    When: run_hooks runs the call
    Then: The call's error propagates, complete sees it, and the hook's error is logged
    """
    # Arrange
    log: list[str] = []
    hooks: Hooks[Any, Any] = Hooks(
        failure=(_raise(RuntimeError("alert failed")),), complete=_recording_hooks(log).complete
    )

    # Act
    with pytest.raises(LookupError, match="out of stock"):
        run_hooks(hooks, _order_placed, _Order("lamp"), _raise(LookupError("out of stock")))

    # Assert
    assert log == ["complete LookupError"]
    assert "alert failed" in caplog.text


def test_run_hooks_base_exception_skips_failure() -> None:
    """
    A BaseException from the call should skip failure, while complete still sees it.

    Given: Recording hooks, and a call raising KeyboardInterrupt
    When: run_hooks runs the call
    Then: failure doesn't run, complete sees the KeyboardInterrupt, and it propagates
    """
    # Arrange
    log: list[str] = []

    # Act
    with pytest.raises(KeyboardInterrupt):
        run_hooks(_recording_hooks(log), _order_placed, _Order("lamp"), _raise(KeyboardInterrupt()))

    # Assert
    assert log == ["before lamp", "complete KeyboardInterrupt"]


def test_run_hooks_rejects_awaitable_from_before_hook() -> None:
    """
    A before hook on the sync path that returns an awaitable should raise TypeError.

    Given: A sync before hook returning a coroutine, which isn't detected as async
    When: run_hooks runs a call
    Then: TypeError names the hook, and the coroutine is closed rather than left unawaited
    """

    # Arrange
    async def work() -> None: ...

    coroutine = work()

    def returns_coroutine(*_: object) -> object:
        return coroutine

    hooks: Hooks[Any, Any] = Hooks(before=(returns_coroutine,))

    # Act / Assert
    with pytest.raises(TypeError, match="returned an awaitable"):
        run_hooks(hooks, _order_placed, _Order("lamp"), str, "reply")
    assert coroutine.cr_frame is None


def test_run_hooks_rejects_awaitable_without_close() -> None:
    """
    A hook returning an awaitable that can't be closed should still raise TypeError.

    Given: A before hook returning an awaitable object with no close method
    When: run_hooks runs a call
    Then: TypeError names the hook, and the call doesn't run
    """
    # Arrange
    calls: list[str] = []

    class Pending:
        def __await__(self) -> Generator[None, None, None]:
            yield

    def returns_pending(*_: object) -> object:
        return Pending()

    hooks: Hooks[Any, Any] = Hooks(before=(returns_pending,))

    # Act / Assert
    with pytest.raises(TypeError, match="returned an awaitable"):
        run_hooks(hooks, _order_placed, _Order("lamp"), calls.append, "call")
    assert calls == []


# endregion
# region: run_hooks_async


async def test_run_hooks_async_mixes_sync_and_async_hooks() -> None:
    """
    run_hooks_async should await async hooks and run sync ones, around an awaited call.

    Given: An async before hook, sync recording hooks, and an async call
    When: run_hooks_async runs the call
    Then: Every hook runs in order around the call, and the call's reply is returned
    """
    # Arrange
    log: list[str] = []
    recording = _recording_hooks(log)

    async def before(event: Any, payload: Any) -> None:
        log.append(f"async before {payload.item}")

    async def call(order: _Order) -> str:
        log.append("call")
        return f"placed {order.item}"

    hooks: Hooks[Any, Any] = Hooks(
        before=(before,), success=recording.success, complete=recording.complete
    )

    # Act
    reply = await run_hooks_async(hooks, _order_placed, _Order("lamp"), call, _Order("lamp"))

    # Assert
    assert reply == "placed lamp"
    assert log == ["async before lamp", "call", "success placed lamp", "complete NoneType"]


async def test_run_hooks_async_order_on_failure() -> None:
    """
    run_hooks_async should run failure then complete when the call raises, and re-raise.

    Given: An async failure hook, a sync complete hook, and an async call that raises
    When: run_hooks_async runs the call
    Then: failure and complete see the error, and the error propagates
    """
    # Arrange
    log: list[str] = []

    async def failure(event: Any, payload: Any, error: Exception) -> None:
        log.append(f"failure {error}")

    async def call() -> None:
        raise LookupError("out of stock")

    hooks: Hooks[Any, Any] = Hooks(failure=(failure,), complete=_recording_hooks(log).complete)

    # Act
    with pytest.raises(LookupError):
        await run_hooks_async(hooks, _order_placed, _Order("lamp"), call)

    # Assert
    assert log == ["failure out of stock", "complete LookupError"]


async def test_run_hooks_async_failure_hook_error_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    An async failure hook that raises should be logged, and the call's error should propagate.

    Given: An async failure hook that raises, around an async call that raises
    When: run_hooks_async runs the call
    Then: The call's error propagates, and the hook's error is logged
    """

    # Arrange
    async def failure(event: Any, payload: Any, error: Exception) -> None:
        raise RuntimeError("alert failed")

    async def call() -> None:
        raise LookupError("out of stock")

    hooks: Hooks[Any, Any] = Hooks(failure=(failure,))

    # Act
    with pytest.raises(LookupError, match="out of stock"):
        await run_hooks_async(hooks, _order_placed, _Order("lamp"), call)

    # Assert
    assert "alert failed" in caplog.text


async def test_run_hooks_async_cancellation_reaches_complete_only() -> None:
    """
    A cancelled call should skip failure, while complete sees the CancelledError.

    Given: Recording hooks around an async call that's cancelled
    When: run_hooks_async runs the call
    Then: failure doesn't run, complete sees CancelledError, and the cancellation propagates
    """
    # Arrange
    log: list[str] = []

    async def call() -> None:
        raise asyncio.CancelledError

    # Act
    with pytest.raises(asyncio.CancelledError):
        await run_hooks_async(_recording_hooks(log), _order_placed, _Order("lamp"), call)

    # Assert
    assert log == ["before lamp", "complete CancelledError"]


async def test_run_hooks_async_observing_hooks_run_concurrently() -> None:
    """
    Observing hooks in one slot should run concurrently, not one after another.

    Given: Two async success hooks, the first waiting until the second has started
    When: run_hooks_async runs a call that returns
    Then: Both finish, which they could only do if the second started while the first waited
    """
    # Arrange
    second_started = asyncio.Event()
    log: list[str] = []

    async def first(event: Any, payload: Any, reply: Any) -> None:
        await asyncio.wait_for(second_started.wait(), timeout=1)
        log.append("first")

    async def second(event: Any, payload: Any, reply: Any) -> None:
        second_started.set()
        log.append("second")

    async def call() -> str:
        return "reply"

    hooks: Hooks[Any, Any] = Hooks(success=(first, second))

    # Act
    reply = await run_hooks_async(hooks, _order_placed, _Order("lamp"), call)

    # Assert
    assert reply == "reply"
    assert log == ["second", "first"]


# endregion
# region: with_handle_hooks


def test_with_handle_hooks_leaves_unhooked_handler() -> None:
    """
    A handler without hooks should be registered as it is.

    Given: A sync handler and no hooks
    When: with_handle_hooks wraps the handler
    Then: The handler itself is returned, still sync
    """
    # Act
    call, is_async = with_handle_hooks(_order_placed, None, str, False)

    # Assert
    assert call is str
    assert is_async is False


def test_with_handle_hooks_sync_wrapper_runs_hooks() -> None:
    """
    A sync handler with sync handle hooks should become a sync call running the hooks.

    Given: Sync hooks, and a sync handler
    When: The wrapped call is called with a payload
    Then: It's sync, and the hooks run around the handler, seeing its return
    """
    # Arrange
    log: list[str] = []
    event = Request(_Order, reply=str)

    # Act
    call, is_async = with_handle_hooks(
        event, _recording_hooks(log), lambda order: f"placed {order.item}", False
    )
    reply = call(_Order("lamp"))

    # Assert
    assert is_async is False
    assert reply == "placed lamp"
    assert log == ["before lamp", "success placed lamp", "complete NoneType"]


@pytest.mark.parametrize("handler_is_async", [True, False], ids=["async handler", "sync handler"])
async def test_with_handle_hooks_async_wrapper(handler_is_async: bool) -> None:
    """
    A handler should become an async call when it or any of its hooks is async.

    Given: An async before hook, and a sync or async handler
    When: The wrapped call is awaited with a payload
    Then: It's async, and the hooks run around the handler, seeing its return
    """
    # Arrange
    log: list[str] = []

    async def before(event: Any, payload: Any) -> None:
        log.append(f"before {payload.item}")

    async def async_handler(order: _Order) -> str:
        return f"placed {order.item}"

    def sync_handler(order: _Order) -> str:
        return f"placed {order.item}"

    recording = _recording_hooks(log)
    event = Request(_Order, reply=str)
    hooks: Hooks[Any, Any] = Hooks(before=(before,), success=recording.success)
    handler = async_handler if handler_is_async else sync_handler

    # Act
    call, is_async = with_handle_hooks(event, hooks, handler, handler_is_async)
    reply = await call(_Order("lamp"))

    # Assert
    assert is_async is True
    assert reply == "placed lamp"
    assert log == ["before lamp", "success placed lamp"]


# endregion
