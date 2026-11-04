"""
Unit tests for an AsyncScope's start hooks, exit hooks and success callbacks.

This test suite verifies the following behaviors:

- Start hooks, sync or async, run in order once the scope is active, and get the
  activation's token, whose state they share with the exit hooks.
- A start hook that raises refuses the activation, closing what was entered and
  propagating its error; activate() raises it too.
- Exit hooks, sync or async, run in order after every resource has exited, with the
  activation's error or None.
- Each activation runs the exit hooks registered when it started.
- An exit hook or success callback that raises is logged, and the rest still run.
- Success callbacks, sync or async, run only after a clean exit, before the exit hooks.
- activate() returns the token the hooks get, and deactivate() runs the exit hooks.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import suppress

import pytest

from stratae.lifecycle import AsyncScope, async_resource
from stratae.lifecycle.scope import AsyncActivation


class _Failure(Exception):
    """A deliberate failure, raised by the test."""


class _CommitFailed(Exception):
    """A failed commit, raised by a resource's cleanup."""


async def _refuse(activation: AsyncActivation) -> None:
    """Refuse the activation, as an async start hook."""
    await asyncio.sleep(0)
    raise _Failure()


def _kind(error: BaseException | None) -> type[BaseException] | None:
    """Return an error's type, or None for no error."""
    return type(error) if error else None


@pytest.fixture(params=["dense", "sparse"])
def work(request: pytest.FixtureRequest) -> AsyncScope:
    """Provide a context scope, with each kind of storage."""
    return AsyncScope("work", storage=request.param)


def _session(work: AsyncScope, events: list[str], *, commit_fails: bool = False):
    """Register a cached async resource that records its entry and how it exited."""

    @work.cache()
    @async_resource
    async def get_session() -> AsyncGenerator[str, None]:
        await asyncio.sleep(0)
        events.append("open")
        try:
            yield "session"
        except BaseException as error:
            events.append(f"rollback {type(error).__name__}")
            raise
        if commit_fails:
            raise _CommitFailed()
        events.append("commit")

    return get_session


async def test_sync_and_async_start_hooks_run_in_order(work: AsyncScope):
    """
    Sync and async start hooks should run in order, once the scope is active.

    Given: A sync start hook, then an async one using a cached resource of the scope
    When: The scope is activated
    Then: Both run in order, and the resource they entered is the block's
    """
    # Arrange
    events: list[str] = []
    get_session = _session(work, events)
    work.on_enter(lambda activation: events.append("sync"))

    @work.on_enter
    async def opened(activation: AsyncActivation) -> None:
        events.append(f"async {await get_session()}")

    # Act
    async with work:
        events.append(f"block {await get_session()}")

    # Assert
    assert events == ["sync", "open", "async session", "block session", "commit"]


async def test_hooks_share_the_activations_state(work: AsyncScope):
    """
    An async start hook and an async exit hook should share the activation's state.

    Given: An async start hook storing a value in the state, and an async exit hook
    When: The scope is activated and deactivated
    Then: The exit hook reads the value
    """
    # Arrange
    seen: list[object] = []

    @work.on_enter
    async def started(activation: AsyncActivation) -> None:
        await asyncio.sleep(0)
        activation.state["started"] = "now"

    @work.on_exit
    async def finished(activation: AsyncActivation, error: BaseException | None) -> None:
        await asyncio.sleep(0)
        seen.append(activation.state["started"])

    # Act
    async with work:
        pass

    # Assert
    assert seen == ["now"]


async def test_raising_start_hook_refuses_the_activation(work: AsyncScope):
    """
    An async start hook that raises should refuse the activation, closing what was entered.

    Given: A start hook that enters a resource, then one that raises, and an exit hook
    When: The scope is activated
    Then: The block never runs, the resource rolls back, the exit hook sees the error, and
        the scope is inactive
    """
    # Arrange
    events: list[str] = []
    get_session = _session(work, events)
    seen: list[type[BaseException] | None] = []

    @work.on_enter
    async def open_session(activation: AsyncActivation) -> None:
        await get_session()

    work.on_enter(_refuse)
    work.on_exit(lambda activation, error: seen.append(_kind(error)))

    # Act
    with pytest.raises(_Failure):
        async with work:
            events.append("block")

    # Assert
    assert events == ["open", "rollback _Failure"]
    assert seen == [_Failure]
    assert not work.is_active()


async def test_refused_activate_raises(work: AsyncScope):
    """
    activate() should raise, leaving the scope inactive, when a start hook refuses.

    Given: An async start hook that raises
    When: activate() is awaited
    Then: The error propagates, and the scope isn't active
    """
    # Arrange
    work.on_enter(_refuse)

    # Act & Assert
    with pytest.raises(_Failure):
        await work.activate()
    assert not work.is_active()


@pytest.mark.parametrize(
    ("block_fails", "commit_fails", "expected"),
    [
        (False, False, None),
        (True, False, _Failure),
        (False, True, _CommitFailed),
    ],
    ids=["clean", "block raises", "commit fails"],
)
async def test_exit_hooks_see_the_activations_outcome(
    work: AsyncScope, block_fails: bool, commit_fails: bool, expected: type[BaseException] | None
):
    """
    Sync and async exit hooks should run after every resource exits, with the outcome.

    Given: A resource whose commit may fail, a sync exit hook, then an async one
    When: The block runs cleanly, raises, or the commit fails
    Then: Both hooks run in order, with the scope inactive, and see None, the block's
        error, or the commit's error
    """
    # Arrange
    get_session = _session(work, [], commit_fails=commit_fails)
    seen: list[tuple[str, type[BaseException] | None, bool]] = []
    work.on_exit(lambda activation, error: seen.append(("sync", _kind(error), work.is_active())))

    @work.on_exit
    async def recorded(activation: AsyncActivation, error: BaseException | None) -> None:
        await asyncio.sleep(0)
        seen.append(("async", _kind(error), work.is_active()))

    async def act() -> None:
        async with work:
            await get_session()
            if block_fails:
                raise _Failure()

    # Act
    with suppress(_Failure, _CommitFailed):
        await act()

    # Assert
    assert seen == [("sync", expected, False), ("async", expected, False)]


async def test_activation_keeps_the_exit_hooks_it_started_with(work: AsyncScope):
    """
    An exit hook registered or removed mid-activation should apply from the next activation.

    Given: An active scope with one exit hook
    When: That hook is removed and another registered, then a second activation runs
    Then: The first activation runs only the original hook, and the second only the new one
    """
    # Arrange
    events: list[str] = []

    def original(activation: AsyncActivation, error: BaseException | None) -> None:
        events.append("original")

    work.on_exit(original)

    # Act
    async with work:
        work.remove_hook(original)
        work.on_exit(lambda activation, error: events.append("added"))
    async with work:
        pass

    # Assert
    assert events == ["original", "added"]


async def test_success_callbacks_run_after_a_commit(work: AsyncScope):
    """
    Sync and async success callbacks should run after the commit, before the exit hooks.

    Given: A resource that commits, an exit hook, and a sync and an async callback
    When: The block runs cleanly
    Then: The callbacks run in order after the commit, then the exit hook
    """
    # Arrange
    events: list[str] = []
    get_session = _session(work, events)
    work.on_exit(lambda activation, error: events.append("exit"))

    async def notify() -> None:
        await asyncio.sleep(0)
        events.append("async")

    # Act
    async with work:
        await get_session()
        work.after_success(lambda: events.append("sync"))
        work.after_success(notify)

    # Assert
    assert events == ["open", "commit", "sync", "async", "exit"]


async def test_success_callbacks_run_without_hooks(work: AsyncScope):
    """
    A success callback should run on a scope that has no hooks.

    Given: A scope without hooks, and a callback registered in the block
    When: The block runs cleanly
    Then: The callback runs
    """
    # Arrange
    events: list[str] = []

    # Act
    async with work:
        work.after_success(lambda: events.append("callback"))

    # Assert
    assert events == ["callback"]


async def test_success_callbacks_skipped_when_the_commit_fails(work: AsyncScope):
    """
    Success callbacks shouldn't run when a resource's cleanup fails.

    Given: A resource whose commit fails, and a callback registered in the block
    When: The block runs
    Then: The commit's error propagates, and the callback never runs
    """
    # Arrange
    events: list[str] = []
    get_session = _session(work, events, commit_fails=True)

    async def act() -> None:
        async with work:
            await get_session()
            work.after_success(lambda: events.append("callback"))

    # Act
    with pytest.raises(_CommitFailed):
        await act()

    # Assert
    assert "callback" not in events


async def test_raising_async_hooks_are_logged(work: AsyncScope, caplog: pytest.LogCaptureFixture):
    """
    An async exit hook or success callback that raises should be logged.

    Given: An async callback and an async exit hook that both raise, then a recording hook
    When: The block runs cleanly
    Then: Both failures are logged, and the recording hook still runs
    """
    # Arrange
    seen: list[BaseException | None] = []

    async def broken_callback() -> None:
        await asyncio.sleep(0)
        raise _Failure()

    @work.on_exit
    async def broken_hook(activation: AsyncActivation, error: BaseException | None) -> None:
        await asyncio.sleep(0)
        raise _Failure()

    work.on_exit(lambda activation, error: seen.append(error))

    # Act
    with caplog.at_level(logging.ERROR, logger="stratae.lifecycle"):
        async with work:
            work.after_success(broken_callback)

    # Assert
    assert seen == [None]
    assert "success callback" in caplog.text
    assert "exit hook" in caplog.text


async def test_activate_returns_the_hooks_token(work: AsyncScope):
    """
    activate() should return the token the hooks get, and run them on activate and deactivate.

    Given: An async start hook, an exit hook, and a callback registered after activate()
    When: The scope is activated, then deactivated
    Then: The hooks get the returned token, and run in order with the callback
    """
    # Arrange
    events: list[str] = []
    tokens: list[AsyncActivation] = []

    @work.on_enter
    async def entered(activation: AsyncActivation) -> None:
        await asyncio.sleep(0)
        tokens.append(activation)
        events.append("enter")

    work.on_exit(lambda activation, error: (tokens.append(activation), events.append("exit")))

    # Act
    activation = await work.activate()
    work.after_success(lambda: events.append("success"))
    await work.deactivate(activation)

    # Assert
    assert events == ["enter", "success", "exit"]
    assert tokens == [activation, activation]
