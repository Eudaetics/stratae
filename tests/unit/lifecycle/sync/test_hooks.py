"""
Unit tests for a sync Scope's start hooks, exit hooks and success callbacks.

This test suite verifies the following behaviors:

- Start hooks run in order once the scope is active, so they can use its cached functions.
- Start and exit hooks get the activation's token, and share values through its state;
  activate() returns that same token.
- A start hook that raises refuses the activation: what was entered closes with its error,
  the later start hooks don't run, the exit hooks see the error and the partial state, a
  parent scope is released, and the error propagates.
- Exit hooks run in order after every resource has exited, with the scope inactive. They
  see the block's error, a cleanup's error, or None, including when an error is suppressed.
- Each activation runs the exit hooks registered when it started; a hook registered or
  removed while it runs takes effect from the next activation.
- remove_hook removes a start or exit hook, including a bound method, and ignores one that
  isn't registered.
- An exit hook or success callback that raises is logged, and the rest still run.
- Success callbacks run only after a clean exit, before the exit hooks.
- A sync scope refuses async hooks and callbacks.
"""

import logging
from collections.abc import Generator
from contextlib import suppress

import pytest

from stratae.lifecycle import Scope, resource
from stratae.lifecycle.exceptions import AsyncHookError, NoActivationError
from stratae.lifecycle.scope import Activation


class _Failure(Exception):
    """A deliberate failure, raised by the test."""


class _CommitFailed(Exception):
    """A failed commit, raised by a resource's cleanup."""


def _refuse(activation: Activation) -> None:
    """Refuse the activation, as a start hook."""
    raise _Failure()


def _kind(error: BaseException | None) -> type[BaseException] | None:
    """Return an error's type, or None for no error."""
    return type(error) if error else None


@pytest.fixture(params=["dense", "sparse"])
def work(request: pytest.FixtureRequest) -> Scope:
    """Provide a context scope, with each kind of storage."""
    return Scope("work", storage=request.param)


def _session(work: Scope, events: list[str], *, commit_fails: bool = False):
    """Register a cached resource that records its entry and how it exited."""

    @work.cache()
    @resource
    def get_session() -> Generator[str, None, None]:
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


def test_start_hooks_run_in_order_inside_the_activation(work: Scope):
    """
    Start hooks should run in order, once the scope is active.

    Given: Two start hooks, the second using a cached resource of the scope
    When: The scope is activated
    Then: Both run in order, and the resource they entered is the block's
    """
    # Arrange
    events: list[str] = []
    get_session = _session(work, events)
    work.on_enter(lambda activation: events.append("first"))
    work.on_enter(lambda activation: events.append(f"second {get_session()}"))

    # Act
    with work:
        events.append(f"block {get_session()}")

    # Assert
    assert events == ["first", "open", "second session", "block session", "commit"]


def test_hooks_share_the_activations_state(work: Scope):
    """
    A start hook and an exit hook should share values through the activation's state.

    Given: A start hook that stores a value in the activation's state, and an exit hook
    When: Two activations run
    Then: The exit hook reads each activation's own value
    """
    # Arrange
    seen: list[object] = []
    counter = iter(range(1, 3))
    work.on_enter(lambda activation: activation.state.update(number=next(counter)))
    work.on_exit(lambda activation, error: seen.append(activation.state["number"]))

    # Act
    with work:
        pass
    with work:
        pass

    # Assert
    assert seen == [1, 2]


def test_activate_returns_the_hooks_token(work: Scope):
    """
    activate() should return the token the hooks get, and run them on activate and deactivate.

    Given: A start hook, an exit hook, and a callback registered after activate()
    When: The scope is activated, then deactivated
    Then: The hooks get the returned token, and run in order with the callback
    """
    # Arrange
    events: list[str] = []
    tokens: list[Activation] = []
    work.on_enter(lambda activation: (tokens.append(activation), events.append("enter")))
    work.on_exit(lambda activation, error: (tokens.append(activation), events.append("exit")))

    # Act
    activation = work.activate()
    work.after_success(lambda: events.append("success"))
    work.deactivate(activation)

    # Assert
    assert events == ["enter", "success", "exit"]
    assert tokens == [activation, activation]


def test_raising_start_hook_refuses_the_activation(work: Scope):
    """
    A start hook that raises should refuse the activation, closing what was entered.

    Given: A start hook that enters a resource and marks the state, one that raises, one
        after it, and an exit hook
    When: The scope is activated
    Then: The block and the later start hook never run, the resource rolls back, the exit
        hook sees the error and only the first hook's state, and the scope is inactive
    """
    # Arrange
    events: list[str] = []
    get_session = _session(work, events)
    seen: list[tuple[type[BaseException] | None, dict[str, object]]] = []

    @work.on_enter
    def open_session(activation: Activation) -> None:
        get_session()
        activation.state["session"] = True

    work.on_enter(_refuse)
    work.on_enter(lambda activation: activation.state.update(never=True))
    work.on_exit(lambda activation, error: seen.append((_kind(error), dict(activation.state))))

    # Act
    with pytest.raises(_Failure), work:
        events.append("block")

    # Assert
    assert events == ["open", "rollback _Failure"]
    assert seen == [(_Failure, {"session": True})]
    assert not work.is_active()


def test_refused_activation_releases_its_parent():
    """
    A refused activation should stop counting against the scope it requires.

    Given: A child scope requiring an active parent, with a start hook that raises
    When: The child is activated, then the parent deactivated
    Then: The child's activation raises, and the parent deactivates cleanly
    """
    # Arrange
    parent = Scope("parent")
    child = Scope("child", requires=parent)
    child.on_enter(_refuse)

    # Act
    activation = parent.activate()
    with pytest.raises(_Failure):
        child.activate()

    # Assert
    assert parent.deactivate(activation) is False


def test_refused_activate_raises(work: Scope):
    """
    activate() should raise, leaving the scope inactive, when a start hook refuses.

    Given: A start hook that raises
    When: activate() is called
    Then: The error propagates, and the scope isn't active
    """
    # Arrange
    work.on_enter(_refuse)

    # Act & Assert
    with pytest.raises(_Failure):
        work.activate()
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
def test_exit_hooks_see_the_activations_outcome(
    work: Scope, block_fails: bool, commit_fails: bool, expected: type[BaseException] | None
):
    """
    Exit hooks should run after every resource has exited, with how the activation ended.

    Given: A resource whose commit may fail, and two exit hooks
    When: The block runs cleanly, raises, or the commit fails
    Then: Both hooks run in order after the resource exits, with the scope inactive, and see
        None, the block's error, or the commit's error
    """
    # Arrange
    get_session = _session(work, [], commit_fails=commit_fails)
    seen: list[tuple[str, type[BaseException] | None, bool]] = []
    work.on_exit(lambda activation, error: seen.append(("first", _kind(error), work.is_active())))
    work.on_exit(lambda activation, error: seen.append(("second", _kind(error), work.is_active())))

    def act() -> None:
        with work:
            get_session()
            if block_fails:
                raise _Failure()

    # Act
    with suppress(_Failure, _CommitFailed):
        act()

    # Assert
    assert seen == [("first", expected, False), ("second", expected, False)]


def test_suppressed_error_counts_as_a_clean_exit(work: Scope):
    """
    An error a resource suppresses should leave the activation ending cleanly.

    Given: A resource that suppresses errors, an exit hook and a success callback
    When: The block raises
    Then: The error is suppressed, the callback runs, and the exit hook sees None
    """
    # Arrange
    seen: list[object] = []

    @work.cache()
    @resource
    def get_forgiving() -> Generator[None, None, None]:
        with suppress(_Failure):
            yield

    work.on_exit(lambda activation, error: seen.append(error))

    # Act
    with work:
        get_forgiving()
        work.after_success(lambda: seen.append("succeeded"))
        raise _Failure()

    # Assert
    assert seen == ["succeeded", None]


def test_activation_keeps_the_exit_hooks_it_started_with(work: Scope):
    """
    An exit hook registered or removed mid-activation should apply from the next activation.

    Given: An active scope with one exit hook
    When: That hook is removed and another registered, then a second activation runs
    Then: The first activation runs only the original hook, and the second only the new one
    """
    # Arrange
    events: list[str] = []

    def original(activation: Activation, error: BaseException | None) -> None:
        events.append("original")

    work.on_exit(original)

    # Act
    with work:
        work.remove_hook(original)
        work.on_exit(lambda activation, error: events.append("added"))
    with work:
        pass

    # Assert
    assert events == ["original", "added"]


def test_exit_hook_registered_mid_activation_skips_it(work: Scope):
    """
    An exit hook registered while an activation without hooks runs should skip that one.

    Given: An active scope with no hooks
    When: An exit hook is registered, then the activation ends and another runs
    Then: The hook runs only for the second activation
    """
    # Arrange
    events: list[str] = []

    # Act
    with work:
        work.on_exit(lambda activation, error: events.append("exit"))
    with work:
        pass

    # Assert
    assert events == ["exit"]


def test_remove_hook_removes_start_and_exit_hooks(work: Scope):
    """
    remove_hook should remove a start or exit hook, including a bound method.

    Given: A start hook, and a bound method registered as an exit hook
    When: Both are removed, the method through a new reference, and the scope is activated
    Then: Neither runs, and removing a hook that isn't registered does nothing
    """
    # Arrange
    events: list[str] = []

    class Recorder:
        def finish(self, activation: Activation, error: BaseException | None) -> None:
            events.append("exit")

    def start(activation: Activation) -> None:
        events.append("enter")

    recorder = Recorder()
    work.on_enter(start)
    work.on_exit(recorder.finish)

    # Act
    work.remove_hook(start)
    work.remove_hook(recorder.finish)
    work.remove_hook(start)
    with work:
        pass

    # Assert
    assert events == []


def test_raising_exit_hook_is_logged(work: Scope, caplog: pytest.LogCaptureFixture):
    """
    An exit hook that raises should be logged, without changing how the activation ended.

    Given: An exit hook that raises, then one that records
    When: The block runs cleanly
    Then: The failure is logged, the second hook still runs, and nothing propagates
    """
    # Arrange
    seen: list[BaseException | None] = []

    @work.on_exit
    def broken(activation: Activation, error: BaseException | None) -> None:
        raise _Failure()

    work.on_exit(lambda activation, error: seen.append(error))

    # Act
    with caplog.at_level(logging.ERROR, logger="stratae.lifecycle"), work:
        pass

    # Assert
    assert seen == [None]
    assert "exit hook" in caplog.text
    assert "for scope 'work' failed" in caplog.text


def test_success_callbacks_run_after_a_commit(work: Scope):
    """
    Success callbacks should run after every resource has exited, before the exit hooks.

    Given: A resource that commits, an exit hook, and two callbacks registered in the block
    When: The block runs cleanly
    Then: The callbacks run in order after the commit, then the exit hook
    """
    # Arrange
    events: list[str] = []
    get_session = _session(work, events)
    work.on_exit(lambda activation, error: events.append("exit"))

    # Act
    with work:
        get_session()
        work.after_success(lambda: events.append("first"))
        work.after_success(lambda: events.append("second"))

    # Assert
    assert events == ["open", "commit", "first", "second", "exit"]


def test_success_callbacks_run_without_hooks(work: Scope):
    """
    A success callback should run on a scope that has no hooks.

    Given: A scope without hooks, and a callback registered in the block
    When: The block runs cleanly
    Then: The callback runs
    """
    # Arrange
    events: list[str] = []

    # Act
    with work:
        work.after_success(lambda: events.append("callback"))

    # Assert
    assert events == ["callback"]


@pytest.mark.parametrize("commit_fails", [False, True], ids=["block raises", "commit fails"])
def test_success_callbacks_skipped_when_the_activation_fails(work: Scope, commit_fails: bool):
    """
    Success callbacks shouldn't run when the activation ends with an error.

    Given: A callback registered in the block
    When: The block raises, or the commit fails
    Then: The callback never runs, and the callbacks don't carry over to the next activation
    """
    # Arrange
    events: list[str] = []
    get_session = _session(work, events, commit_fails=commit_fails)

    def act() -> None:
        with work:
            get_session()
            work.after_success(lambda: events.append("callback"))
            if not commit_fails:
                raise _Failure()

    # Act
    with pytest.raises((_Failure, _CommitFailed)):
        act()
    with work:
        pass

    # Assert
    assert "callback" not in events


def test_raising_success_callback_is_logged(work: Scope, caplog: pytest.LogCaptureFixture):
    """
    A success callback that raises should be logged, and the others should still run.

    Given: A callback that raises, then one that records
    When: The block runs cleanly
    Then: The failure is logged, and the second callback still runs
    """
    # Arrange
    events: list[str] = []

    def broken() -> None:
        raise _Failure()

    # Act
    with caplog.at_level(logging.ERROR, logger="stratae.lifecycle"), work:
        work.after_success(broken)
        work.after_success(lambda: events.append("second"))

    # Assert
    assert events == ["second"]
    assert "success callback" in caplog.text


def test_after_success_needs_an_active_scope(work: Scope):
    """
    after_success should refuse a callback when the scope isn't active.

    Given: A scope that isn't active
    When: A callback is registered
    Then: NoActivationError is raised
    """
    # Act & Assert
    with pytest.raises(NoActivationError):
        work.after_success(lambda: None)


def test_decorators_return_the_hook(work: Scope):
    """
    on_enter and on_exit should return the hook they register, so they can decorate it.

    Given: A start hook and an exit hook
    When: They're registered as decorators
    Then: The names still refer to the functions
    """

    # Arrange
    def start(activation: Activation) -> None: ...

    def finish(activation: Activation, error: BaseException | None) -> None: ...

    # Act & Assert
    assert work.on_enter(start) is start
    assert work.on_exit(finish) is finish


@pytest.mark.parametrize("register", ["on_enter", "on_exit", "after_success"])
def test_sync_scope_refuses_async_hooks(work: Scope, register: str):
    """
    A sync scope should refuse an async hook or callback, which it can't await.

    Given: An async function
    When: It's registered as a start hook, exit hook or success callback
    Then: AsyncHookError is raised
    """

    # Arrange
    async def hook(*_: object) -> None: ...

    # Act & Assert
    with pytest.raises(AsyncHookError, match="use an AsyncScope"), work:
        getattr(work, register)(hook)
