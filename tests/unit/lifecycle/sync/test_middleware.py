"""
Unit tests for middleware on a sync Scope.

This test suite verifies the following behaviors:

Scope middleware:
- Each activation enters the middleware in order once the scope is active, and exits it in
  reverse after the activation's resources have closed, with the scope no longer active.
- activate() and deactivate() enter and exit the middleware as `with` does.
- An exit sees the activation's error, or None.
- An exit sees an error a resource's cleanup raised, such as a failed commit.
- A middleware raising on entry refuses the activation: the scope isn't left active, the
  middleware already entered is exited with the error, later middleware isn't entered, and
  the error propagates.
- A refused activation releases its required parent.
- A middleware returning something that isn't a context manager refuses the activation.
- An exit can suppress the activation's error, as an outer `with` statement can.
- An error raised on exit propagates as a resource's cleanup error does: alone after a clean
  activation, or grouped with the activation's error in CleanupErrors.
- middleware= refuses a lone callable, and a class that isn't a sync context manager.
- The middleware is kept as a tuple, given in any sequence.
"""

from collections.abc import Generator
from contextlib import contextmanager, suppress
from typing import Any

import pytest

from stratae.lifecycle.exceptions import (
    CleanupErrors,
    MiddlewareNotEnterableError,
    MiddlewareNotTupleError,
    ScopeActivationError,
)
from stratae.lifecycle.scope import Scope


class _Refused(RuntimeError):
    """A deliberate refusal, raised by a middleware on entry."""

    def __init__(self) -> None:
        super().__init__("refused")


class _Failed(LookupError):
    """A deliberate failure, raised in an activation's block."""

    def __init__(self) -> None:
        super().__init__("failed")


def _recording(log: list[str], name: str):
    """Return a middleware recording its entry, whether the scope was active, and its exit."""

    @contextmanager
    def middleware(scope: Scope) -> Generator[None, None, None]:
        log.append(f"enter {name} active={scope.is_active()}")
        try:
            yield
        except BaseException as error:
            log.append(f"exit {name} {type(error).__name__} active={scope.is_active()}")
            raise
        log.append(f"exit {name} None active={scope.is_active()}")

    return middleware


def _resource(scope: Scope, log: list[str]):
    """Return a cached resource that records when it's entered and closed."""

    @scope.cache()
    def resource() -> Generator[str, None, None]:
        log.append("resource entered")
        try:
            yield "resource"
        finally:
            log.append("resource closed")

    return resource


def test_middleware_wraps_each_activation_in_order():
    """
    Middleware should be entered in order and exited in reverse, around the resources.

    Given: A scope with two recording middleware and a resource
    When: The scope is activated, the resource used, and the scope deactivated
    Then: Both enter in order with the scope active, the resource closes first, and both
        exit in reverse with the scope no longer active
    """
    # Arrange
    log: list[str] = []
    scope = Scope("work", middleware=(_recording(log, "outer"), _recording(log, "inner")))
    resource = _resource(scope, log)

    # Act
    with scope:
        resource()

    # Assert
    assert log == [
        "enter outer active=True",
        "enter inner active=True",
        "resource entered",
        "resource closed",
        "exit inner None active=False",
        "exit outer None active=False",
    ]


def test_middleware_can_use_cached_functions_on_entry():
    """
    A middleware entering an activation should be able to call the scope's cached functions.

    Given: A scope whose middleware calls a cached function on entry
    When: The scope is activated and the function called again in its block
    Then: The block gets the value the middleware cached
    """
    # Arrange
    made: list[int] = []

    @contextmanager
    def warm(scope: Scope) -> Generator[None, None, None]:
        made.append(cached())
        yield

    scope = Scope("work", middleware=(warm,))

    @scope.cache()
    def cached() -> int:
        return len(made) + 1

    # Act
    with scope:
        seen = cached()

    # Assert
    assert made == [1]
    assert seen == 1


def test_activate_and_deactivate_run_middleware():
    """
    activate() and deactivate() should enter and exit the middleware as `with` does.

    Given: A scope with a recording middleware
    When: It's activated with activate() and deactivated with the token
    Then: The middleware is entered by activate() and exited by deactivate()
    """
    # Arrange
    log: list[str] = []
    scope = Scope("work", middleware=(_recording(log, "middleware"),))

    # Act
    activation = scope.activate()
    entered = list(log)
    scope.deactivate(activation)

    # Assert
    assert entered == ["enter middleware active=True"]
    assert log == ["enter middleware active=True", "exit middleware None active=False"]


def test_exit_sees_activation_error():
    """
    A middleware's exit should see the error the activation ended with.

    Given: A scope with a recording middleware
    When: Its block raises
    Then: The middleware's exit sees the error, which propagates
    """
    # Arrange
    log: list[str] = []
    scope = Scope("work", middleware=(_recording(log, "middleware"),))

    # Act
    with pytest.raises(_Failed), scope:
        raise _Failed

    # Assert
    assert log[-1] == "exit middleware _Failed active=False"


def test_exit_sees_failed_commit():
    """
    A middleware's exit should see an error raised by a resource's cleanup.

    Given: A scope with a recording middleware, and a resource whose cleanup raises, as a
        failed commit would
    When: The block uses the resource and ends cleanly
    Then: The middleware's exit sees the cleanup's error, which propagates
    """
    # Arrange
    log: list[str] = []
    scope = Scope("work", middleware=(_recording(log, "middleware"),))

    @scope.cache()
    def transaction() -> Generator[str, None, None]:
        yield "transaction"
        raise _Failed

    # Act
    with pytest.raises(_Failed), scope:
        transaction()

    # Assert
    assert log[-1] == "exit middleware _Failed active=False"


def test_refusing_middleware_ends_activation():
    """
    A middleware raising on entry should refuse the activation.

    Given: A scope whose first middleware records, second uses a resource then refuses, and
        third records
    When: The scope is activated
    Then: The error propagates, the scope isn't active, the resource is closed, the first
        middleware is exited with the error, and the third is never entered
    """
    # Arrange
    log: list[str] = []

    @contextmanager
    def refuse(scope: Scope) -> Generator[None, None, None]:
        resource()
        raise _Refused
        yield  # unreachable, but makes this a generator

    scope = Scope(
        "work",
        middleware=(_recording(log, "first"), refuse, _recording(log, "third")),
    )
    resource = _resource(scope, log)

    # Act
    with pytest.raises(_Refused), scope:
        log.append("block ran")

    # Assert
    assert not scope.is_active()
    assert log == [
        "enter first active=True",
        "resource entered",
        "resource closed",
        "exit first _Refused active=False",
    ]


def test_refused_activation_releases_parent():
    """
    A refused activation should stop counting against the scope it requires.

    Given: A parent scope, and a child requiring it whose middleware refuses
    When: The child's activation is refused inside the parent
    Then: The parent can still be deactivated
    """

    # Arrange
    @contextmanager
    def refuse(scope: Scope) -> Generator[None, None, None]:
        raise _Refused
        yield  # unreachable, but makes this a generator

    parent = Scope("parent")
    child = Scope("child", requires=parent, middleware=(refuse,))
    activation = parent.activate()

    # Act
    with pytest.raises(_Refused):
        child.activate()

    # Assert
    parent.deactivate(activation)
    assert not parent.is_active()


def test_middleware_not_returning_context_manager_refuses():
    """
    A middleware returning something that isn't a context manager should refuse the activation.

    Given: A scope whose middleware returns a plain value
    When: The scope is activated
    Then: TypeError propagates and the scope isn't active
    """
    # Arrange
    scope = Scope("work", middleware=(lambda scope: "not a context manager",))  # pyright: ignore[reportArgumentType]

    # Act / Assert
    with pytest.raises(TypeError), scope:
        pass
    assert not scope.is_active()


def test_exit_can_suppress_activation_error():
    """
    A middleware's exit should be able to suppress the activation's error, as `with` can.

    Given: A scope whose middleware suppresses the error its block raises
    When: The block raises
    Then: The error doesn't propagate
    """

    # Arrange
    @contextmanager
    def swallow(scope: Scope) -> Generator[None, None, None]:
        with suppress(_Failed):
            yield

    scope = Scope("work", middleware=(swallow,))

    # Act
    with scope:
        raise _Failed

    # Assert
    assert not scope.is_active()


def test_error_raised_on_exit_after_clean_activation_propagates():
    """
    An error a middleware raises on exit should propagate when the activation ended cleanly.

    Given: A scope whose middleware raises on exit
    When: The block ends cleanly
    Then: The middleware's error propagates on its own
    """

    # Arrange
    @contextmanager
    def fail_on_exit(scope: Scope) -> Generator[None, None, None]:
        yield
        raise _Refused

    scope = Scope("work", middleware=(fail_on_exit,))

    # Act / Assert
    with pytest.raises(_Refused), scope:
        pass


def test_error_raised_on_exit_is_grouped_with_activation_error():
    """
    An error a middleware raises on exit should be grouped with the activation's own.

    Middleware is held as the activation's first resources, so its exit errors follow the
    rule for a resource's cleanup: neither error is lost.

    Given: A scope whose middleware raises its own error on exit
    When: The block raises
    Then: CleanupErrors holds the block's error and the middleware's
    """

    # Arrange
    @contextmanager
    def translate(scope: Scope) -> Generator[None, None, None]:
        try:
            yield
        except _Failed as error:
            raise _Refused from error

    scope = Scope("work", middleware=(translate,))

    # Act
    with pytest.raises(CleanupErrors) as raised, scope:
        raise _Failed

    # Assert
    assert {type(error) for error in raised.value.exceptions} == {_Failed, _Refused}


def test_lone_middleware_refused():
    """
    middleware= should refuse a single callable given in place of a tuple.

    Given: A middleware callable
    When: A scope is built with it alone as middleware
    Then: MiddlewareNotTupleError names the scope
    """
    # Act / Assert
    with pytest.raises(MiddlewareNotTupleError, match="'work'"):
        Scope("work", middleware=_recording([], "lone"))  # pyright: ignore[reportArgumentType]


class _AsyncOnly:
    """An async context manager class, which a sync scope can't enter."""

    def __init__(self, scope: Any) -> None:
        self.scope = scope

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_: object) -> None:
        return None


class _NotAManager:
    """A class that isn't a context manager."""

    def __init__(self, scope: Any) -> None:
        self.scope = scope


@pytest.mark.parametrize("middleware", [_AsyncOnly, _NotAManager], ids=["async", "plain"])
def test_unenterable_middleware_class_refused(middleware: type):
    """
    A sync scope should refuse a middleware class it can't enter when it's built.

    Given: An async context manager class, or a class that isn't a context manager
    When: A sync scope is built with it as middleware
    Then: MiddlewareNotEnterableError names the scope and the class
    """
    # Act / Assert
    with pytest.raises(MiddlewareNotEnterableError, match=middleware.__name__):
        Scope("work", middleware=(middleware,))


def test_middleware_kept_as_tuple():
    """
    A scope should keep its middleware as a tuple, given in any sequence.

    Given: Two middleware in a list
    When: A scope is built with them
    Then: Its middleware is a tuple of both, in order
    """
    # Arrange
    first, second = _recording([], "first"), _recording([], "second")

    # Act
    scope = Scope("work", middleware=[first, second])  # pyright: ignore[reportArgumentType]

    # Assert
    assert scope.middleware == (first, second)


def test_scope_without_middleware_has_none():
    """
    A scope built without middleware should report none, and activate as before.

    Given: A scope with no middleware
    When: It's activated and deactivated
    Then: Its middleware is empty, and activation raises nothing
    """
    # Arrange
    scope = Scope("work")

    # Act
    with scope:
        active = scope.is_active()

    # Assert
    assert scope.middleware == ()
    assert active


def test_deactivating_wrong_activation_keeps_middleware_entered():
    """
    A deactivation refused for the wrong activation shouldn't exit the middleware.

    Given: A shared scope with a recording middleware, activated once
    When: An already-deactivated token is passed to deactivate()
    Then: ScopeActivationError is raised, and the live activation's middleware stays entered
    """
    # Arrange
    log: list[str] = []
    scope = Scope("work", "shared", middleware=(_recording(log, "middleware"),))
    stale = scope.activate()
    scope.deactivate(stale)
    live = scope.activate()

    # Act
    with pytest.raises(ScopeActivationError):
        scope.deactivate(stale)

    # Assert
    assert log.count("exit middleware None active=False") == 1
    scope.deactivate(live)
    assert log.count("exit middleware None active=False") == 2
