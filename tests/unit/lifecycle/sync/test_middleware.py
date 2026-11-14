"""
Unit tests for middleware on a sync Scope.

This test suite verifies the following behaviors:

Scope middleware:
- Each activation runs the middleware in order up to its yield once the scope is active, and
  resumes it in reverse after the activation's resources have closed, with the scope no
  longer active.
- activate() and deactivate() run the middleware as `with` does.
- A middleware sees the activation's error raised at its yield, or nothing.
- A middleware sees an error a resource's cleanup raised, such as a failed commit.
- A middleware raising before its yield refuses the activation: the scope isn't left active,
  the middleware already running sees the error, later middleware isn't run, and the error
  propagates.
- A refused activation releases its required parent.
- A middleware can't swallow the activation's error: trying raises MiddlewareError from it,
  grouped with the error in CleanupErrors as a cleanup error is. A BaseException that isn't
  an Exception, such as KeyboardInterrupt, propagates as it is.
- A middleware must yield exactly once: one that never yields refuses the activation, and
  one that yields again raises ResourceDidNotStopError.
- An error raised after the yield propagates as a resource's cleanup error does: alone after
  a clean activation, or grouped with the activation's error in CleanupErrors.
- middleware= refuses a lone callable, a string, an item that isn't callable, and anything
  but a generator function.
- The middleware is kept as a tuple, given in any sequence.
"""

import asyncio
from collections.abc import AsyncGenerator, Generator
from contextlib import suppress
from typing import Any

import pytest

from stratae.lifecycle.exceptions import (
    CleanupErrors,
    MiddlewareError,
    ResourceDidNotStopError,
    ResourceDidNotYieldError,
    ScopeActivationError,
)
from stratae.lifecycle.scope import Scope
from stratae.middleware import MiddlewareKindError


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


def test_middleware_that_isnt_a_generator_function_refused():
    """
    A middleware that isn't a generator function should be refused when the scope is built.

    Given: A middleware returning a plain value
    When: A scope is built with it
    Then: MiddlewareKindError says the scope's middleware must be a generator function
    """
    # Act / Assert
    with pytest.raises(MiddlewareKindError, match="must be a generator function"):
        Scope("work", middleware=(lambda scope: "not a generator",))  # pyright: ignore[reportArgumentType]


def test_exit_cannot_swallow_activation_error():
    """
    A middleware's exit shouldn't be able to swallow the activation's error.

    Middleware is held as the activation's first resources, so the refusal is grouped with
    the activation's error, as any error an exit raises is.

    Given: A scope whose middleware suppresses the error its block raises
    When: The block raises
    Then: CleanupErrors holds the block's error and a MiddlewareError raised from it, and the
        scope isn't active
    """

    # Arrange
    def swallow(scope: Scope) -> Generator[None, None, None]:
        with suppress(_Failed):
            yield

    scope = Scope("work", middleware=(swallow,))

    # Act
    with pytest.raises(CleanupErrors) as raised, scope:
        raise _Failed

    # Assert
    (failed,) = [error for error in raised.value.exceptions if isinstance(error, _Failed)]
    (refusal,) = [error for error in raised.value.exceptions if isinstance(error, MiddlewareError)]
    assert refusal.__cause__ is failed
    assert not scope.is_active()


class _Interrupt(BaseException):
    """A BaseException that isn't an Exception, as KeyboardInterrupt is."""


def test_swallowing_base_exception_raises_it_again():
    """
    A middleware trying to swallow a BaseException that isn't an Exception should re-raise it.

    Given: A scope whose middleware returns whatever its yield raises
    When: The block raises a BaseException that isn't an Exception
    Then: That error propagates as it is, not as MiddlewareError
    """

    # Arrange
    def swallow_all(scope: Scope) -> Generator[None, None, None]:
        try:
            yield
        except BaseException:
            return

    scope = Scope("work", middleware=(swallow_all,))

    # Act / Assert
    with pytest.raises(_Interrupt), scope:
        raise _Interrupt
    assert not scope.is_active()


def _never_yields(scope: Scope) -> Generator[None, None, None]:
    return
    yield


def _yields_twice(scope: Scope) -> Generator[None, None, None]:
    try:
        yield
    finally:
        yield


def test_middleware_that_never_yields_refuses():
    """
    A middleware that returns without yielding should refuse the activation.

    Given: A scope whose middleware returns before its yield
    When: The scope is activated
    Then: ResourceDidNotYieldError propagates, and the scope isn't active
    """
    # Arrange
    scope = Scope("work", middleware=(_never_yields,))

    # Act / Assert
    with pytest.raises(ResourceDidNotYieldError), scope:
        pass
    assert not scope.is_active()


def test_middleware_yielding_again_after_clean_activation_refused():
    """
    A middleware that yields again after a clean activation should be refused.

    Given: A scope whose middleware yields a second time
    When: The block ends cleanly
    Then: ResourceDidNotStopError is raised, and the scope isn't active
    """
    # Arrange
    scope = Scope("work", middleware=(_yields_twice,))

    # Act / Assert
    with pytest.raises(ResourceDidNotStopError), scope:
        pass
    assert not scope.is_active()


def test_middleware_yielding_again_after_error_refused():
    """
    A middleware that yields again when the block raises should be refused.

    Given: A scope whose middleware yields a second time
    When: The block raises
    Then: CleanupErrors holds the block's error and ResourceDidNotStopError
    """
    # Arrange
    scope = Scope("work", middleware=(_yields_twice,))

    # Act
    with pytest.raises(CleanupErrors) as raised, scope:
        raise _Failed

    # Assert
    assert {type(error) for error in raised.value.exceptions} == {
        ResourceDidNotStopError,
        _Failed,
    }


def test_error_raised_on_exit_after_clean_activation_propagates():
    """
    An error a middleware raises on exit should propagate when the activation ended cleanly.

    Given: A scope whose middleware raises on exit
    When: The block ends cleanly
    Then: The middleware's error propagates on its own
    """

    # Arrange
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
    Then: A TypeError names the scope
    """
    # Act / Assert
    with pytest.raises(TypeError, match="Scope 'work' middleware= takes a tuple of callables"):
        Scope("work", middleware=_recording([], "lone"))  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("middleware", ["trace", b"trace"], ids=["str", "bytes"])
def test_string_middleware_refused(middleware: object):
    """
    middleware= should refuse a string, rather than splitting it into characters.

    Given: A string, or bytes, in place of a tuple
    When: A scope is built with it as middleware
    Then: A TypeError names the scope: a string isn't a tuple, and bytes hold ints
    """
    # Act / Assert
    with pytest.raises(TypeError, match="Scope 'work' middleware="):
        Scope("work", middleware=middleware)  # pyright: ignore[reportArgumentType]


def test_uncallable_middleware_refused():
    """
    middleware= should refuse an item that isn't callable when the scope is built.

    Given: A tuple holding None
    When: A scope is built with it as middleware
    Then: A TypeError names the scope and the item
    """
    # Act / Assert
    with pytest.raises(
        TypeError, match="Scope 'work' middleware= holds None, which isn't callable"
    ):
        Scope("work", middleware=(None,))  # pyright: ignore[reportArgumentType]


async def _async_generator(scope: Any) -> AsyncGenerator[None, None]:
    await asyncio.sleep(0)
    yield


def _plain(scope: Any) -> None:
    return None


@pytest.mark.parametrize(
    "middleware", [_async_generator, _plain], ids=["async-generator", "plain-function"]
)
def test_middleware_of_the_wrong_kind_refused(middleware: Any):
    """
    A sync scope should refuse a middleware that isn't a generator function when it's built.

    Given: An async generator function, or a plain function
    When: A sync scope is built with it as middleware
    Then: MiddlewareKindError names the scope and the middleware
    """
    # Act / Assert
    with pytest.raises(MiddlewareKindError, match=rf"Scope 'work'.*{middleware.__name__}"):
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
