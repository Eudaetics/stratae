"""Test suite for caching sync generator functions: registration and cleanup."""

import re
from contextlib import suppress
from typing import NoReturn
from unittest.mock import Mock

import pytest

from stratae.lifecycle.exceptions import ScopeInactiveError
from stratae.lifecycle.scope import Scope


class _AFailure(RuntimeError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("A Failure")


class _BFailure(KeyError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("B Failure")


class _CallerFailure(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("Caller Failure")


class _FirstFailure(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("first failure")


class _TestFailure(ValueError):
    """A deliberate failure, raised by the test."""

    def __init__(self) -> None:
        super().__init__("Test Failure")


def test_generator_sync(scope: Scope):
    """
    Test using a synchronous generator using the decorator syntax.

    Given: A Scope and a generator function,
    When: The generator function is cached in that scope,
    Then: The generator should be registered for cleanup in that scope.
    """
    # Arrange
    mock_cleanup = Mock()
    mock_commit = Mock()

    class SimpleObject: ...

    @scope.cache()
    def sample_generator():
        try:
            yield SimpleObject()
            mock_commit()
        finally:
            mock_cleanup()

    # Act
    with scope:
        call_1 = sample_generator()
        call_2 = sample_generator()

        # Assert
        mock_commit.assert_not_called()
        mock_cleanup.assert_not_called()

    assert isinstance(call_1, SimpleObject)
    assert call_1 is call_2
    mock_commit.assert_called_once()
    mock_cleanup.assert_called_once()


def test_decorate_generator_inactive_scope(scope: Scope):
    """
    Test using a synchronous generator with an inactive scope.

    Given: A Scope and a cached generator function,
    When: An attempt is made to use the generator before its scope is active,
    Then: A ScopeInactiveError should be raised.
    """

    # Arrange
    @scope.cache()
    def sample_generator():
        yield "test"

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match=re.escape("Scope 'application' is not active.")):
        sample_generator()


def test_decorate_generator_sync_exception_cleanup(scope: Scope):
    """
    Test using a synchronous generator that raises an exception during cleanup.

    Given: A Scope and a cached generator function,
    When: The decorated function raises an exception in cleanup,
    Then: The exception should be propagated correctly.
    """
    # Arrange
    mock_cleanup = Mock()

    class SimpleObject: ...

    @scope.cache()
    def sample_generator():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup()
            raise _TestFailure()

    # Act & Assert
    def act() -> None:
        with scope:
            sample_generator()
            mock_cleanup.assert_not_called()

    with pytest.raises(ValueError, match="Test Failure"):
        act()


def test_decorator_generator_sync_exception_handling(scope: Scope):
    """
    Test using a synchronous generator with an exception block.

    Given: A Scope and a cached generator function,
    When: The decorated function raises an exception in the try block,
    Then: the exception should be propagated and the cleanup function should be called.
    """
    # Arrange
    mock_cleanup = Mock()
    mock_failure = Mock(side_effect=ValueError("Test Failure"))
    mock_except = Mock()

    class SimpleObject: ...

    @scope.cache()
    def sample_generator():
        try:
            yield SimpleObject()
            mock_failure()
        except ValueError:
            mock_except()
            raise
        finally:
            mock_cleanup()

    # Act & Assert
    def act() -> None:
        with scope:
            sample_generator()
            mock_cleanup.assert_not_called()
            mock_failure.assert_not_called()
            mock_except.assert_not_called()

    with pytest.raises(ValueError, match="Test Failure"):
        act()

    mock_except.assert_called_once()
    mock_cleanup.assert_called_once()


def test_multiple_generators_sync(scope: Scope):
    """
    Test registering multiple synchronous generators for cleanup.

    Given: A Scope and multiple cached generator functions,
    When: The generator functions are cached in that scope,
    Then: All generators should be cleaned up in the correct (LIFO) order.
    """
    # Arrange
    cleanup_order: list[str] = []
    mock_cleanup_1 = Mock(side_effect=lambda: cleanup_order.append("first"))
    mock_cleanup_2 = Mock(side_effect=lambda: cleanup_order.append("second"))
    mock_cleanup_3 = Mock(side_effect=lambda: cleanup_order.append("third"))

    class SimpleObject: ...

    @scope.cache()
    def generator_one():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @scope.cache()
    def generator_two():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @scope.cache()
    def generator_three():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act
    with scope:
        generator_one()
        generator_two()
        generator_three()

    # Assert
    assert cleanup_order == ["third", "second", "first"]
    mock_cleanup_1.assert_called_once()
    mock_cleanup_2.assert_called_once()
    mock_cleanup_3.assert_called_once()


def test_last_cleanup_failure_propagates_with_earlier_ones_chained(scope: Scope):
    """
    Each cleanup failure should replace the one before it, which stays in its context chain.

    Given: A Scope and three cached generator functions whose cleanups each raise
    When: The generators are cleaned up, last entered first
    Then: The first-entered resource's error is raised, with the second's as its context and
        the third's as the second's, and every cleanup runs
    """
    # Arrange
    mock_cleanup_1 = Mock(side_effect=ValueError("First Failure"))
    mock_cleanup_2 = Mock(side_effect=KeyError("Second Failure"))
    mock_cleanup_3 = Mock(side_effect=RuntimeError("Third Failure"))

    class SimpleObject: ...

    @scope.cache()
    def generator_one():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @scope.cache()
    def generator_two():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @scope.cache()
    def generator_three():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act
    def act() -> None:
        with scope:
            generator_one()
            generator_two()
            generator_three()

    with pytest.raises(ValueError, match="First Failure") as raised:
        act()

    # Assert
    second = raised.value.__context__
    assert isinstance(second, KeyError)
    assert isinstance(second.__context__, RuntimeError)
    mock_cleanup_1.assert_called_once()
    mock_cleanup_2.assert_called_once()
    mock_cleanup_3.assert_called_once()


def test_generator_sees_exception_raised_in_caller_block(scope: Scope):
    """
    Test that an exception raised by the caller, not the generator, still reaches it.

    Given: A Scope and a cached generator using try/except around its yield to
        distinguish success from failure,
    When: Code inside the active scope's with block raises after the resource has
        already been entered, rather than the generator's own body raising,
    Then: The exception should be delivered to the generator at its yield point, so
        the except branch runs, and the same exception should still propagate.
    """
    # Arrange
    mock_commit = Mock()
    mock_rollback = Mock()

    class SimpleObject: ...

    @scope.cache()
    def sample_generator():
        try:
            yield SimpleObject()
            mock_commit()
        except Exception:
            mock_rollback()
            raise

    # Act & Assert
    def act() -> None:
        with scope:
            sample_generator()
            raise _CallerFailure()

    with pytest.raises(ValueError, match="Caller Failure"):
        act()

    mock_rollback.assert_called_once()
    mock_commit.assert_not_called()


def test_generator_can_suppress_exception_raised_in_caller_block(scope: Scope):
    """
    Test that a generator can suppress an exception raised in the caller's with block.

    Given: A Scope and a cached generator that catches and swallows, rather than
        re-raising, an exception raised after its yield,
    When: Code inside the active scope's with block raises after the resource has
        already been entered,
    Then: The exception should be suppressed - the with block should exit normally.
    """
    # Arrange
    mock_suppressed = Mock()

    class SimpleObject: ...

    @scope.cache()
    def sample_generator():
        try:
            yield SimpleObject()
        except ValueError:
            mock_suppressed()

    # Act
    with scope:
        sample_generator()
        raise _CallerFailure()

    # Assert
    mock_suppressed.assert_called_once()


def test_each_resource_error_replaces_the_one_before(scope: Scope):
    """
    A resource raising its own error should replace the one it was given, keeping it chained.

    Given: A Scope and two cached generators, each raising its own new error while
        handling whatever it's thrown
    When: Code inside the active scope's with block raises after both are entered
    Then: The first-entered resource's error is raised, with the other's as its context and
        the caller's as that one's
    """

    # Arrange
    class SimpleObject: ...

    @scope.cache()
    def resource_a():
        try:
            yield SimpleObject()
        except Exception:
            raise _AFailure() from None

    @scope.cache()
    def resource_b():
        try:
            yield SimpleObject()
        except Exception:
            raise _BFailure() from None

    # Act
    def act() -> None:
        with scope:
            resource_a()
            resource_b()
            raise _CallerFailure()

    with pytest.raises(_AFailure) as raised:
        act()

    # Assert
    b_failure = raised.value.__context__
    assert isinstance(b_failure, _BFailure)
    assert isinstance(b_failure.__context__, _CallerFailure)


def _fail_unrelated() -> NoReturn:
    raise _TestFailure()


def test_error_raised_after_handling_still_chains_the_one_before(scope: Scope):
    """
    An error raised after a resource finished handling the one it was given should chain it.

    Given: Two cached generators: the later raising its own error from the caller's, and the
        earlier catching that, then raising a new error while handling an unrelated one
    When: Code inside the active scope's with block raises after both are entered
    Then: The earlier resource's error is raised, and following its context chain reaches
        the later resource's error, then the caller's
    """

    # Arrange
    @scope.cache()
    def resource_a():
        with suppress(Exception):
            yield "a"
        try:
            _fail_unrelated()
        except _TestFailure:
            raise _AFailure() from None

    @scope.cache()
    def resource_b():
        try:
            yield "b"
        except Exception:
            raise _BFailure() from None

    # Act
    def act() -> None:
        with scope:
            resource_a()
            resource_b()
            raise _CallerFailure()

    with pytest.raises(_AFailure) as raised:
        act()

    # Assert
    chain: list[type[BaseException]] = []
    error: BaseException | None = raised.value
    while error is not None:
        chain.append(type(error))
        error = error.__context__
    assert chain == [_AFailure, _TestFailure, _BFailure, _CallerFailure]


def test_resource_suppressing_a_later_cleanup_error_suppresses_the_activations(
    scope: Scope,
):
    """
    A resource suppressing the error a later-entered resource raised should end the activation.

    Given: Two cached generators: the later raising its own error from the caller's, and the
        earlier suppressing whatever it's thrown
    When: Code inside the active scope's with block raises after both are entered
    Then: Nothing propagates, and both resources closed
    """
    # Arrange
    closed: list[str] = []

    @scope.cache()
    def resource_a():
        with suppress(Exception):
            yield "a"
        closed.append("a")

    @scope.cache()
    def resource_b():
        try:
            yield "b"
        except Exception:
            closed.append("b")
            raise _BFailure() from None

    # Act
    with scope:
        resource_a()
        resource_b()
        raise _CallerFailure()

    # Assert
    assert closed == ["b", "a"]


def test_resource_reraising_callers_exception_propagates_it(scope: Scope):
    """
    A resource re-raising the caller's exception should make it propagate again.

    Given: A Scope and two cached generators, the later one raising its own new error and
        the earlier one re-raising the caller's exception in place of it
    When: Code inside the active scope's with block raises after both are entered
    Then: The caller's exception is raised, with the later resource's error as its context
    """
    # Arrange
    caller_failure = _CallerFailure()

    class SimpleObject: ...

    @scope.cache()
    def resource_a():
        try:
            yield SimpleObject()
        except Exception:
            raise caller_failure from None

    @scope.cache()
    def resource_b():
        try:
            yield SimpleObject()
        except Exception:
            raise _BFailure() from None

    # Act
    def act() -> None:
        with scope:
            resource_a()
            resource_b()
            raise caller_failure

    with pytest.raises(_CallerFailure) as raised:
        act()

    # Assert
    assert raised.value is caller_failure
    assert isinstance(caller_failure.__context__, _BFailure)


def test_resource_swallowing_exception_hides_it_from_earlier_resources(scope: Scope):
    """
    Test that a resource swallowing the caller's exception hides it from earlier resources.

    Given: A Scope and two cached generators - resource_a entered first, resource_b
        entered second, where resource_b catches and swallows the caller's exception
        instead of re-raising it,
    When: Code inside the active scope's with block raises after both resources have
        been entered,
    Then: resource_b's swallow should run, resource_a should see a clean close (its
        success path, not its except branch) since it closes after resource_b in LIFO
        order, and the exception should not propagate out of the with block at all.
    """
    # Arrange
    mock_commit_a = Mock()
    mock_rollback_a = Mock()
    mock_swallowed_b = Mock()

    class SimpleObject: ...

    @scope.cache()
    def resource_a():
        try:
            yield SimpleObject()
            mock_commit_a()
        except Exception:
            mock_rollback_a()
            raise

    @scope.cache()
    def resource_b():
        try:
            yield SimpleObject()
        except ValueError:
            mock_swallowed_b()

    # Act
    with scope:
        resource_a()
        resource_b()
        raise _CallerFailure()

    # Assert
    mock_swallowed_b.assert_called_once()
    mock_commit_a.assert_called_once()
    mock_rollback_a.assert_not_called()


class _Interrupt(BaseException):
    """A non-Exception error, standing in for cancellation or KeyboardInterrupt."""

    def __init__(self) -> None:
        super().__init__("cancelled during cleanup")


def test_cleanup_raising_non_exception_still_closes_earlier_resources(scope: Scope):
    """
    A cleanup raising a BaseException that isn't an Exception doesn't skip later cleanups.

    Given: Two cached resources, the second-entered of which raises a non-Exception
        BaseException while closing,
    When: The scope deactivates,
    Then: The first resource should still be closed, and the BaseException should propagate.
    """
    # Arrange
    closed: list[str] = []

    @scope.cache()
    def first():
        try:
            yield "first"
        finally:
            closed.append("first")

    @scope.cache()
    def second():
        yield "second"
        raise _Interrupt()

    # Act
    def act() -> None:
        with scope:
            first()
            second()

    with pytest.raises(_Interrupt):
        act()

    # Assert
    assert closed == ["first"]


def test_cleanup_error_replaces_a_base_exception_and_chains_it(scope: Scope):
    """
    An error a cleanup raises while a BaseException propagates should replace it, as `with` does.

    Given: Two cached resources, the later raising a non-Exception BaseException while
        closing, and the earlier raising a ValueError from its cleanup
    When: The scope deactivates
    Then: The ValueError is raised, with the BaseException as its context
    """

    # Arrange
    @scope.cache()
    def first():
        try:
            yield "first"
        finally:
            raise _FirstFailure()

    @scope.cache()
    def second():
        yield "second"
        raise _Interrupt()

    # Act
    def act() -> None:
        with scope:
            first()
            second()

    with pytest.raises(_FirstFailure) as raised:
        act()

    # Assert
    assert isinstance(raised.value.__context__, _Interrupt)
