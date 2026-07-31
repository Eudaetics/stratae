"""Test suite for synchronous generator (@resource) registration and cleanup via Scope.cache."""

from unittest.mock import Mock

import pytest

from stratae.lifecycle.exceptions import ScopeInactiveError
from stratae.lifecycle.resource import resource
from stratae.lifecycle.scope import Scope


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
    @resource
    def sample_generator():
        try:
            yield SimpleObject()
            mock_commit()
        finally:
            mock_cleanup()

    # Act
    with scope.activate():
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
    @resource
    def sample_generator():
        yield "test"

    # Act & Assert
    with pytest.raises(ScopeInactiveError, match="Scope 'application' is not active."):
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
    @resource
    def sample_generator():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup()
            raise ValueError("Test Failure")

    # Act & Assert
    with pytest.raises(ValueError, match="Test Failure"):  # noqa: S5778
        with scope.activate():
            sample_generator()
            mock_cleanup.assert_not_called()


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
    @resource
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
    with pytest.raises(ValueError, match="Test Failure"):  # noqa: S5778
        with scope.activate():
            sample_generator()
            mock_cleanup.assert_not_called()
            mock_failure.assert_not_called()
            mock_except.assert_not_called()

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
    @resource
    def generator_one():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @scope.cache()
    @resource
    def generator_two():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @scope.cache()
    @resource
    def generator_three():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act
    with scope.activate():
        generator_one()
        generator_two()
        generator_three()

    # Assert
    assert cleanup_order == ["third", "second", "first"]
    mock_cleanup_1.assert_called_once()
    mock_cleanup_2.assert_called_once()
    mock_cleanup_3.assert_called_once()


def test_generator_sync_exception_group(scope: Scope):
    """
    Test that multiple exceptions during generator cleanup are collected into an ExceptionGroup.

    Given: A Scope and multiple cached generator functions that raise exceptions,
    When: The generators are cleaned up,
    Then: An ExceptionGroup containing all exceptions should be raised.
    """
    # Arrange
    mock_cleanup_1 = Mock(side_effect=ValueError("First Failure"))
    mock_cleanup_2 = Mock(side_effect=KeyError("Second Failure"))
    mock_cleanup_3 = Mock(side_effect=RuntimeError("Third Failure"))

    class SimpleObject: ...

    @scope.cache()
    @resource
    def generator_one():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_1()

    @scope.cache()
    @resource
    def generator_two():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_2()

    @scope.cache()
    @resource
    def generator_three():
        try:
            yield SimpleObject()
        finally:
            mock_cleanup_3()

    # Act & Assert
    with pytest.raises(ExceptionGroup) as exceptions:  # noqa: S5778
        with scope.activate():
            generator_one()
            generator_two()
            generator_three()

    assert len(exceptions.value.exceptions) == 3
    assert any(
        isinstance(exc, ValueError) and str(exc) == "First Failure"
        for exc in exceptions.value.exceptions
    )
    assert any(
        isinstance(exc, KeyError) and str(exc) == "'Second Failure'"
        for exc in exceptions.value.exceptions
    )
    assert any(
        isinstance(exc, RuntimeError) and str(exc) == "Third Failure"
        for exc in exceptions.value.exceptions
    )
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
    @resource
    def sample_generator():
        try:
            yield SimpleObject()
            mock_commit()
        except Exception:
            mock_rollback()
            raise

    # Act & Assert
    with pytest.raises(ValueError, match="Caller Failure"):  # noqa: S5778
        with scope.activate():
            sample_generator()
            raise ValueError("Caller Failure")

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
    @resource
    def sample_generator():
        try:
            yield SimpleObject()
        except ValueError:
            mock_suppressed()

    # Act
    with scope.activate():
        sample_generator()
        raise ValueError("Caller Failure")

    # Assert
    mock_suppressed.assert_called_once()


def test_multiple_generators_each_raise_from_callers_exception(scope: Scope):
    """
    Test that resources each raising their own exception from the caller's are grouped.

    Given: A Scope and two cached generators, each of which raises its own new
        exception while handling whatever it's thrown, rather than re-raising it
        unchanged,
    When: Code inside the active scope's with block raises after both resources have
        been entered,
    Then: An ExceptionGroup containing the caller's original exception plus each
        resource's own exception should be raised, chained in close (LIFO) order.
    """

    # Arrange
    class SimpleObject: ...

    @scope.cache()
    @resource
    def resource_a():
        try:
            yield SimpleObject()
        except Exception:
            raise RuntimeError("A Failure") from None

    @scope.cache()
    @resource
    def resource_b():
        try:
            yield SimpleObject()
        except Exception:
            raise KeyError("B Failure") from None

    # Act & Assert
    with pytest.raises(ExceptionGroup) as exceptions:  # noqa: S5778
        with scope.activate():
            resource_a()
            resource_b()
            raise ValueError("Caller Failure")

    assert len(exceptions.value.exceptions) == 3
    assert any(
        isinstance(exc, ValueError) and str(exc) == "Caller Failure"
        for exc in exceptions.value.exceptions
    )
    assert any(
        isinstance(exc, RuntimeError) and str(exc) == "A Failure"
        for exc in exceptions.value.exceptions
    )
    assert any(
        isinstance(exc, KeyError) and str(exc) == "'B Failure'"
        for exc in exceptions.value.exceptions
    )


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
    @resource
    def resource_a():
        try:
            yield SimpleObject()
            mock_commit_a()
        except Exception:
            mock_rollback_a()
            raise

    @scope.cache()
    @resource
    def resource_b():
        try:
            yield SimpleObject()
        except ValueError:
            mock_swallowed_b()

    # Act
    with scope.activate():
        resource_a()
        resource_b()
        raise ValueError("Caller Failure")

    # Assert
    mock_swallowed_b.assert_called_once()
    mock_commit_a.assert_called_once()
    mock_rollback_a.assert_not_called()
