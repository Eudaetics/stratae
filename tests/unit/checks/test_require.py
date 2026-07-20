"""Test suite for the require guard decorator."""

from unittest.mock import AsyncMock, Mock, call

import pytest

from stratae.checks import require


def test_require_sync_check_runs_before_sync_function():
    """
    Verify a sync check runs before a sync function.

    Given: A sync check and a sync function decorated with require.
    When: The decorated function is called.
    Then: The check should run before the function body executes.
    """
    # Arrange
    manager = Mock()

    @require(manager.check)
    def fn() -> None:
        manager.fn()

    # Act
    fn()

    # Assert
    assert manager.mock_calls == [call.check(), call.fn()]


def test_require_runs_multiple_checks_in_order():
    """
    Verify multiple checks run in the order given.

    Given: Two sync checks and a sync function decorated with require.
    When: The decorated function is called.
    Then: The checks should run in the order passed to require, before the function.
    """
    # Arrange
    manager = Mock()

    @require(manager.first, manager.second)
    def fn() -> None:
        manager.fn()

    # Act
    fn()

    # Assert
    assert manager.mock_calls == [call.first(), call.second(), call.fn()]


def test_require_discards_check_return_value():
    """
    Verify a check's return value is discarded.

    Given: A check that returns a truthy value and a function decorated with require.
    When: The decorated function is called.
    Then: The function's own return value should be returned, unaffected by the check.
    """
    # Arrange
    check = Mock(return_value="ignored")

    @require(check)
    def fn() -> str:
        return "fn result"

    # Act
    result = fn()

    # Assert
    assert result == "fn result"


def test_require_sync_check_raise_prevents_function_call():
    """
    Verify a raising sync check prevents the wrapped function from running.

    Given: A check that raises and a sync function decorated with require.
    When: The decorated function is called.
    Then: The check's exception should propagate, and the function body should not run.
    """
    # Arrange
    fn_mock = Mock()
    check = Mock(side_effect=PermissionError("denied"))

    @require(check)
    def fn() -> None:
        fn_mock()

    # Act / Assert
    with pytest.raises(PermissionError, match="denied"):
        fn()
    fn_mock.assert_not_called()


def test_require_raise_stops_remaining_checks():
    """
    Verify a raising check prevents later checks from running.

    Given: A raising check followed by another check, on a sync function.
    When: The decorated function is called.
    Then: The exception should propagate, and the later check should not run.
    """
    # Arrange
    later_check = Mock()
    failing_check = Mock(side_effect=PermissionError("denied"))

    @require(failing_check, later_check)
    def fn() -> None: ...

    # Act / Assert
    with pytest.raises(PermissionError, match="denied"):
        fn()
    later_check.assert_not_called()


def test_require_sync_function_rejects_async_check():
    """
    Verify a sync function cannot be decorated with an async check.

    Given: An async check and a sync function.
    When: require is applied to the sync function with the async check.
    Then: A TypeError should be raised at decoration time.
    """
    # Arrange
    async_check = AsyncMock()

    # Act / Assert
    with pytest.raises(TypeError, match="async check"):

        @require(async_check)
        def _() -> None: ...


async def test_require_async_check_runs_before_async_function():
    """
    Verify an async check runs before an async function.

    Given: An async check and an async function decorated with require.
    When: The decorated function is awaited.
    Then: The check should run before the function body executes.
    """
    # Arrange
    manager = Mock()
    check = AsyncMock()
    manager.attach_mock(check, "check")

    @require(check)
    async def fn() -> None:
        manager.fn()

    # Act
    await fn()

    # Assert
    assert manager.mock_calls == [call.check(), call.fn()]


async def test_require_async_function_accepts_mixed_sync_and_async_checks():
    """
    Verify an async function accepts a mix of sync and async checks.

    Given: A sync check and an async check, both attached to an async function.
    When: The decorated function is awaited.
    Then: Both checks should run, in order, before the function body executes.
    """
    # Arrange
    manager = Mock()
    sync_check = Mock()
    async_check = AsyncMock()
    manager.attach_mock(sync_check, "sync_check")
    manager.attach_mock(async_check, "async_check")

    @require(sync_check, async_check)
    async def fn() -> None:
        manager.fn()

    # Act
    await fn()

    # Assert
    assert manager.mock_calls == [call.sync_check(), call.async_check(), call.fn()]


async def test_require_async_check_raise_prevents_function_call():
    """
    Verify a raising async check prevents the wrapped async function from running.

    Given: An async check that raises and an async function decorated with require.
    When: The decorated function is awaited.
    Then: The check's exception should propagate, and the function body should not run.
    """
    # Arrange
    fn_mock = Mock()
    check = AsyncMock(side_effect=PermissionError("denied"))

    @require(check)
    async def fn() -> None:
        fn_mock()

    # Act / Assert
    with pytest.raises(PermissionError, match="denied"):
        await fn()
    fn_mock.assert_not_called()


def test_require_passes_through_args_and_kwargs():
    """
    Verify the wrapped function still receives its original arguments.

    Given: A function with positional and keyword arguments decorated with require.
    When: The decorated function is called with arguments.
    Then: The arguments should reach the original function unchanged.
    """

    # Arrange
    @require(Mock())
    def fn(a: int, *, b: int) -> int:
        return a + b

    # Act
    result = fn(1, b=2)

    # Assert
    assert result == 3


def test_require_with_no_checks_calls_function_directly():
    """
    Verify require with no checks still calls the wrapped function.

    Given: A function decorated with require and no checks given.
    When: The decorated function is called.
    Then: The function should run normally.
    """

    # Arrange
    @require()
    def fn() -> str:
        return "ok"

    # Act / Assert
    assert fn() == "ok"
