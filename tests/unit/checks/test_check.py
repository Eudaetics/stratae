"""Test suite for check functions used to help guard/validate."""

from unittest.mock import AsyncMock, Mock, call

import pytest

from stratae.checks import check, check_async


def test_check_runs_all_functions():
    """
    `check` should run every function when no function raises.

    Given: A set of zero-argument callable checks
    When: `check` is called over the checks and none raise
    Then: `check` should not raise.
    """
    # Arrange
    fn = Mock()

    # Act
    check(fn.first, fn.second, fn.third)

    # Assert
    assert fn.mock_calls == [call.first(), call.second(), call.third()]


def test_check_stops_on_failure():
    """
    `check` should stop at the first raising check.

    Given: A failing check followed by another check
    When: `check` is called and the first check raises
    Then: The exception should propagate and the second check should not run.
    """
    # Arrange
    failing_check = Mock(side_effect=ValueError("broken"))
    later_check = Mock()

    # Act / Assert
    with pytest.raises(ValueError, match="broken"):
        check(failing_check, later_check)
    later_check.assert_not_called()


def test_check_raises_on_async():
    """
    `check` raises when using a function that returns an awaitable.

    Given: An async check.
    When: `check` is called with the async check.
    Then: `check` should raise TypeError instead of running it.
    """
    with pytest.raises(TypeError, match="returned an awaitable"):
        check(AsyncMock())


def test_check_gathers_errors():
    """
    `check` runs all functions when errors="gather" regardless of errors.

    Given: The check function called with errors="gather",
    When: Multiple checks raise,
    Then: All functions are ran and an ExceptionGroup with all errors is raised.
    """
    # Arrange
    fail1 = Mock(side_effect=ValueError("broken 1"))
    fail2 = Mock(side_effect=TypeError("broken 2"))
    good = Mock()

    # Act
    with pytest.raises(ExceptionGroup) as exc_info:
        check(fail1, fail2, good, mode="gather")

    # Assert
    assert len(exc_info.value.exceptions) == 2
    assert "broken 1" in str(exc_info.value.exceptions[0])
    assert "broken 2" in str(exc_info.value.exceptions[1])
    fail1.assert_called_once()
    fail2.assert_called_once()
    good.assert_called_once()


def test_check_any_returns_first_success():
    """
    `check` immediately returns on first success when errors="any".

    Given: `check` is called with errors="any",
    When: A check doesn't raise,
    Then: `check` immediately returns without raising.
    """
    # Arrange
    fn = Mock()

    # Act
    check(fn.first, fn.second, fn.third, mode="any")

    # Assert
    assert fn.mock_calls == [call.first()]


def test_check_any_swallows_errors_if_one_succeeds():
    """
    `check` returns on first success when errors="any" and swallows errors.

    Given: `check` is called with errors="any",
    When: Some called checks fail, but a later check doesn't raise,
    Then: `check` returns without raising.
    """
    # Arrange
    fn = Mock()
    fn.fail = Mock(side_effect=ValueError("broken"))

    # Act
    check(fn.fail, fn.first, fn.second, fn.third, mode="any")

    # Assert
    assert fn.mock_calls == [call.fail(), call.first()]


async def test_check_async_runs_all_functions():
    """
    `check_async` should run every function when no function raises.

    Given: A set of zero-argument callable checks
    When: `check_async` is called over the checks and none raise
    Then: `check_async` should not raise.
    """
    # Arrange
    fn = AsyncMock()

    # Act
    await check_async(fn.first, fn.second, fn.third)

    # Assert
    assert fn.mock_calls == [call.first(), call.second(), call.third()]


async def test_check_async_stops_on_failure():
    """
    `check_async` should stop at the first raising check.

    Given: A failing check followed by another check
    When: `check_async` is called and the first check raises
    Then: The exception should propagate and the second check should not run.
    """
    # Arrange
    failing_check = Mock(side_effect=ValueError("broken"))
    later_check = Mock()

    # Act / Assert
    with pytest.raises(ValueError, match="broken"):
        await check_async(failing_check, later_check)
    later_check.assert_not_called()


async def test_check_async_gathers_errors():
    """
    `check_async` runs all functions when errors="gather" regardless of errors.

    Given: The check function called with errors="gather",
    When: Multiple checks raise,
    Then: All functions are ran and an ExceptionGroup with all errors is raised.
    """
    # Arrange
    fail1 = AsyncMock(side_effect=ValueError("broken 1"))
    fail2 = Mock(side_effect=TypeError("broken 2"))
    good = Mock()

    # Act
    with pytest.raises(ExceptionGroup) as exc_info:
        await check_async(fail1, fail2, good, mode="gather")

    # Assert
    assert len(exc_info.value.exceptions) == 2
    assert "broken 1" in str(exc_info.value.exceptions[0])
    assert "broken 2" in str(exc_info.value.exceptions[1])
    fail1.assert_called_once()
    fail2.assert_called_once()
    good.assert_called_once()


async def test_check_async_any_returns_first_success():
    """
    `check_async` immediately returns on first success when errors="any".

    Given: `check_async` is called with errors="any",
    When: A check doesn't raise,
    Then: check_async immediately returns without raising.
    """
    # Arrange
    fn = Mock()

    # Act
    await check_async(fn.first, fn.second, fn.third, mode="any")

    # Assert
    assert fn.mock_calls == [call.first()]


async def test_check_async_any_swallows_errors_if_one_succeeds():
    """
    `check_async` returns on first success when errors="any" and swallows errors.

    Given: `check_async` is called with errors="any",
    When: Some called checks fail, but a later check doesn't raise,
    Then: `check_async` returns without raising.
    """
    # Arrange
    fn = Mock()
    fn.fail = Mock(side_effect=ValueError("broken"))

    # Act
    await check_async(fn.fail, fn.first, fn.second, fn.third, mode="any")

    # Assert
    assert fn.mock_calls == [call.fail(), call.first()]
