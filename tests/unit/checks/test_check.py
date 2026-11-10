"""Test suite for check functions used to help guard/validate."""

import asyncio
from collections.abc import Generator
from inspect import iscoroutinefunction
from unittest.mock import AsyncMock, Mock, call

import pytest

from stratae.checks import CheckMode, CheckResultError, all_of, any_of, check, check_async


def _passing[M: Mock](mock: M, *children: str) -> M:
    """Make `mock` and its named children passing checks: calling one returns `None`."""
    mock.return_value = None
    for name in children:
        getattr(mock, name).return_value = None
    return mock


def test_check_runs_all_functions():
    """
    `check` should run every function when no function raises.

    Given: A set of zero-argument callable checks
    When: `check` is called over the checks and none raise
    Then: `check` should not raise.
    """
    # Arrange
    fn = _passing(Mock(), "first", "second", "third")

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
    later_check = Mock(return_value=None)

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
    good = Mock(return_value=None)

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
    fn = _passing(Mock(), "first", "second", "third")

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
    fn = _passing(Mock(), "first", "second", "third")
    fn.fail = Mock(side_effect=ValueError("broken"))

    # Act
    check(fn.fail, fn.first, fn.second, fn.third, mode="any")

    # Assert
    assert fn.mock_calls == [call.fail(), call.first()]


def test_check_any_swallows_errors_if_only_last_succeeds():
    """
    `check` returns on first success when errors="any" and swallows errors.

    Given: `check` is called with errors="any",
    When: Some called checks fail, and only the last check doesn't raise,
    Then: `check` returns without raising.
    """
    # Arrange
    fn = _passing(Mock(), "first", "second", "third")
    fn.fail = Mock(side_effect=ValueError("broken"))

    # Act
    check(fn.fail, fn.first, mode="any")

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
    fn = _passing(AsyncMock(), "first", "second", "third")

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
    later_check = Mock(return_value=None)

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
    good = Mock(return_value=None)

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
    fn = _passing(Mock(), "first", "second", "third")

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
    fn = _passing(Mock(), "first", "second", "third")
    fn.fail = Mock(side_effect=ValueError("broken"))

    # Act
    await check_async(fn.fail, fn.first, fn.second, fn.third, mode="any")

    # Assert
    assert fn.mock_calls == [call.fail(), call.first()]


async def test_check_async_any_swallows_errors_if_only_last_succeeds():
    """
    `check_async` returns on first success when errors="any" and swallows errors.

    Given: `check_async` is called with errors="any",
    When: Some called checks fail, and only the last check doesn't raise,
    Then: `check_async` returns without raising.
    """
    # Arrange
    fn = _passing(Mock(), "first", "second", "third")
    fn.fail = Mock(side_effect=ValueError("broken"))

    # Act
    await check_async(fn.fail, fn.first, mode="any")

    # Assert
    assert fn.mock_calls == [call.fail(), call.first()]


def test_any_of_returns_sync_callable_when_all_checks_are_sync():
    """
    `any_of` returns a sync callable when none of its checks are async.

    Given: Two sync checks.
    When: `any_of` combines them.
    Then: The returned callable should not be a coroutine function.
    """
    # Act
    combined = any_of(Mock(return_value=None), Mock(return_value=None))

    # Assert
    assert not iscoroutinefunction(combined)


def test_any_of_succeeds_if_one_check_succeeds():
    """
    `any_of` returns a check that stops at the first success.

    Given: A failing check followed by a succeeding check.
    When: The combined check returned by `any_of` is called.
    Then: It should not raise, and should stop running after the first success.
    """
    # Arrange
    fn = _passing(Mock(), "first", "second", "third")
    fn.fail = Mock(side_effect=ValueError("broken"))
    combined = any_of(fn.fail, fn.first, fn.second)

    # Act
    combined()

    # Assert
    assert fn.mock_calls == [call.fail(), call.first()]


def test_any_of_raises_exception_group_if_all_checks_fail():
    """
    `any_of` raises an ExceptionGroup when every check fails.

    Given: Two failing checks.
    When: The combined check returned by `any_of` is called.
    Then: An ExceptionGroup containing both failures should be raised.
    """
    # Arrange
    fail1 = Mock(side_effect=ValueError("broken 1"))
    fail2 = Mock(side_effect=TypeError("broken 2"))
    combined = any_of(fail1, fail2)

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exc_info:
        combined()
    assert len(exc_info.value.exceptions) == 2


def test_any_of_nests_inside_check_and_passes_if_group_succeeds():
    """
    `any_of` can nest inside `check` to satisfy one branch of an "all".

    Given: `check(any_of(is_admin, is_owner), not_pending)` where
        `is_admin` fails but `is_owner` and `not_pending` succeed.
    When: `check` is called.
    Then: It should not raise, since the "any" group is satisfied.
    """
    # Arrange
    is_admin = Mock(side_effect=PermissionError("not an admin"))
    is_owner = Mock(return_value=None)
    not_pending = Mock(return_value=None)

    # Act
    check(any_of(is_admin, is_owner), not_pending)

    # Assert
    is_admin.assert_called_once()
    is_owner.assert_called_once()
    not_pending.assert_called_once()


def test_any_of_nests_inside_check_and_fails_if_group_fails():
    """
    `any_of` nested inside `check` still fails the outer check if the group fails.

    Given: `check(any_of(is_admin, is_owner), not_pending)` where both
        `is_admin` and `is_owner` fail.
    When: `check` is called.
    Then: The ExceptionGroup from the failed "any" group should propagate,
        and `not_pending` should not run.
    """
    # Arrange
    is_admin = Mock(side_effect=PermissionError("not an admin"))
    is_owner = Mock(side_effect=PermissionError("not the owner"))
    not_pending = Mock(return_value=None)

    # Act / Assert
    with pytest.raises(ExceptionGroup):
        check(any_of(is_admin, is_owner), not_pending)
    not_pending.assert_not_called()


def test_any_of_returns_async_callable_when_any_check_is_async():
    """
    `any_of` returns an async callable when any of its checks is async.

    Given: A sync check and an async check.
    When: `any_of` combines them.
    Then: The returned callable should be a coroutine function.
    """
    # Act
    combined = any_of(Mock(return_value=None), AsyncMock(return_value=None))

    # Assert
    assert iscoroutinefunction(combined)


async def test_any_of_async_succeeds_via_check_async():
    """
    An async `any_of` group runs through `check_async` and succeeds if any check does.

    Given: A failing sync check and a succeeding async check.
    When: The combined check returned by `any_of` is run via `check_async`.
    Then: It should not raise.
    """
    # Arrange
    fail = Mock(side_effect=ValueError("broken"))
    succeed = AsyncMock(return_value=None)
    combined = any_of(fail, succeed)

    # Act
    await check_async(combined)

    # Assert
    fail.assert_called_once()
    succeed.assert_called_once()


def test_any_of_async_group_raises_type_error_in_sync_check():
    """
    An async `any_of` group cannot run inside a sync `check`.

    Given: `any_of` combining a sync and an async check, which produces an
        async deferred callable.
    When: The combined check is passed to `check` instead of `check_async`.
    Then: `check` should raise TypeError instead of running it.
    """
    # Arrange
    combined = any_of(Mock(return_value=None), AsyncMock(return_value=None))

    # Act / Assert
    with pytest.raises(TypeError, match="returned an awaitable"):
        check(combined)


def test_all_of_returns_sync_callable_when_all_checks_are_sync():
    """
    `all_of` returns a sync callable when none of its checks are async.

    Given: Two sync checks.
    When: `all_of` combines them.
    Then: The returned callable should not be a coroutine function.
    """
    # Act
    combined = all_of(Mock(return_value=None), Mock(return_value=None))

    # Assert
    assert not iscoroutinefunction(combined)


def test_all_of_succeeds_if_all_checks_succeed():
    """
    `all_of` doesn't raise when every one of its checks succeeds.

    Given: Two succeeding checks.
    When: The combined check returned by `all_of` is called.
    Then: Both checks should run and no exception should be raised.
    """
    # Arrange
    fn = _passing(Mock(), "first", "second", "third")
    combined = all_of(fn.first, fn.second)

    # Act
    combined()

    # Assert
    assert fn.mock_calls == [call.first(), call.second()]


def test_all_of_raises_first_failure_and_stops_remaining_checks():
    """
    `all_of` stops and raises at the first failing check.

    Given: A failing check followed by another check.
    When: The combined check returned by `all_of` is called.
    Then: The first check's exception should propagate, and the second check should not run.
    """
    # Arrange
    failing_check = Mock(side_effect=ValueError("broken"))
    later_check = Mock(return_value=None)
    combined = all_of(failing_check, later_check)

    # Act / Assert
    with pytest.raises(ValueError, match="broken"):
        combined()
    later_check.assert_not_called()


def test_all_of_nests_inside_check_any_and_passes_if_group_succeeds():
    """
    `all_of` can nest inside `check(mode="any")` to satisfy the "any" as a unit.

    Given: `check(is_super_admin, all_of(is_manager, manages_target_user),
        mode="any")` where `is_super_admin` fails but `is_manager` and
        `manages_target_user` both succeed.
    When: `check` is called.
    Then: It should not raise, since the "all" group is satisfied.
    """
    # Arrange
    is_super_admin = Mock(side_effect=PermissionError("not a super admin"))
    is_manager = Mock(return_value=None)
    manages_target_user = Mock(return_value=None)

    # Act
    check(is_super_admin, all_of(is_manager, manages_target_user), mode="any")

    # Assert
    is_super_admin.assert_called_once()
    is_manager.assert_called_once()
    manages_target_user.assert_called_once()


def test_all_of_nests_inside_check_any_and_fails_if_group_fails():
    """
    `all_of` nested inside `check(mode="any")` still fails the "any" if the group fails.

    Given: `check(is_super_admin, all_of(is_manager, manages_target_user),
        mode="any")` where `is_super_admin` fails and `manages_target_user`
        (inside the "all" group) also fails.
    When: `check` is called.
    Then: An ExceptionGroup containing both failures should be raised.
    """
    # Arrange
    is_super_admin = Mock(side_effect=PermissionError("not a super admin"))
    is_manager = Mock(return_value=None)
    manages_target_user = Mock(side_effect=ValueError("wrong department"))

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exc_info:
        check(is_super_admin, all_of(is_manager, manages_target_user), mode="any")
    assert len(exc_info.value.exceptions) == 2


def test_all_of_returns_async_callable_when_any_check_is_async():
    """
    `all_of` returns an async callable when any of its checks is async.

    Given: A sync check and an async check.
    When: `all_of` combines them.
    Then: The returned callable should be a coroutine function.
    """
    # Act
    combined = all_of(Mock(return_value=None), AsyncMock(return_value=None))

    # Assert
    assert iscoroutinefunction(combined)


async def test_all_of_async_succeeds_via_check_async():
    """
    An async `all_of` group runs through `check_async` and succeeds if all checks do.

    Given: A succeeding sync check and a succeeding async check.
    When: The combined check returned by `all_of` is run via `check_async`.
    Then: It should not raise.
    """
    # Arrange
    first = Mock(return_value=None)
    second = AsyncMock(return_value=None)
    combined = all_of(first, second)

    # Act
    await check_async(combined)

    # Assert
    first.assert_called_once()
    second.assert_called_once()


def test_all_of_async_group_raises_type_error_in_sync_check():
    """
    An async `all_of` group cannot run inside a sync `check`.

    Given: `all_of` combining a sync and an async check, which produces an
        async deferred callable.
    When: The combined check is passed to `check` instead of `check_async`.
    Then: `check` should raise TypeError instead of running it.
    """
    # Arrange
    combined = all_of(Mock(return_value=None), AsyncMock(return_value=None))

    # Act / Assert
    with pytest.raises(TypeError, match="returned an awaitable"):
        check(combined)


class _AsyncCallableCheck:
    """A check that's an object with async __call__, recording each call."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> None:
        self.calls += 1


async def test_all_of_and_any_of_treat_async_callable_object_as_async():
    """
    `all_of` and `any_of` treat an object with async __call__ as an async check.

    Given: A check that's an instance whose __call__ is async.
    When: `all_of` and `any_of` each combine it, and the results run via `check_async`.
    Then: Both combined checks are coroutine functions, and the check is awaited by each.
    """
    # Arrange
    async_check = _AsyncCallableCheck()

    # Act
    combined = [all_of(async_check), any_of(async_check)]
    for group in combined:
        await check_async(group)

    # Assert
    assert all(iscoroutinefunction(group) for group in combined)
    assert async_check.calls == 2


@pytest.mark.parametrize("mode", ["any", "gather"])
def test_check_async_check_type_error_propagates_whatever_the_mode(mode: CheckMode):
    """
    `check` raises its "returned an awaitable" TypeError even when failures are collected.

    Given: An async check followed by a passing sync check.
    When: `check` runs them in a mode that collects failures.
    Then: The TypeError propagates, rather than counting as a failed check - in "any" mode
        the passing check would otherwise swallow it.
    """

    # Arrange
    async def async_check() -> None: ...

    passing = Mock(return_value=None)

    # Act / Assert
    with pytest.raises(TypeError, match="returned an awaitable"):
        check(async_check, passing, mode=mode)
    passing.assert_not_called()


def test_check_any_mode_without_checks_passes():
    """
    `check` with mode="any" and no checks passes: "any" only fails if every check raises.

    Given: No checks, as when an empty tuple is spread into the call.
    When: `check` is called with mode="any".
    Then: It passes.
    """
    check(mode="any")


async def test_check_async_any_mode_without_checks_passes():
    """
    `check_async` with mode="any" and no checks passes: none of them raised.

    Given: No checks, as when an empty tuple is spread into the call.
    When: `check_async` is called with mode="any".
    Then: It passes.
    """
    await check_async(mode="any")


def test_any_of_without_checks_passes():
    """
    `any_of` with no checks passes: "any" only fails if every check raises.

    Given: No checks, as when an empty tuple is spread into the call.
    When: The group `any_of` builds is run through `check`.
    Then: It passes.
    """
    check(any_of())


def test_all_of_without_checks_passes():
    """
    `all_of` with no checks passes: all of no checks is vacuously true.

    Given: No checks.
    When: The group `all_of` builds is run through `check`.
    Then: It passes.
    """
    check(all_of())


_VALUES = pytest.mark.parametrize(
    ("mode", "result"),
    [("all", True), ("all", False), ("any", 0), ("gather", "")],
    ids=["all-true", "all-false", "any-zero", "gather-empty"],
)


@_VALUES
def test_check_refuses_check_returning_value(mode: CheckMode, result: object):
    """
    `check` should refuse a check returning anything but None, whatever the mode.

    A check written as a predicate, returning False, would otherwise pass.

    Given: A check returning a value, even a falsy one, then a passing check
    When: `check` runs them in any mode
    Then: CheckResultError names the value, rather than counting as a pass or a failure, and
        the later check doesn't run
    """

    # Arrange
    def returns_value() -> object:
        return result

    later = Mock(return_value=None)

    # Act / Assert
    with pytest.raises(CheckResultError, match=f"returned {result!r}"):
        check(returns_value, later, mode=mode)  # pyright: ignore[reportArgumentType]
    later.assert_not_called()


@_VALUES
async def test_check_async_refuses_sync_check_returning_value(mode: CheckMode, result: object):
    """
    `check_async` should refuse a sync check returning anything but None, whatever the mode.

    Given: A sync check returning a value, even a falsy one, then a passing check
    When: `check_async` runs them in any mode
    Then: CheckResultError names the value, and the later check doesn't run
    """

    # Arrange
    def returns_value() -> object:
        return result

    later = Mock(return_value=None)

    # Act / Assert
    with pytest.raises(CheckResultError, match=f"returned {result!r}"):
        await check_async(returns_value, later, mode=mode)  # pyright: ignore[reportArgumentType]
    later.assert_not_called()


@_VALUES
async def test_check_async_refuses_async_check_resolving_to_value(mode: CheckMode, result: object):
    """
    `check_async` should refuse an async check resolving to anything but None.

    Given: An async check resolving to a value, even a falsy one, then a passing check
    When: `check_async` runs them in any mode
    Then: CheckResultError names the value, and the later check doesn't run
    """

    # Arrange
    async def resolves_to_value() -> object:
        await asyncio.sleep(0)
        return result

    later = Mock(return_value=None)

    # Act / Assert
    with pytest.raises(CheckResultError, match=f"returned {result!r}"):
        await check_async(resolves_to_value, later, mode=mode)  # pyright: ignore[reportArgumentType]
    later.assert_not_called()


class _Recorded:
    """An awaitable that isn't a coroutine, recording when it's awaited."""

    def __init__(self, log: list[str]) -> None:
        self.log = log

    def __await__(self) -> Generator[None, None, None]:
        yield
        self.log.append("awaited")


async def test_check_async_awaits_awaitable_from_sync_check():
    """
    `check_async` should await an awaitable a sync check returns, even one that isn't a coroutine.

    Given: A sync check returning an awaitable object, then another check
    When: `check_async` is called
    Then: The awaitable is awaited before the next check runs
    """
    # Arrange
    log: list[str] = []
    second = Mock(side_effect=lambda: log.append("second"))

    # Act
    await check_async(lambda: _Recorded(log), second)

    # Assert
    assert log == ["awaited", "second"]
