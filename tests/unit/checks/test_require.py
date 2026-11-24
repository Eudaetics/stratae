"""Test suite for the require guard decorator."""

import asyncio
import re
from collections.abc import Callable
from functools import partial
from unittest.mock import AsyncMock, Mock, call

import pytest

from stratae.checks import (
    AsyncCheckOnSyncError,
    CheckFailures,
    CheckMode,
    CheckResultError,
    check_async,
    require,
)


def _passing[M: Mock](mock: M, *children: str) -> M:
    """Make `mock` and its named children passing checks: calling one returns `None`."""
    mock.return_value = None
    for name in children:
        getattr(mock, name).return_value = None
    return mock


def test_require_sync_check_runs_before_sync_function():
    """
    Verify a sync check runs before a sync function.

    Given: A sync check and a sync function decorated with require.
    When: The decorated function is called.
    Then: The check should run before the function body executes.
    """
    # Arrange
    manager = _passing(Mock(), "check", "first", "second")

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
    manager = _passing(Mock(), "check", "first", "second")

    @require(manager.first, manager.second)
    def fn() -> None:
        manager.fn()

    # Act
    fn()

    # Assert
    assert manager.mock_calls == [call.first(), call.second(), call.fn()]


def test_require_refuses_check_returning_value():
    """
    Verify a check returning a value is refused, and the function doesn't run.

    Given: A check that returns a value, and a function decorated with require.
    When: The decorated function is called.
    Then: CheckResultError should be raised, and the function body shouldn't run.
    """
    # Arrange
    body = Mock()

    def check() -> str:
        return "ignored"

    @require(check)  # pyright: ignore[reportArgumentType]
    def fn() -> None:
        body()

    # Act / Assert
    with pytest.raises(CheckResultError, match="returned 'ignored'"):
        fn()
    body.assert_not_called()


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
    later_check = Mock(return_value=None)
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
    manager = _passing(Mock(), "check", "first", "second")
    check = AsyncMock(return_value=None)
    manager.attach_mock(check, "check")

    @require(check)
    async def fn() -> None:
        await asyncio.sleep(0)
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
    manager = _passing(Mock(), "check", "first", "second")
    sync_check = Mock(return_value=None)
    async_check = AsyncMock(return_value=None)
    manager.attach_mock(sync_check, "sync_check")
    manager.attach_mock(async_check, "async_check")

    @require(sync_check, async_check)
    async def fn() -> None:
        await asyncio.sleep(0)
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
        await asyncio.sleep(0)
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
    @require(Mock(return_value=None))
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


def test_require_sync_function_rejects_async_callable_object_check():
    """
    A sync function rejects a check that's an object with async __call__.

    Given: A check that's an instance whose __call__ is async.
    When: require applies it to a sync function.
    Then: A TypeError should be raised at decoration time.
    """

    # Arrange
    class Check:
        async def __call__(self) -> None: ...

    # Act / Assert
    with pytest.raises(TypeError, match="async check"):

        @require(Check())
        def handler() -> None: ...


def test_require_any_mode_without_checks_calls_function():
    """
    Verify require with mode="any" and no checks calls the function: none of them raised.

    Given: A function decorated with require, mode="any", and no checks.
    When: The function is called.
    Then: It runs and returns its value.
    """

    # Arrange
    @require(mode="any")
    def handler() -> int:
        return 1

    # Act & Assert
    assert handler() == 1


def _sync_function(value: int) -> int:
    return value


class _SyncCallable:
    def __call__(self) -> None: ...


@pytest.mark.parametrize(
    "fn",
    [
        pytest.param(_sync_function, id="function"),
        pytest.param(partial(_sync_function, 1), id="partial"),
        pytest.param(_SyncCallable(), id="callable object"),
    ],
)
def test_require_names_any_sync_callable_it_refuses(fn: Callable[..., object]):
    """
    The refusal of an async check names the sync callable, however it's made.

    Given: A sync callable that may have no `__name__`.
    When: require applies an async check to it.
    Then: AsyncCheckOnSyncError should be raised, naming the callable.
    """
    # Arrange
    async_check = AsyncMock()

    # Act / Assert
    with pytest.raises(AsyncCheckOnSyncError, match=re.escape(repr(fn))):
        require(async_check)(fn)


def _returns_value() -> str:
    return "ignored"


async def _resolves_to_value() -> str:
    await asyncio.sleep(0)
    return "ignored"


@pytest.mark.parametrize(
    "check",
    [
        pytest.param(_returns_value, id="sync"),
        pytest.param(_resolves_to_value, id="async"),
    ],
)
async def test_require_async_function_refuses_check_returning_value(
    check: Callable[[], object],
):
    """
    An async function refuses a sync or async check that gives a value.

    Given: A check returning, or resolving to, a value, before an async function.
    When: The decorated function is awaited.
    Then: CheckResultError should be raised, and the function body shouldn't run.
    """
    # Arrange
    body = Mock()

    @require(check)  # pyright: ignore[reportArgumentType]
    async def fn() -> None:
        await asyncio.sleep(0)
        body()

    # Act / Assert
    with pytest.raises(CheckResultError, match="returned 'ignored'"):
        await fn()
    body.assert_not_called()


async def test_require_async_function_awaits_sync_check_returning_awaitable():
    """
    An async function awaits the awaitable a sync check returns.

    Given: A sync lambda returning an async check's coroutine, before an async function.
    When: The decorated function is awaited.
    Then: The coroutine should be awaited before the function body runs.
    """
    # Arrange
    manager = Mock()
    async_check = AsyncMock(return_value=None)
    manager.attach_mock(async_check, "async_check")

    @require(lambda: async_check())
    async def fn() -> None:
        await asyncio.sleep(0)
        manager.fn()

    # Act
    await fn()

    # Assert
    assert manager.mock_calls == [call.async_check(), call.fn()]
    async_check.assert_awaited_once()


async def test_require_async_function_raise_stops_remaining_checks():
    """
    A raising async check stops the checks after it.

    Given: A raising async check, then a sync check, before an async function.
    When: The decorated function is awaited.
    Then: The error should propagate, and neither the later check nor the body should run.
    """
    # Arrange
    later = Mock(return_value=None)
    body = Mock()

    @require(AsyncMock(side_effect=PermissionError("denied")), later)
    async def fn() -> None:
        await asyncio.sleep(0)
        body()

    # Act / Assert
    with pytest.raises(PermissionError, match="denied"):
        await fn()
    later.assert_not_called()
    body.assert_not_called()


def _denied() -> None:
    raise PermissionError("denied")


async def _denied_async() -> None:
    await asyncio.sleep(0)
    raise LookupError("missing")


def _sync_target() -> str:
    return "ran"


async def _async_target() -> str:
    await asyncio.sleep(0)
    return "ran"


async def _call(fn: Callable[[], object]) -> object:
    result = fn()
    return await result if asyncio.iscoroutine(result) else result


@pytest.mark.parametrize(
    ("target", "checks", "raised"),
    [
        pytest.param(
            _sync_target, (_denied, _denied), [PermissionError, PermissionError], id="sync"
        ),
        pytest.param(
            _async_target, (_denied, _denied_async), [PermissionError, LookupError], id="async"
        ),
    ],
)
async def test_require_gather_mode_raises_every_failure(
    target: Callable[[], object],
    checks: tuple[Callable[[], object], ...],
    raised: list[type[Exception]],
):
    """
    In "gather" mode, every check runs and all failures are raised together.

    Given: Two failing checks before a function, in "gather" mode.
    When: The decorated function is called.
    Then: CheckFailures should hold both failures, in order.
    """
    # Arrange
    fn = require(*checks, mode="gather")(target)  # pyright: ignore[reportArgumentType]

    # Act / Assert
    with pytest.raises(CheckFailures) as caught:
        await _call(fn)
    assert [type(exc) for exc in caught.value.exceptions] == raised


@pytest.mark.parametrize(
    ("target", "check"),
    [
        pytest.param(_sync_target, Mock(return_value=None), id="sync"),
        pytest.param(_async_target, AsyncMock(return_value=None), id="async"),
    ],
)
async def test_require_any_mode_runs_function_after_a_passing_check(
    target: Callable[[], object], check: Callable[[], object]
):
    """
    In "any" mode, one passing check lets the function run.

    Given: A failing check then a passing one before a function, in "any" mode.
    When: The decorated function is called.
    Then: The function should run and return its result.
    """
    # Arrange
    fn = require(_denied, check, mode="any")(target)  # pyright: ignore[reportArgumentType]

    # Act
    result = await _call(fn)

    # Assert
    assert result == "ran"


type _Log = list[str]


def _make_check(kind: str, log: _Log) -> Callable[[], object]:
    def sync() -> object:
        log.append(kind)
        if kind.startswith("fail"):
            raise PermissionError(kind)
        return "value" if kind.startswith("value") else None

    async def resolve() -> object:
        await asyncio.sleep(0)
        return sync()

    if kind.endswith("async"):
        return resolve
    if kind == "awaitable":
        return lambda: resolve()
    return sync


def _outcome(exc: BaseException | None) -> object:
    if exc is None:
        return None
    if isinstance(exc, CheckFailures):
        return (type(exc), exc.message, [_outcome(inner) for inner in exc.exceptions])
    return (type(exc), re.sub(r" at 0x[0-9a-f]+", "", str(exc)))


async def _through_check_async(kinds: tuple[str, ...], mode: CheckMode) -> tuple[object, _Log]:
    log: _Log = []
    try:
        await check_async(*[_make_check(kind, log) for kind in kinds], mode=mode)  # pyright: ignore[reportArgumentType]
    except Exception as exc:
        return _outcome(exc), log
    return None, log


async def _through_require(kinds: tuple[str, ...], mode: CheckMode) -> tuple[object, _Log]:
    log: _Log = []

    async def body() -> None:
        await asyncio.sleep(0)
        log.append("body")

    checks = [_make_check(kind, log) for kind in kinds]
    try:
        await require(*checks, mode=mode)(body)()  # pyright: ignore[reportArgumentType]
    except Exception as exc:
        return _outcome(exc), log
    assert log[-1] == "body"
    return None, log[:-1]


_CHECK_RUNS = [
    ("pass", "pass async"),
    ("fail", "pass async", "pass"),
    ("pass", "fail async", "fail"),
    ("value",),
    ("value async", "pass"),
    ("awaitable", "pass"),
    ("fail", "fail async"),
]


@pytest.mark.parametrize("mode", ["all", "gather", "any"])
@pytest.mark.parametrize("kinds", _CHECK_RUNS, ids=", ".join)
async def test_require_runs_checks_as_check_async_does(kinds: tuple[str, ...], mode: CheckMode):
    """
    An async function's checks run exactly as check_async runs them.

    The async wrapper keeps its own copy of check_async's loop, so this pins the two together.

    Given: Passing, failing, value-giving and awaitable-returning checks, in each mode.
    When: They run through check_async, and through require before an async function.
    Then: The same checks should run, with the same outcome, and the body only on a pass.
    """
    # Act
    expected = await _through_check_async(kinds, mode)
    actual = await _through_require(kinds, mode)

    # Assert
    assert actual == expected
