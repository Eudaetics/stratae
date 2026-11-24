"""Test suite for making checks from predicates."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import assert_type

import pytest

from stratae.callables import is_async_callable
from stratae.checks import (
    CheckFailures,
    CheckResultError,
    any_of,
    check,
    check_async,
    require,
    that,
)


class Denied(Exception):
    """A failure with a reason and a code."""

    def __init__(self, reason: str, *, code: int) -> None:
        """Keep the reason as the message, and the code beside it."""
        super().__init__(reason)
        self.code = code


async def _true() -> bool:
    await asyncio.sleep(0)
    return True


async def _false() -> bool:
    await asyncio.sleep(0)
    return False


def test_that_passes_when_the_predicate_holds():
    """
    A check made from a true predicate passes.

    Given: A predicate returning True.
    When: Its check runs.
    Then: Nothing should be raised.
    """
    # Act / Assert
    check(that(lambda: True, PermissionError, "not an admin"))


def test_that_raises_the_error_when_the_predicate_is_false():
    """
    A check made from a false predicate raises the error built from its arguments.

    Given: A predicate returning False, an error class, a reason and a keyword argument.
    When: Its check runs.
    Then: The error should be built from the arguments and raised.
    """
    # Arrange
    guard = that(lambda: False, Denied, "not an admin", code=403)

    # Act / Assert
    with pytest.raises(Denied, match="not an admin") as caught:
        check(guard)
    assert caught.value.code == 403


def test_that_raises_a_new_error_on_each_failure():
    """
    Each failure raises an error of its own.

    Given: A check made from a false predicate.
    When: It runs twice.
    Then: Each run should raise a different error object.
    """
    # Arrange
    guard = that(lambda: False, PermissionError)
    raised: list[Exception] = []

    # Act
    for _ in range(2):
        try:
            check(guard)
        except PermissionError as exc:
            raised.append(exc)

    # Assert
    assert len(raised) == 2
    assert raised[0] is not raised[1]


@pytest.mark.parametrize(
    ("predicate", "fails"),
    [pytest.param(_true, False, id="true"), pytest.param(_false, True, id="false")],
)
async def test_that_awaits_an_async_predicate(
    predicate: Callable[[], Awaitable[bool]], fails: bool
):
    """
    A check made from an async predicate is async, and fails when it resolves to False.

    Given: An async predicate.
    When: Its check runs through check_async.
    Then: The error should be raised only if the predicate resolves to False.
    """
    # Arrange
    guard = that(predicate, PermissionError, "denied")

    # Act / Assert
    assert is_async_callable(guard)
    if fails:
        with pytest.raises(PermissionError, match="denied"):
            await check_async(guard)
    else:
        await check_async(guard)


def test_that_guards_a_function_under_require():
    """
    A check made from a predicate guards a function like any other check.

    Given: A function under require with a check made from a false predicate.
    When: The function is called.
    Then: The error should be raised, and the function shouldn't run.
    """
    # Arrange
    ran: list[int] = []

    @require(that(lambda: False, PermissionError, "not an admin"))
    def delete(account_id: int) -> None:
        ran.append(account_id)

    # Act / Assert
    with pytest.raises(PermissionError, match="not an admin"):
        delete(24)
    assert ran == []


async def test_that_composes_with_any_of():
    """
    Checks made from predicates combine with any_of.

    Given: any_of over a false sync predicate's check and a false async predicate's check.
    When: The combined check runs.
    Then: Both errors should be raised together.
    """
    # Arrange
    guard = any_of(that(lambda: False, PermissionError), that(_false, LookupError))

    # Act / Assert
    with pytest.raises(CheckFailures) as caught:
        await check_async(guard)
    assert [type(exc) for exc in caught.value.exceptions] == [PermissionError, LookupError]


def test_that_is_typed_by_its_predicate():
    """
    A sync predicate gives a sync check, and an async one gives an async check.

    Given: A sync predicate and an async one.
    When: Each is made into a check.
    Then: The checks should be typed sync and async.
    """
    # Act / Assert
    assert_type(that(lambda: True, PermissionError), Callable[[], None])
    assert_type(that(_true, Denied, "reason", code=1), Callable[[], Awaitable[None]])


def test_a_refused_predicate_points_to_that():
    """
    A check returning a value is refused with a message pointing to `that`.

    Given: A predicate passed as a check.
    When: It runs.
    Then: CheckResultError should say to wrap the predicate with `that`.
    """
    # Act / Assert
    with pytest.raises(CheckResultError, match=r"wrap it: that\(predicate, error\)"):
        check(lambda: False)  # pyright: ignore[reportArgumentType]
