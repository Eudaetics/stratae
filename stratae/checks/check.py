"""
Run collections of zero-argument checks, raising or gathering their failures.

A check is any zero-argument callable that raises on failure and returns
normally on success. `check` runs a sequence of sync checks, raising
``TypeError`` if any check returns an awaitable; `check_async` runs a
sequence that may mix sync and async checks, awaiting the async ones.

Both accept an `errors` keyword controlling failure handling:

* ``"raise"`` (default): stop at the first failing check and propagate its
  exception.
* ``"gather"``: run every check regardless of earlier failures, then raise
  an :class:`ExceptionGroup` containing all of them.

See :func:`check` and :func:`check_async` for examples.
"""

from inspect import isawaitable
from typing import Any, Callable, Literal

CheckMode = Literal["all", "gather", "any"]


def _settle(exc: Exception, mode: CheckMode, exceptions: list[Exception]):
    """Append to the exception list if errors="gather"."""
    if mode == "all":
        raise exc
    exceptions.append(exc)


def _raise_gathered(exceptions: list[Exception], label: str) -> None:
    """Raise the gathered exceptions as an ExceptionGroup, if there are any."""
    if exceptions:
        raise ExceptionGroup(f"Failures in {label}", exceptions)


def check(*checks: Callable[[], Any], mode: CheckMode = "all"):
    """
    Run each check in order, triggering side effects or raising errors.

    Args:
        checks: Zero-argument, synchronous callables to run in order. Each
            should raise on failure. If a check returns an awaitable (for
            example, it's a coroutine function), it's rejected with
            ``TypeError`` rather than run; use :func:`check_async` for
            checks that may be async.
        mode: How to handle a failing check. "all" stops at the first
            failure and propagates its exception. "gather" runs every
            check regardless of earlier failures and raises them together
            at the end. "any" returns immediately upon first successful
            check, swallowing errors from any preceding check.

    Raises:
        Exception: The exception raised by the first failing check, when
            errors is "raise".
        TypeError: A check returned an awaitable instead of running
            synchronously. Checks earlier in the sequence have already
            run by this point.
        ExceptionGroup: All exceptions raised by failing checks, when
            errors is "gather", or "any" when all checks fail.

    Example:
        Validate a batch of form fields, stopping at the first failure::

            username = "sam"
            email = "not-an-email"

            def check_username_not_empty():
                assert username.strip(), "username must not be empty"

            def check_email_has_at_sign():
                assert "@" in email, "email must contain '@'"

            check(check_username_not_empty, check_email_has_at_sign)
            # raises AssertionError: email must contain '@'

        Collect every failing validation instead of stopping at the first::

            check(check_username_not_empty, check_email_has_at_sign, errors="gather")
            # raises ExceptionGroup: Failures in check (1 sub-exception)

    """
    exceptions: list[Exception] = []
    for fn in checks:
        try:
            result = fn()
            if isawaitable(result):
                getattr(result, "close", lambda: None)()
                raise TypeError(f"{fn!r} returned an awaitable; use check_async() instead.")
            if mode == "any":
                return
        except Exception as exc:
            _settle(exc, mode, exceptions)
    _raise_gathered(exceptions, "check")


async def check_async(*checks: Callable[[], Any], mode: CheckMode = "all"):
    """
    Run each check in order, awaiting async checks and calling sync ones directly.

    Args:
        checks: Zero-argument callables to run in order. Each should raise
            on failure. May be sync or async.
        mode: How to handle a failing check. "all" stops at the first
            failure and propagates its exception. "gather" runs every
            check regardless of earlier failures and raises them together
            at the end. "any" returns immediately upon first successful
            check, swallowing errors from any preceding check.

    Raises:
        Exception: The exception raised by the first failing check, when
            errors is "all".
        ExceptionGroup: All exceptions raised by failing checks, when
            errors is "gather", or "any" when all checks fail.

    Example:
        Mix a local format check with a remote uniqueness check::

            username = "sam"

            def check_username_not_empty():
                assert username.strip(), "username must not be empty"

            async def check_username_available():
                assert not await is_username_taken(username)

            await check_async(check_username_not_empty, check_username_available)

    """
    exceptions: list[Exception] = []
    for fn in checks:
        try:
            result = fn()
            if isawaitable(result):
                await result
            if mode == "any":
                return
        except Exception as exc:
            _settle(exc, mode, exceptions)
    _raise_gathered(exceptions, "check_async")
