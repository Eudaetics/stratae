"""
Run collections of zero-argument checks, raising or gathering their failures.

A check is any zero-argument callable that raises on failure and returns
normally on success. `check` runs a sequence of sync checks, raising
``TypeError`` if any check returns an awaitable; `check_async` runs a
sequence that may mix sync and async checks, awaiting the async ones.

Both accept a `mode` keyword controlling failure handling:

* ``"all"`` (default): stop at the first failing check and propagate its
  exception.
* ``"gather"``: run every check regardless of earlier failures, then raise
  an :class:`ExceptionGroup` containing all of them.
* ``"any"``: return immediately on first successful check, ignoring errors
  from earlier checks.

See :func:`check` and :func:`check_async` for examples.
"""

from inspect import isawaitable, iscoroutinefunction
from typing import Any, Callable, Literal

type CheckMode = Literal["all", "gather", "any"]


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

        Succeed as soon as any one of several equivalent checks passes::

            def check_is_admin():
                assert user.is_admin, "not an admin"

            def check_owns_resource():
                assert resource.owner_id == user.id, "not the owner"

            check(check_is_admin, check_owns_resource, mode="any")
            # passes if either check succeeds; raises ExceptionGroup only if both fail

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

        Try a fast local check before falling back to a slower remote one::

            def check_in_local_cache():
                assert username in local_cache, "not cached locally"

            async def check_in_remote_store():
                assert await remote_store.exists(username), "not found remotely"

            await check_async(check_in_local_cache, check_in_remote_store, mode="any")
            # passes if either succeeds; raises ExceptionGroup only if both fail

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


def any_of(*checks: Callable[[], Any]) -> Callable[[], Any]:
    """
    Combine checks into a single deferred check that passes if any one does.

    Lets an "any" group nest inside an "all" group (or another "any"
    group), so boolean requirements like ``(is_admin or is_owner) and
    not_pending`` can be expressed directly::

        check(any_of(is_admin, is_owner), not_pending)

    If any of `checks` is an async function, the returned check is async
    too, and can only be run through :func:`check_async` (directly, or
    nested inside another group that is). If all of `checks` are sync,
    the returned check is sync and works with either :func:`check` or
    :func:`check_async`.

    Args:
        checks: Zero-argument callables to try, in order, until one
            succeeds.

    Returns:
        A single zero-argument callable that succeeds as soon as any of
        `checks` succeeds, and raises an :class:`ExceptionGroup` of all
        their failures if none do. Async if any of `checks` is async,
        sync otherwise.

    Example:
        .. code-block:: python

            check(any_of(is_admin, is_owner), not_pending)

    """
    if any(iscoroutinefunction(fn) for fn in checks):

        async def _any_of_async() -> None:
            await check_async(*checks, mode="any")

        return _any_of_async

    def _any_of_sync() -> None:
        check(*checks, mode="any")

    return _any_of_sync


def all_of(*checks: Callable[[], Any]) -> Callable[[], Any]:
    """
    Combine checks into a single deferred check that passes only if all do.

    Lets an "all" group nest inside an "any" group (or another "all"
    group), so boolean requirements like ``is_super_admin or (is_manager
    and manages_target_user)`` can be expressed as a single check inside
    an outer "any"::

        check(is_super_admin, all_of(is_manager, manages_target_user), mode="any")

    If any of `checks` is an async function, the returned check is async
    too, and can only be run through :func:`check_async` (directly, or
    nested inside another group that is). If all of `checks` are sync,
    the returned check is sync and works with either :func:`check` or
    :func:`check_async`.

    Args:
        checks: Zero-argument callables to run, in order. The first to
            fail aborts the rest.

    Returns:
        A single zero-argument callable that succeeds only if every one
        of `checks` succeeds, and raises the first failure otherwise.
        Async if any of `checks` is async, sync otherwise.

    Example:
        .. code-block:: python

            check(is_super_admin, all_of(is_manager, manages_target_user), mode="any")

    """
    if any(iscoroutinefunction(fn) for fn in checks):

        async def _all_of_async() -> None:
            await check_async(*checks, mode="all")

        return _all_of_async

    def _all_of_sync() -> None:
        check(*checks, mode="all")

    return _all_of_sync
