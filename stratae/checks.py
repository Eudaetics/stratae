"""
Run collections of zero-argument checks, raising or gathering their failures.

A check is a zero-argument callable that returns `None` to pass and raises to fail. A check
that returns anything else is refused with {py:exc}`CheckResultError`, since a check written
as a predicate, such as `lambda: user.is_admin`, would otherwise always pass. An async check
resolves to `None` the same way. {py:func}`check` runs sync
checks, {py:func}`check_async` runs sync or async ones, and {py:func}`require` runs them
before a decorated function. {py:func}`any_of` and {py:func}`all_of` nest groups. The
`mode` keyword controls failure handling:

* `"all"` (default): raise the first failure.
* `"gather"`: run every check, then raise all failures as a {py:exc}`CheckFailures` group.
* `"any"`: pass on the first success; raise all failures if none succeed.


````{example} Using require to check authorization
```{code-block} python
from types import SimpleNamespace
from stratae.checks import all_of, require

user = SimpleNamespace(id=7, is_admin=False)
resource = SimpleNamespace(id=101, owner_id=7, archived=True)

def is_admin():
    assert user.is_admin, "not an admin"

def is_owner():
    assert resource.owner_id == user.id, "not the owner"

def not_archived():
    assert not resource.archived, "resource is archived"

@require(is_admin, all_of(is_owner, not_archived), mode="any")
def delete_resource(resource_id: int):
    print(f"Deleting resource: {resource_id}")

try:
    delete_resource(resource.id)
except ExceptionGroup as exc:
    print("Access denied:", exc.exceptions[-1])
```
```{output}
Access denied: resource is archived
```
````
"""

from collections.abc import Awaitable, Callable, Sequence
from functools import wraps
from inspect import isawaitable
from types import CoroutineType
from typing import Any, Literal, Self, cast, overload

from stratae._callables import is_async_callable

type CheckMode = Literal["all", "gather", "any"]

type _Check = Callable[[], Awaitable[None] | None]
"""A check: returns `None` to pass, or on async paths an awaitable resolving to it."""


class CheckFailures(ExceptionGroup[Exception]):
    """Raised with every failure, in "gather" mode, or "any" mode when no check passes."""

    def __new__(cls, label: str, failures: Sequence[Exception]) -> Self:
        """
        Group the failures of the checks a function ran.

        :param label: The function that ran the checks.
        :param failures: The checks' failures.
        """
        return super().__new__(cls, f"Failures in {label}", failures)

    def __init__(self, label: str, failures: Sequence[Exception]) -> None:
        """Group the failures of the checks a function ran."""
        super().__init__(f"Failures in {label}", failures)


class CheckResultError(TypeError):
    """
    Raised when a check returns, or resolves to, something other than `None`.

    A check passes by returning `None` and fails by raising, so a returned value is ignored.
    A check returning `False` would pass, so any value is refused instead.
    """

    def __init__(self, fn: Callable[[], object], result: object) -> None:
        """
        Name the check and what it returned.

        :param fn: The check.
        :param result: What it returned, or resolved to.
        """
        super().__init__(
            f"{fn!r} returned {result!r}; a check returns None to pass and raises to fail"
        )


class AwaitableCheckError(TypeError):
    """Raised when {py:func}`check` runs a check that returns an awaitable."""

    def __init__(self, fn: Callable[[], object]) -> None:
        """:param fn: The check."""
        super().__init__(f"{fn!r} returned an awaitable; use check_async() instead.")


class AsyncCheckOnSyncError(TypeError):
    """Raised when {py:func}`require` is given async checks for a sync function."""

    def __init__(self, fn: Callable[..., Any], count: int) -> None:
        """
        Describe the sync function and its async checks.

        :param fn: The sync function.
        :param count: How many of its checks are async.
        """
        super().__init__(
            f"require() got {count} async check(s) but {fn.__name__!r} is sync; "
            "an async check cannot run from inside a sync function"
        )


def _settle(exc: Exception, mode: CheckMode, exceptions: list[Exception]):
    """Record exc for "gather" or "any" mode, or raise it for "all"."""
    if mode == "all":
        raise exc
    exceptions.append(exc)


def _raise_gathered(exceptions: list[Exception], label: str) -> None:
    """Raise the gathered exceptions as an ExceptionGroup, if there are any."""
    if exceptions:
        raise CheckFailures(label, exceptions)


def check(*checks: _Check, mode: CheckMode = "all"):
    """
    Run sync checks in order.

    :param checks: Zero-argument sync callables returning `None` to pass, raising to fail.
    :param mode: `"all"`, `"gather"`, or `"any"`; see the module docs.
    :raises Exception: The first failure, in "all" mode.
    :raises AwaitableCheckError: If a check returns an awaitable; use {py:func}`check_async`.
    :raises CheckResultError: If a check returns anything else but `None`, whatever the mode.
    :raises CheckFailures: All failures, in "gather" mode, or "any" mode if every check fails.
    """
    exceptions: list[Exception] = []
    for fn in checks:
        try:
            result = fn()
        except Exception as exc:
            _settle(exc, mode, exceptions)
            continue
        if result is not None:
            if isawaitable(result):
                getattr(result, "close", lambda: None)()
                raise AwaitableCheckError(fn)
            raise CheckResultError(fn, result)
        if mode == "any":
            return
    _raise_gathered(exceptions, "check")


async def check_async(*checks: _Check, mode: CheckMode = "all"):
    """
    Run sync or async checks in order, awaiting the async ones.

    :param checks: Zero-argument callables returning `None` to pass, or an awaitable resolving
        to it, and raising to fail.
    :param mode: `"all"`, `"gather"`, or `"any"`; see the module docs.
    :raises Exception: The first failure, in "all" mode.
    :raises CheckResultError: If a check returns, or resolves to, anything else but `None`,
        whatever the mode.
    :raises CheckFailures: All failures, in "gather" mode, or "any" mode if every check fails.
    """
    exceptions: list[Exception] = []
    for fn in checks:
        try:
            result: object = fn()
            # None and coroutines skip isawaitable, which costs several times either test.
            if result is not None and (isinstance(result, CoroutineType) or isawaitable(result)):
                result = await result
        except Exception as exc:
            _settle(exc, mode, exceptions)
            continue
        if result is not None:
            raise CheckResultError(fn, result)
        if mode == "any":
            return
    _raise_gathered(exceptions, "check_async")


def any_of(*checks: _Check) -> _Check:
    """
    Combine checks into one that passes if any of them does.

    The result is async if any check is async, and then needs {py:func}`check_async`.

    :param checks: Zero-argument callables to try in order.
    :returns: A check raising {py:exc}`CheckFailures` with all failures if none pass.
    """
    if any(is_async_callable(fn) for fn in checks):

        async def _any_of_async() -> None:
            await check_async(*checks, mode="any")

        return _any_of_async

    def _any_of_sync() -> None:
        check(*checks, mode="any")

    return _any_of_sync


def all_of(*checks: _Check) -> _Check:
    """
    Combine checks into one that passes only if all of them do.

    The result is async if any check is async, and then needs {py:func}`check_async`.

    :param checks: Zero-argument callables to run in order.
    :returns: A check raising the first failure.
    """
    if any(is_async_callable(fn) for fn in checks):

        async def _all_of_async() -> None:
            await check_async(*checks, mode="all")

        return _all_of_async

    def _all_of_sync() -> None:
        check(*checks, mode="all")

    return _all_of_sync


def _wrap_async[**P, R](
    fn: Callable[P, Awaitable[R]], checks: tuple[_Check, ...], mode: CheckMode
) -> Callable[P, Awaitable[R]]:
    @wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        await check_async(*checks, mode=mode)
        return await fn(*args, **kwargs)

    return wrapper


def _wrap_sync[**P, R](
    fn: Callable[P, R], checks: tuple[_Check, ...], mode: CheckMode
) -> Callable[P, R]:
    async_checks = [guard for guard in checks if is_async_callable(guard)]
    if async_checks:
        raise AsyncCheckOnSyncError(fn, len(async_checks))

    @wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        check(*checks, mode=mode)
        return fn(*args, **kwargs)

    return wrapper


def require[**P, R](
    *checks: _Check, mode: CheckMode = "all"
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Run checks before the decorated function.

    A sync function accepts only sync checks. An async function accepts both. With no
    checks, the function is returned unchanged.

    :param P: Parameter specification of the decorated function.
    :param R: Return type of the decorated function.
    :param checks: Zero-argument callables to run before the function.
    :param mode: `"all"`, `"gather"`, or `"any"`; see the module docs. The function is only
        called if the checks pass.
    :returns: A decorator preserving the function's signature.
    :raises AsyncCheckOnSyncError: At decoration, if a sync function is given an async check.
    """

    @overload
    def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]: ...
    @overload
    def decorator(fn: Callable[P, R]) -> Callable[P, R]: ...
    def decorator(
        fn: Callable[P, R] | Callable[P, Awaitable[R]],
    ) -> Callable[P, R] | Callable[P, Awaitable[R]]:
        if not checks:
            return fn
        if is_async_callable(fn):
            return _wrap_async(fn, checks, mode)
        return cast(Callable[P, R], _wrap_sync(fn, checks, mode))

    return decorator
