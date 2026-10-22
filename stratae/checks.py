"""
Run collections of zero-argument checks, raising or gathering their failures.

A check is a zero-argument callable that raises on failure. {py:func}`check` runs sync
checks, {py:func}`check_async` runs sync or async ones, and {py:func}`require` runs them
before a decorated function. {py:func}`any_of` and {py:func}`all_of` nest groups. The
`mode` keyword controls failure handling:

* `"all"` (default): raise the first failure.
* `"gather"`: run every check, then raise all failures as an {py:exc}`ExceptionGroup`.
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

from functools import wraps
from inspect import isawaitable
from typing import Any, Awaitable, Callable, Literal, TypeGuard, cast, overload

from stratae._callables import is_async_callable

type CheckMode = Literal["all", "gather", "any"]

_Check = Callable[[], Any]


def _settle(exc: Exception, mode: CheckMode, exceptions: list[Exception]):
    """Record exc for "gather" or "any" mode, or raise it for "all"."""
    if mode == "all":
        raise exc
    exceptions.append(exc)


def _raise_gathered(exceptions: list[Exception], label: str) -> None:
    """Raise the gathered exceptions as an ExceptionGroup, if there are any."""
    if exceptions:
        raise ExceptionGroup(f"Failures in {label}", exceptions)


def check(*checks: Callable[[], Any], mode: CheckMode = "all"):
    """
    Run sync checks in order.

    :param checks: Zero-argument sync callables that raise on failure.
    :param mode: `"all"`, `"gather"`, or `"any"`; see the module docs.
    :raises Exception: The first failure, in "all" mode.
    :raises TypeError: If a check returns an awaitable; use {py:func}`check_async`.
    :raises ExceptionGroup: All failures, in "gather" mode, or "any" mode if every check fails.
    """
    exceptions: list[Exception] = []
    for fn in checks:
        try:
            result = fn()
        except Exception as exc:
            _settle(exc, mode, exceptions)
            continue
        if isawaitable(result):
            getattr(result, "close", lambda: None)()
            raise TypeError(f"{fn!r} returned an awaitable; use check_async() instead.")
        if mode == "any":
            return
    _raise_gathered(exceptions, "check")


async def check_async(*checks: Callable[[], Any], mode: CheckMode = "all"):
    """
    Run sync or async checks in order, awaiting the async ones.

    :param checks: Zero-argument callables that raise on failure.
    :param mode: `"all"`, `"gather"`, or `"any"`; see the module docs.
    :raises Exception: The first failure, in "all" mode.
    :raises ExceptionGroup: All failures, in "gather" mode, or "any" mode if every check fails.
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
    Combine checks into one that passes if any of them does.

    The result is async if any check is async, and then needs {py:func}`check_async`.

    :param checks: Zero-argument callables to try in order.
    :returns: A check raising an {py:exc}`ExceptionGroup` of all failures if none pass.
    """
    if any(is_async_callable(fn) for fn in checks):

        async def _any_of_async() -> None:
            await check_async(*checks, mode="any")

        return _any_of_async

    def _any_of_sync() -> None:
        check(*checks, mode="any")

    return _any_of_sync


def all_of(*checks: Callable[[], Any]) -> Callable[[], Any]:
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


def _is_async_fn[**P, R](
    fn: Callable[P, R] | Callable[P, Awaitable[R]],
) -> TypeGuard[Callable[P, Awaitable[R]]]:
    """Narrow a callable to its async form."""
    return is_async_callable(fn)


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
    async_checks = [guard for guard in checks if _is_async_fn(guard)]
    if async_checks:
        raise TypeError(
            f"require() got {len(async_checks)} async check(s) but {fn.__name__!r} is sync; "
            "an async check cannot run from inside a sync function"
        )

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
    :raises TypeError: At decoration, if a sync function is given an async check.
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
        if _is_async_fn(fn):
            return _wrap_async(fn, checks, mode)
        return cast(Callable[P, R], _wrap_sync(fn, checks, mode))

    return decorator
