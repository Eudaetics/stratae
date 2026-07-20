"""
Decorator for running zero-arg guard checks before a function.

`require` wraps a function with one or more zero-arg checks that run in
order ahead of the call. A check's return value is discarded; only its
side effects and raises matter. Accepts an `errors` keyword controlling
failure handling, same as `check`/`check_async`: "raise" (default) stops
at the first failing check and propagates its exception; "gather" runs
every check regardless of earlier failures and raises an
:class:`ExceptionGroup` containing all of them. Either way, the wrapped
function is never called if a check fails.

Sync functions only accept sync checks (an async check cannot be awaited
from inside a sync call), validated eagerly at decoration time so a
sync/async mismatch fails at import rather than on first request. Async
functions accept a mix of sync and async checks. Sync checks run inline
and async checks are awaited in the order given.
"""

import inspect
from functools import wraps
from typing import Any, Awaitable, Callable, TypeGuard, cast, overload

from .check import CheckMode, check, check_async

_Check = Callable[[], Any]


def _is_async_fn[**P, R](
    fn: Callable[P, R] | Callable[P, Awaitable[R]],
) -> TypeGuard[Callable[P, Awaitable[R]]]:
    """Narrow a callable (zero-arg guard or arbitrary-signature fn) to its async form."""
    return inspect.iscoroutinefunction(fn)


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
    Run zero-arg guard checks before the decorated function in order.

    Each check's return value is discarded; only side effects and raises
    matter. The first check to raise aborts the remaining checks and the
    wrapped function. Each function must be zero-arg callable. Use dependency
    injection, lambdas, or other tools to ensure any function that relies
    on dynamic behavior can be called without passing in arguments.

    Decorating an async function runs sync checks inline and awaits async
    checks, in the order given. Decorating a sync function requires all
    checks to be sync, validated eagerly at decoration time so a mismatch
    fails at import rather than on first call. With no checks, the
    function is returned unchanged.

    Type Parameters:
        P: Parameter specification of the decorated function.
        R: Return type of the decorated function.

    Args:
        *checks: Zero-arg callables to run, in order, before each call to
            the decorated function.

    Returns:
        A decorator that wraps its target function with the given checks,
        preserving its signature.

    Args:
        checks: Zero-argument guard callables to run, in order, before ``fn``.
        mode: How to handle a failing check. "raise" (default) stops at
            the first failure and propagates its exception, so ``fn`` is
            never called. "gather" runs every check regardless of earlier
            failures and raises them together as an :class:`ExceptionGroup`,
            still short-circuiting ``fn``.

    Raises:
        TypeError: At decoration time, if the decorated function is sync
            but a check is async, since there is no safe way to await an
            async check from inside a sync function.

    Example:
        .. code-block:: python

            @require(is_admin)
            def delete_user(user_id: int) -> None: ...

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
