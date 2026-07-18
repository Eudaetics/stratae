"""
Decorator for running zero-arg guard checks before a function.

`require` wraps a function with one or more zero-arg checks that run in
order ahead of the call. A check's return value is discarded; only its
side effects and raises matter. The first check to raise aborts the
remaining checks and the wrapped function immediately.

Sync functions only accept sync checks (an async check cannot be awaited
from inside a sync call), validated eagerly at decoration time so a
sync/async mismatch fails at import rather than on first request. Async
functions accept a mix of sync and async checks. Sync checks run inline
and async checks are awaited in the order given.
"""

import inspect
from functools import wraps
from typing import Any, Awaitable, Callable, Literal, TypeGuard, cast, overload

_Check = Callable[[], Any]
_AsyncCheck = Callable[[], Awaitable[Any]]
_CheckEntry = tuple[Literal[True], _AsyncCheck] | tuple[Literal[False], _Check]


def _is_async_fn[**P, R](
    fn: Callable[P, R] | Callable[P, Awaitable[R]],
) -> TypeGuard[Callable[P, Awaitable[R]]]:
    """Narrow a callable (zero-arg guard or arbitrary-signature fn) to its async form."""
    return inspect.iscoroutinefunction(fn)


def _wrap_async[**P, R](
    fn: Callable[P, Awaitable[R]], checks: tuple[_Check, ...]
) -> Callable[P, Awaitable[R]]:
    check_plan: list[_CheckEntry] = [
        (True, check) if _is_async_fn(check) else (False, check) for check in checks
    ]

    @wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        for entry in check_plan:
            match entry:
                case (True, check):
                    await check()
                case (False, check):
                    check()
        return await fn(*args, **kwargs)

    return wrapper


def _wrap_sync[**P, R](fn: Callable[P, R], checks: tuple[_Check, ...]) -> Callable[P, R]:
    async_checks = [check for check in checks if _is_async_fn(check)]
    if async_checks:
        raise TypeError(
            f"require() got {len(async_checks)} async check(s) but {fn.__name__!r} is sync; "
            "an async check cannot run from inside a sync function"
        )

    @wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        for check in checks:
            check()
        return fn(*args, **kwargs)

    return wrapper


def require[**P, R](*checks: _Check) -> Callable[[Callable[P, R]], Callable[P, R]]:
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
            return _wrap_async(fn, checks)
        return cast(Callable[P, R], _wrap_sync(fn, checks))

    return decorator
