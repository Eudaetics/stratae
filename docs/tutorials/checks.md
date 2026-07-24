# Checks

`stratae.checks` runs a set of preconditions and raises if they don't hold. A "check" is nothing more than a zero-argument callable that raises on failure — no special base class or return protocol, so checks are usually closures, lambdas, or bound methods that capture whatever state they need.

## Running checks

```python
from stratae.checks import check


def is_admin() -> None:
    assert user.is_admin, "not an admin"


def not_suspended() -> None:
    assert not user.suspended, "account suspended"


check(is_admin, not_suspended)
```

By default (`mode="all"`), `check` stops at the first failure and propagates its exception as-is. Two other modes change how failures are collected:
- `mode="gather"` — run every check regardless of earlier failures, then raise one `ExceptionGroup` if any failed. Useful for form or batch validation, where you want to report every problem at once instead of stopping at the first.
- `mode="any"` — succeed as soon as one check passes; if all fail, raises an `ExceptionGroup` of every failure.

`check` only accepts synchronous checks — passing one that returns an awaitable raises `TypeError` telling you to use `check_async` instead. `check_async` accepts a mix of sync and async checks in the same call, awaiting the async ones.

## Guarding a function

`require` is the decorator form — it runs its checks before the wrapped function and discards their return values, since only side effects (raising, or not) matter:

```python
from stratae.checks import require


def is_admin() -> None:
    assert user.is_admin


@require(is_admin)
def delete_account(account_id: int) -> None: ...
```

For a sync function, every check passed to `require` must be sync — an async check raises `TypeError` at decoration time, not buried in a call-time failure. An async-decorated function can mix sync and async checks freely. Decorating with no checks at all is a no-op: `require()` returns the function unchanged.

## Composing boolean logic

`check`'s `mode` is one setting for the whole call, so it can't express nested logic on its own. `any_of` and `all_of` each collapse a group of checks into a single check, so groups can nest inside each other:

```python
from stratae.checks import any_of, all_of, check

# (is_admin OR is_owner) AND not_pending
check(any_of(is_admin, is_owner), not_pending)

# is_super_admin OR (is_manager AND manages_target_user)
check(is_super_admin, all_of(is_manager, manages_target_user), mode="any")
```

If any check inside `any_of`/`all_of` is async, the combinator it returns is itself async, and can only be run via `check_async` (or nested inside another async group) — the "async-ness" is contagious upward through the composition.

## A health-check example

`mode="any"` reads naturally as a fallback chain — try the fast path, fall back to the slow one:

```python
def cache_healthy() -> None:
    assert redis.ping()


async def store_healthy() -> None:
    assert await db.execute("SELECT 1")


await check_async(cache_healthy, store_healthy, mode="any")
```

Full signatures: {doc}`stratae.checks API reference <../apidocs/stratae.checks/stratae.checks>`.
