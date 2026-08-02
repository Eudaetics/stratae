# Checks

`stratae.checks` provide options for running a set of preconditions. Most often these would be guard-style checks to prevent running code by raising if one of the checks fail. A "check" here is nothing more than a zero-argument callable that raises on failure. Using lambdas, closures, or dependency injection are all options to provide run-time information for a check.

## Running checks

````{example} Running a set of checks
```{code-block} python
from types import SimpleNamespace
from stratae.checks import check

user = SimpleNamespace(is_admin=True, suspended=False)

def is_admin() -> None:
    assert user.is_admin, "not an admin"

def not_suspended() -> None:
    assert not user.suspended, "account suspended"

check(is_admin, not_suspended)
print("checks passed")
```
```{output}
checks passed
```
````

By default (`mode="all"`), `check` stops at the first failure and propagates its exception as-is. Two other modes change how failures are collected:
- `mode="gather"` — run every check regardless of earlier failures, then raise one `ExceptionGroup` if any failed. Useful for form or batch validation, where you want to report every problem at once instead of stopping at the first.
- `mode="any"` — succeed as soon as one check passes; if all fail, raises an `ExceptionGroup` of every failure.

`check` only accepts synchronous checks — passing one that returns an awaitable raises `TypeError` telling you to use `check_async` instead. `check_async` accepts a mix of sync and async checks in the same call, awaiting the async ones.

## Guarding a function

`require` is the decorator form — it runs its checks before the wrapped function and discards their return values, since only side effects (raising, or not) matter:

````{example} Guarding a function with require
```{code-block} python
from types import SimpleNamespace
from stratae.checks import require

user = SimpleNamespace(is_admin=True)

def is_admin() -> None:
    assert user.is_admin, "not an admin"

@require(is_admin)
def delete_account(account_id: int) -> None:
    print(f"deleting account {account_id}")

delete_account(24)
```
```{output}
deleting account 24
```
````

For a sync function, every check passed to `require` must be sync — an async check raises `TypeError` at decoration time, not buried in a call-time failure. An async-decorated function can mix sync and async checks freely. Decorating with no checks at all is a no-op: `require()` returns the function unchanged.

## Composing boolean logic

`check`'s `mode` is one setting for the whole call, so it can't express nested logic on its own. `any_of` and `all_of` each collapse a group of checks into a single check, so groups can nest inside each other:

````{example} Composing any_of and all_of checks
```{code-block} python
from types import SimpleNamespace
from stratae.checks import all_of, any_of, check

user = SimpleNamespace(id=7, is_admin=False, is_super_admin=False, is_manager=True)
resource = SimpleNamespace(owner_id=7, pending=False)
target_user = SimpleNamespace(manager_id=7)

def is_admin() -> None:
    assert user.is_admin, "not an admin"

def is_owner() -> None:
    assert resource.owner_id == user.id, "not the owner"

def not_pending() -> None:
    assert not resource.pending, "resource is pending"

def is_super_admin() -> None:
    assert user.is_super_admin, "not a super admin"

def is_manager() -> None:
    assert user.is_manager, "not a manager"

def manages_target_user() -> None:
    assert target_user.manager_id == user.id, "does not manage this user"

# (is_admin OR is_owner) AND not_pending
check(any_of(is_admin, is_owner), not_pending)
print("resource access granted")

# is_super_admin OR (is_manager AND manages_target_user)
check(is_super_admin, all_of(is_manager, manages_target_user), mode="any")
print("target user access granted")
```
```{output}
resource access granted
target user access granted
```
````

If any check inside `any_of`/`all_of` is async, the combinator it returns is itself async, and can only be run via `check_async` (or nested inside another async group) — the "async-ness" is contagious upward through the composition.

## A health-check example

`mode="any"` reads naturally as a fallback chain — try the fast path, fall back to the slow one:

````{example} Falling back to a slower health check
```{code-block} python
import asyncio
from stratae.checks import check_async

def cache_healthy() -> None:
    print("checking cache")
    raise ConnectionError("cache unreachable")

async def store_healthy() -> None:
    print("checking store")

async def main() -> None:
    await check_async(cache_healthy, store_healthy, mode="any")
    print("healthy")

asyncio.run(main())
```
```{output}
checking cache
checking store
healthy
```
````

Full signatures: {doc}`stratae.checks API reference <../apidocs/stratae.checks/stratae.checks>`.
