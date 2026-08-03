# Checks

`stratae.checks` provide options for running a set of conditions. Most often these would be guard-style checks to prevent running code by raising if one of the checks fail. A "check" here is nothing more than a zero-argument callable that raises on failure. Using lambdas, closures, or dependency injection are all options to provide run-time information for a check.

```{motivation}
Without `checks`, a dependency with side effects still needs a spot in the function's parameter list. That's true even when the function body never uses it. Moving the check into `checks` removes the need to pass in data for which the function has no other use. The same pattern fits any guard-style precondition, not just side-effect dependencies. `checks` grew from that one fix into the general-purpose module described here.
```

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

```{attention}
`check` only accepts synchronous checks. Passing one that returns an awaitable raises `TypeError` telling you to use `check_async` instead. `check_async` accepts a mix of sync and async checks in the same call, awaiting the async ones.
```

Here, `not_suspended` is async while `is_admin` stays sync, and `check_async` runs both in the same call.

````{example} Running async checks
```{code-block} python
import asyncio
from types import SimpleNamespace
from stratae.checks import check_async

user = SimpleNamespace(is_admin=True, suspended=False)

def is_admin() -> None:
    assert user.is_admin, "not an admin"

async def not_suspended() -> None:
    assert not user.suspended, "account suspended"

async def main() -> None:
    await check_async(is_admin, not_suspended)
    print("checks passed")

asyncio.run(main())
```
```{output}
checks passed
```
````

### Optional modes
By default (`mode="all"`), `check` stops at the first failure and propagates its exception as-is. Two other modes change how failures are collected:
- `mode="gather"` -- run every check regardless of earlier failures, then raise one `ExceptionGroup` if any failed. Useful for form or batch validation, where you want to report every problem at once instead of stopping at the first.
- `mode="any"` -- succeed as soon as one check passes; if all fail, raises an `ExceptionGroup` of every failure.

### Boolean expressions
`mode` is one setting for the whole `check` call, so it can't express nested AND/OR logic on its own. `any_of` and `all_of` collect a set of checks into a group, so `check(any_of(is_admin, is_owner), not_pending)` reads as `(is_admin OR is_owner) AND not_pending`. See [Composing boolean logic](#composing-boolean-logic) for the full walkthrough.


## Guarding a function

`require` is the decorator form of `check`. It runs before the wrapped function and discards the return values of the given checks, since only side effects (raising, or not) matter.

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

For a sync function, every check passed to `require` must be sync. An async check raises `TypeError` at decoration time instead of buried in a call-time failure. An async-decorated function can mix sync and async checks freely. Decorating with no checks at all is a no-op: `require()` returns the function unchanged.

## Composing boolean logic

`check`'s `mode` is one setting for the whole call, so it can't express nested logic on its own. `any_of` and `all_of` each collapse checks into a single group. These groups can be nested for more complex evaluations.

````{example} Composing any_of and all_of checks
```{code-block} python
from types import SimpleNamespace
from stratae.checks import all_of, any_of, check

user = SimpleNamespace(
    id=7, is_admin=False, is_super_admin=False, is_manager=True
)
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

If any check inside `any_of`/`all_of` is async, the combination it returns is itself async, and can only be run via `check_async` (or nested inside another async group). The "async-ness" is contagious upward through the composition.

## An authentication example

`mode="any"` fits a precondition with more than one valid way to satisfy it, where it doesn't matter which one does. A user is authenticated whether they passed a password or an SSO token, and nothing downstream needs to know which.

````{example} Accepting any of several authentication methods
```{code-block} python
import asyncio
from stratae.checks import check_async

async def verified_by_password() -> None:
    print("checking password")
    assert False, "incorrect password"

async def verified_by_sso() -> None:
    print("checking SSO token")

async def main() -> None:
    await check_async(verified_by_password, verified_by_sso, mode="any")
    print("authenticated")

asyncio.run(main())
```
```{output}
checking password
checking SSO token
authenticated
```
````

## Integrations

`checks` composes with the rest of stratae. A check is just a callable, so it slots into injected dependencies, scoped resources, and event handlers without any special glue. It fits into external frameworks just as easily.

### Stratae modules

`@inject` strips fully-injected parameters from a function's visible signature. A check that only needs injected dependencies becomes a zero-argument callable on its own, and can be passed straight to `require` or `check`.

````{example} Guarding with an injected dependency
```{code-block} python
from types import SimpleNamespace
from typing import Annotated
from stratae.checks import require
from stratae.depends import Depends, inject

current_user = SimpleNamespace(is_admin=False)

def get_current_user() -> SimpleNamespace:
    return current_user

type CurrentUser = Annotated[SimpleNamespace, Depends(get_current_user)]

@inject
def is_admin(user: CurrentUser) -> None:
    assert user.is_admin, "not an admin"

@require(is_admin)
def delete_account(account_id: int) -> None:
    print(f"deleting account {account_id}")

try:
    delete_account(24)
except AssertionError as exc:
    print("rejected:", exc)

current_user.is_admin = True
delete_account(24)
```
```{output}
rejected: not an admin
deleting account 24
```
````

A check isn't limited to closures over plain module state either. It can call straight into a `lifecycle`-scoped resource. Here `not_suspended` reads `get_account()`, which is cached once per `request` activation. The same account instance used elsewhere in the request gets reused for free.

````{example} Guarding with a scoped resource
```{code-block} python
from types import SimpleNamespace
from stratae.checks import require
from stratae.lifecycle import Scope

request = Scope("request")

@request.cache()
def get_account() -> SimpleNamespace:
    return SimpleNamespace(is_suspended=True)

def not_suspended() -> None:
    assert not get_account().is_suspended, "account suspended"

@require(not_suspended)
def place_order(order_id: int) -> None:
    print(f"processing order {order_id}")

with request.activate():
    account = get_account()
    try:
        place_order(42)
    except AssertionError as exc:
        print("rejected:", exc)

    account.is_suspended = False
    place_order(42)
```
```{output}
rejected: account suspended
processing order 42
```
````

`require` doesn't care that the function it wraps is registered as an event handler either. Stack it closer to the function than `@bus.handle`, so the check runs before the handler body. A failing check surfaces through the bus's normal failure handling: `DirectBus` wraps any failing `PubSub` handler in an `ExceptionGroup`, so the check's `AssertionError` shows up there instead of on its own.

````{example} Guarding an event handler with require
```{code-block} python
from types import SimpleNamespace
from stratae.checks import require
from stratae.events import DirectBus, Event, PubSub

class OrderPlaced:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

store = SimpleNamespace(accepting_orders=False)
bus = DirectBus()
order_placed = Event(PubSub, OrderPlaced)
place_order = bus.bind(order_placed, factory=OrderPlaced)

def accepting_orders() -> None:
    assert store.accepting_orders, "not accepting orders"

@bus.handle(order_placed)
@require(accepting_orders)
def _(order: OrderPlaced) -> None:
    print(f"processing order {order.order_id}")

try:
    place_order(order_id=42)
except ExceptionGroup as exc:
    print("rejected:", exc.exceptions[0])

store.accepting_orders = True
place_order(order_id=42)
```
```{output}
rejected: not accepting orders
processing order 42
```
````

All three compose without any special-casing. `application` caches an account store; `request` resolves the current user from it as an injected dependency; `@inject` collapses that whole chain into the zero-argument check `require` expects. The same admin check then guards an `events` handler exactly like the standalone example above, just built from a longer dependency chain.

````{example} Combining depends, lifecycle, and events
```{code-block} python
from types import SimpleNamespace
from typing import Annotated
from stratae.checks import require
from stratae.depends import Depends, inject
from stratae.events import DirectBus, Event, PubSub
from stratae.lifecycle import Scope

class RefundIssued:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

application = Scope("application", isolation="shared")
request = Scope("request", requires=application)
bus = DirectBus()
refund_issued = Event(PubSub, RefundIssued)
issue_refund = bus.bind(refund_issued, factory=RefundIssued)

@application.cache()
def get_accounts() -> dict[str, SimpleNamespace]:
    return {
        "alice": SimpleNamespace(is_admin=False),
        "bob": SimpleNamespace(is_admin=True),
    }

type Accounts = Annotated[dict[str, SimpleNamespace], Depends(get_accounts)]

@request.cache()
@inject
def get_current_user(accounts: Accounts) -> SimpleNamespace:
    return accounts[current_user_id]

type CurrentUser = Annotated[SimpleNamespace, Depends(get_current_user)]

@inject
def is_admin(user: CurrentUser) -> None:
    assert user.is_admin, "not an admin"

@bus.handle(refund_issued)
@require(is_admin)
def _(refund: RefundIssued) -> None:
    print(f"refunding order {refund.order_id}")

current_user_id = "alice"
with application.activate():
    with request.activate():
        try:
            issue_refund(order_id=42)
        except ExceptionGroup as exc:
            print("rejected:", exc.exceptions[0])

    current_user_id = "bob"
    with request.activate():
        issue_refund(order_id=42)
```
```{output}
rejected: not an admin
refunding order 42
```
````

### External tools

`require` stacks under a route decorator too, guarding the endpoint before its body runs. A check can raise `fastapi.HTTPException` directly. FastAPI's built-in handling turns that into the response.

````{example} Guarding a FastAPI route with require
```{code-block} python
from types import SimpleNamespace
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from stratae.checks import require

app = FastAPI()

current_user = SimpleNamespace(is_admin=False)

def is_admin() -> None:
    if not current_user.is_admin:
        raise HTTPException(403, "not an admin")

@app.delete("/accounts/{account_id}")
@require(is_admin)
async def delete_account(account_id: int) -> dict[str, int]:
    return {"deleted": account_id}

with TestClient(app) as client:
    response = client.delete("/accounts/24")
    print(response.status_code)

    current_user.is_admin = True
    response = client.delete("/accounts/24")
    print(response.json())
```
```{output}
403
{'deleted': 24}
```
````

```{tip}
FastAPI already has its own dependency system for exactly this. A guard-only `Depends(is_admin)` runs before the handler and can raise the same way. `require` is worth reaching for mainly when the same guard needs to run outside a FastAPI request too, since a `require`d check has no framework attached to it. It also sidesteps FastAPI's own dependency-resolution overhead for a check that doesn't need it. Fittingly, `checks` traces its origin to exactly this pattern in FastAPI: an endpoint dependency used purely for its side effects, its return value thrown away.
```

Full signatures: {doc}`stratae.checks API reference <../apidocs/stratae.checks/stratae.checks>`.
