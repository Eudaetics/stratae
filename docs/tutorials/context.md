# Context

`stratae.context` wraps a `contextvars.ContextVar` in a small, named, callable object. It solves one specific problem: values that need to flow through a call chain without being threaded as an explicit parameter through every function in between. A current user, a tenant id, a feature flag are typical examples. Each stays properly isolated between concurrent tasks.

```{motivation}
stratae has no central container. Nothing owns a request lifecycle the way an ASGI framework does, and there's no single entry point every dependency chain resolves from. There's no way to ask "what's the current user" deep inside a call chain since there is no central tracking. `Context` solves this the way React's Context API solves prop drilling. Instead of threading a value through every function signature down the chain, set a concurrency-safe, isolated value once. Anything deeper in the chain reads it directly, with nothing upstream having to know that read is even happening.

The motivating detail is that `Context` is callable. That's what lets an instance work as a `Depends()` provider directly, with no lambda or adapter function standing in between it and the context var it wraps. Wrapping `ContextVar` this way also smooths out its integration with the rest of stratae, described below.
```

## Getting and setting

A `Context[T]` wraps one `ContextVar` with a name used in error messages and an optional fallback default. Calling it reads the current value; `.use(value)` sets it for the duration of a `with` block, then restores whatever was there before.

````{example} Reading and setting a Context value
```{code-block} python
from stratae.context import Context

current_user = Context[str]("current_user", default="guest")

print(current_user())

with current_user.use("alice"):
    print(current_user())
print(current_user())
```
```{output}
guest
alice
guest
```
````

`Context` is callable: `ctx()` is an alias for `ctx.get()`.

`.use()` nests cleanly. Each call allocates its own scope, so entering a nested `.use()` doesn't clobber the outer one, and leaving it restores exactly what was active before, not the constructor's default.

````{example} Nesting scoped overrides
```{code-block} python
with current_user.use("alice"):
    with current_user.use("bob"):  # e.g. "view as" impersonation
        print(current_user())
    print(current_user())
```
```{output}
bob
alice
```
````

### Default behavior

Calling a `Context` checks three places for a value, in this order: whatever's been set in the current context, a default passed to that specific call, then the constructor's default. If none of those exist, it raises.

````{example} Falling back to a constructor default, or overriding it per call
```{code-block} python
from stratae.context import Context

permission = Context[str]("permission", default="guest")

print(permission())
print(permission(default="anonymous"))
```
```{output}
guest
anonymous
```
````

Unlike a raw `ContextVar`, an unset read with no default anywhere raises `RuntimeError` with a hint (`"Use with user_id.use(value):"`) instead of a bare `LookupError`. Sometimes even a configured default needs to be bypassed instead of masking a missing value. A security-sensitive read where "guest" would be a bug should fail hard when nothing's been set. Pass the `IGNORE_DEFAULT` sentinel per-call to force having a set value.

````{example} Forcing a hard failure with IGNORE_DEFAULT
```{code-block} python
from stratae.context import IGNORE_DEFAULT

try:
    current_user(IGNORE_DEFAULT)
except RuntimeError as exc:
    print(exc)
```
```{output}
Context 'current_user' is not set. Use `with current_user.use(value):` to set it.
```
````

`IGNORE_DEFAULT` is only valid as a per-call default; passing it to the constructor raises `ValueError`.

## Integrations

`Context` composes with the rest of stratae the same way `checks` does. Because a `Context` instance is callable with no required arguments, it slots directly into anything that expects a zero-argument provider.

### Stratae modules

`Context.__call__` takes no required arguments, so a `Context` instance is already shaped like a provider. It can be passed straight to `Depends()`, with no adapter function needed in between.

````{example} Injecting a Context value
```{code-block} python
from typing import Annotated
from stratae.depends import Depends, inject

@inject
def audit_log(action: str, user: Annotated[str, Depends(current_user)]) -> None:
    print(f"{user}: {action}")

with current_user.use("alice"):
    audit_log("deleted account")
```
```{output}
alice: deleted account
```
````

This composes with `.use()`'s scoping directly. Swap what's active in the context, and a whole DI-resolved call tree picks up the new value without any function signature changing. The same pattern works for swapping *behavior*, not just data. A `Context[Callable[[], str]]` can hold a strategy function, get temporarily replaced with `.use()` for an A/B test or a feature flag, and every injected function depending on it picks up the swap transparently.

````{example} Swapping behavior for premium feature access
```{code-block} python
from typing import Annotated, Callable
from stratae.context import Context
from stratae.depends import Depends, inject

def free_tier_export(rows: list[str]) -> str:
    preview = ", ".join(rows[:3])
    return f"{preview}... (upgrade to export all {len(rows)} rows)"

def premium_tier_export(rows: list[str]) -> str:
    return ", ".join(rows)

export_report = Context[Callable[[list[str]], str]]("export_report", default=free_tier_export)
type DataExporter = Annotated[Callable[[list[str]], str], Depends(export_report)]

data = ["alice", "bob", "carol", "dave"]

@inject
def run_export(exporter: DataExporter) -> None:
    print(exporter(data))

run_export()

with export_report.use(premium_tier_export):
    run_export()

run_export()
```
```{output}
alice, bob, carol... (upgrade to export all 4 rows)
alice, bob, carol, dave
alice, bob, carol... (upgrade to export all 4 rows)
```
````

### External tools

A `Context` is a natural fit for per-request state in a web framework, especially for code that never appears in FastAPI's own dependency graph. FastAPI can only hand a value to something it explicitly resolves as a `Depends()`. A plain helper buried a few calls deep in the route body isn't one of those. Set the value once in middleware. That helper can still pull it in with `Depends(current_user)`, the same as anywhere else in stratae. Nothing needs to be wired through the route itself.

````{example} Setting a Context value from FastAPI middleware
```{code-block} python
from typing import Annotated
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from stratae.context import Context
from stratae.depends import Depends, inject

current_user = Context[str]("current_user", default="guest")
app = FastAPI()

@app.middleware("http")
async def bind_current_user(request: Request, call_next):
    with current_user.use(request.headers.get("x-user", "guest")):
        return await call_next(request)

@inject
def log_action(action: str, user: Annotated[str, Depends(current_user)]) -> None:
    print(f"{user}: {action}")

@app.get("/whoami")
async def whoami() -> dict[str, str]:
    log_action("checked whoami")
    return {"user": current_user()}

with TestClient(app) as client:
    client.get("/whoami")
    client.get("/whoami", headers={"x-user": "alice"})
```
```{output}
guest: checked whoami
alice: checked whoami
```
````

Full signatures: {doc}`stratae.context API reference <../apidocs/stratae.context/stratae.context>`.
