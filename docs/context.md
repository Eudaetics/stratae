# Context

`stratae.context` wraps a `contextvars.ContextVar` in a small, named, callable object. It solves one specific problem: values — a current user, a tenant id, a feature flag — that need to flow through a call chain without being threaded as an explicit parameter through every function in between, while staying properly isolated between concurrent tasks.

## Getting and setting

```python
from stratae.context import Context

current_user = Context[str]("current_user", default="guest")

current_user()  # "guest" -- falls back to the constructor default

with current_user.use("alice"):
    current_user()  # "alice"
current_user()  # back to "guest"
```

`Context` is callable — `ctx()` and `ctx.get()` are the same thing. Resolution checks, in order: the value set in the current context, then a default passed to this specific `.get()`/`__call__` call, then the constructor's default, then raises. Unlike a raw `ContextVar`, an unset read raises `RuntimeError` with a hint (`"Use with user_id.use(value):"`) instead of a bare `LookupError`.

`.use(value)` is a context manager that restores the previous value on exit, and nests cleanly — each call allocates its own scope, so a nested `.use()` inside another doesn't clobber it:

```python
with current_user.use("alice"):
    with current_user.use("bob"):       # e.g. "view as" impersonation
        current_user()  # "bob"
    current_user()  # "alice" -- restored
```

If you need to bypass a configured default and force a hard failure when nothing's been set — a security-sensitive read where "guest" would be a bug, say — pass the `IGNORE` sentinel per-call:

```python
from stratae.context import IGNORE

current_user(IGNORE)  # raises LookupError if unset, ignoring the "guest" default
```

`IGNORE` is only valid as a per-call default; passing it to the constructor raises `ValueError`.

## Using Context as a dependency

Because `Context.__call__` takes no required arguments, a `Context` instance can be passed straight to `Depends()` — no adapter needed:

```python
from stratae.depends import Depends, Injected, inject

@inject
def audit_log(action: str, user: Injected[str, Depends(current_user)]) -> None:
    print(f"{user}: {action}")

with current_user.use("alice"):
    audit_log("deleted account")  # "alice: deleted account"
```

This composes with `.use()`'s scoping directly: swap what a whole DI-resolved call tree sees by changing what's active in the context, without touching any function signature. The same pattern works for swapping *behavior*, not just data — a `Context[Callable[[], str]]` holding a strategy function, temporarily replaced with `.use()` for an A/B test or a feature flag, consumed transparently by every injected function that depends on it.

Full signatures: {doc}`stratae.context API reference <apidocs/stratae.context/stratae.context>`.
