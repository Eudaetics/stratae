# Lifecycle

`stratae.lifecycle` scopes caching and cleanup to a unit of work — a request, a background job, an application run. Where `stratae.depends` wires a value through, `stratae.lifecycle` decides how long that value lives and what happens to it when the unit of work ends.

## Scopes

A `Scope` is a name plus two independent settings: `isolation` and `storage`.

```python
from stratae.lifecycle import Scope

Scope("application", isolation="shared", storage="sparse")
Scope("request")  # isolation="context", storage="dense" by default
```

**`isolation`** controls who sees a cached value:
- `"context"` (default) — backed by a `contextvars.ContextVar`. Each execution context — each `asyncio` task, in practice — gets its own independent cache. Use this for per-request or per-job state that shouldn't leak between concurrent tasks.
- `"shared"` — one cache visible to every thread and task while the scope is active, guarded by a lock so concurrent first-computations don't race. Use this for process-wide singletons like a connection pool.

**`storage`** is a performance knob, not a behavioral one:
- `"dense"` (default) — a flat list, indexed by slot. Faster when most registered functions in the scope get touched on most activations, or when there are few of them (below roughly fifty).
- `"sparse"` — lazy dict-like allocation. Faster when a scope has many registered functions but only a handful are touched per activation.

Start with the defaults; reach for `"shared"` and `"sparse"` only when you know you need them.

## Starting a scope, caching within it

`Lifecycle` (sync) and `AsyncLifecycle` (async) are built from a non-empty list of uniquely-named scopes:

```python
from stratae.lifecycle import Lifecycle, Scope

lifecycle = Lifecycle(
    [
        Scope("application", isolation="shared"),
        Scope("request"),
    ]
)


@lifecycle.cache("application")
def get_database() -> Database:
    return Database(connect())


with lifecycle.start("application"):
    get_database()  # runs the function, caches the result
    get_database()  # returns the cached result
# activation ends here -- the cache is gone
```

`.cache(scope)` behaves like `functools.lru_cache`, scoped to one activation: a function that takes arguments gets one cached value *per distinct argument set* within that activation, not one value overall.

```python
@lifecycle.cache("request")
def get_user(user_id: int) -> User: ...


with lifecycle.start("request"):
    get_user(1)  # computed
    get_user(1)  # cached
    get_user(2)  # computed separately
```

Two keyword-only options adjust this, and are mutually exclusive:
- `ignore_params=True` — collapse to a single cached value per activation regardless of arguments, for when you know the result won't vary within one activation even though the function takes parameters.
- `cache_key=fn` — derive the cache key from the arguments yourself, e.g. keying on `user.id` instead of the whole `user` object.

A function that takes no parameters always uses the fast single-value path automatically.

Scopes can be activated in any order — you don't have to start `"application"` before `"request"` — though starting broader scopes first is the natural way to get meaningful sharing between them.

## Resources: cleanup on scope exit

`resource` and `async_resource` turn a single-yield generator function into an auto-entered context manager for `.cache()`. The yielded value is what gets cached; the code after `yield` runs when the *scope* exits, not when the function returns:

```python
from stratae.lifecycle import resource


@lifecycle.cache("application")
@resource
def get_database():
    conn = Database(connect())
    try:
        yield conn
    finally:
        conn.close()
```

`@lifecycle.cache(...)` must be the outer decorator and `@resource`/`@async_resource` the inner one — cache needs to see the tagged, wrapped function to know to auto-enter it. Skipping `@resource` on a generator function is a real footgun: without it, `.cache()` treats the function as an ordinary callable and caches the unconsumed generator object itself, not the value it would have yielded.

If a scope has several open resources, they close in LIFO order — the reverse of the order they were entered, same as `contextlib.ExitStack`. If more than one raises while closing, the exceptions are chained into a single `ExceptionGroup` rather than the last one silently winning.

## A worked example: two-tier app scopes

```python
lifecycle = Lifecycle(
    [
        Scope("application", isolation="shared"),
        Scope("request"),
    ]
)


@lifecycle.cache("application")
@resource
def get_pool():
    pool = ConnectionPool(connect())
    try:
        yield pool
    finally:
        pool.close()


@lifecycle.cache("request")
def get_request_id() -> str:
    return str(uuid4())


with lifecycle.start("application"):
    with lifecycle.start("request"):
        pool = get_pool()  # opened once for the whole application activation
        request_id = get_request_id()  # fresh per request activation
    # request scope exits -- request_id's cache is gone
# application scope exits -- pool.close() runs here
```

Because `"request"` is context-isolated, two concurrent `asyncio` tasks each starting their own `"request"` activation get independent `get_request_id()` values, even though both see the same shared `get_pool()` connection pool from the `"shared"` `"application"` scope underneath them.

## Sync vs async

| | `Lifecycle` | `AsyncLifecycle` |
|---|---|---|
| Activate | `with lifecycle.start(scope):` | `async with lifecycle.start(scope):` |
| Cacheable functions | sync only | sync, async, `resource`, and `async_resource` — all four, auto-detected |
| Resource decorator | `resource` | `async_resource` (or `resource`, if the cleanup itself is sync) |

`AsyncLifecycle.cache` accepts a plain sync function too, wrapping it to work from async code — a single `.cache("request")` call site works no matter which of the four kinds the decorated function is.

## Errors

| Exception | Raised when |
|---|---|
| `LifecycleConfigurationError` | Invalid scope name/isolation/storage, empty scope list, or duplicate scope names |
| `ScopeNotFoundError` | Referencing a scope name that was never declared |
| `ScopeInactiveError` | Referencing a scope that's declared but not currently active in this context |
| `ScopeActivationError` | Popping a manual activation with a stale or already-used token |

`ScopeNotFoundError` and `ScopeInactiveError` are deliberately distinct — "never declared" and "declared but not started here" are different bugs.

Full signatures and every exported name: {doc}`stratae.lifecycle API reference <../apidocs/stratae.lifecycle/stratae.lifecycle>`.
