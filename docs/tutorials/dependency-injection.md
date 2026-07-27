# Dependency Injection

`stratae.depends` wires a function's parameters to provider callables, resolved at call time. It handles exactly one job — connecting injected parameters to the functions that produce their values — and stays out of caching, scoping, or lifecycle concerns entirely; that's `stratae.lifecycle`'s job, and the two compose deliberately (see [Combining with lifecycle](#combining-with-lifecycle) below).

## Marking and injecting

A provider is any callable, sync or async, wrapped in `Depends`. Mark the parameter that should receive its result with `Annotated[T, Depends(provider)]`, and decorate the function with `@inject`:

```python
from typing import Annotated
from stratae.depends import Depends, inject


def get_tax_rate() -> float:
    return 0.08


@inject
def total_with_tax(subtotal: float, tax_rate: Annotated[float, Depends(get_tax_rate)]) -> float:
    return subtotal + subtotal * tax_rate


total_with_tax(100.0)  # 108.0 -- tax_rate is resolved, not passed
```

`@inject` inspects the signature once, at decoration time, and generates a wrapper whose visible signature drops the injected parameters entirely — callers never see or pass `tax_rate`. If a function has no injected parameters, `@inject` returns it unchanged, so it's safe to apply broadly.

A common ergonomic pattern is a type alias for a dependency used in several places:

```python
type TaxRate = Annotated[float, Depends(get_tax_rate)]


@inject
def total_with_tax(subtotal: float, tax_rate: TaxRate) -> float: ...
```

## Composing providers

Providers can themselves take injected parameters, resolved recursively when the function that uses them is decorated:

```python
def get_config() -> Config: ...


def get_database(config: Annotated[Config, Depends(get_config)]) -> Database:
    return Database(config.dsn)


@inject
def create_user(name: str, db: Annotated[Database, Depends(get_database)]) -> User: ...
```

Only `create_user` needs `@inject` — the whole chain (`create_user` → `get_database` → `get_config`) is wired up the moment `create_user` is decorated. A cycle anywhere in that chain raises `CircularDependencyError` at decoration time, rather than surfacing as a buried `RecursionError`.

## Providers aren't cached

Every call to an injected function re-runs its providers, unless you layer caching on top yourself:

```python
create_user("Alice")  # calls get_database() (and get_config())
create_user("Bob")  # calls get_database() (and get_config()) again
```

This is a deliberate boundary, not an oversight: `stratae.depends` only wires values through; scoping how long a value lives is [`stratae.lifecycle`](lifecycle)'s job. See [Combining with lifecycle](#combining-with-lifecycle).

## Sync and async

Async functions can depend on a mix of sync and async providers — `@inject` awaits the async ones and calls the sync ones directly. Sync functions can only depend on sync providers; depending on an async provider from a sync function raises `InjectionSignatureError` at decoration time, since there's no way to await inside a sync call.

```python
async def get_current_user(user_id: Annotated[int, Depends(get_user_id)]) -> User:
    return await db.fetch_user(user_id)


@inject
async def handle_request(user: Annotated[User, Depends(get_current_user)]) -> Response: ...
```

Generator and async-generator functions can be injection targets too, not just plain functions and coroutines.

## Combining with lifecycle

The flagship pattern is a provider cached by `stratae.lifecycle`, then injected normally — `depends` doesn't need to know the value is cached, it just calls whatever callable it's given:

```python
from stratae.lifecycle import Lifecycle, Scope

lifecycle = Lifecycle([Scope("application", isolation="shared")])


@lifecycle.cache("application")
def get_database() -> Database:
    return Database(connect())


@inject
def create_user(name: str, db: Annotated[Database, Depends(get_database)]) -> User: ...


with lifecycle.start("application"):
    create_user("Alice")  # get_database() runs once per activation, not once per call
    create_user("Bob")  # cached value reused
```

See the [Lifecycle guide](lifecycle) for how scopes, caching, and cleanup actually work.

## Swapping providers for tests

`override` (single) and `overrides` (several at once) replace a provider's value for the duration of a `with` block. They target the exact callable passed to `Depends(...)` — not an equivalent function, the same object — and raise `DependencyNotFoundError` otherwise:

```python
from stratae.depends import override


def test_create_user():
    fake_db = FakeDatabase()
    with override(get_database, fake_db):
        user = create_user("Alice")
    assert fake_db.users == [user]
```

The override value is used as-is — it's not called, even for an async provider. Overrides are stored per `contextvars` context, so concurrent tests or tasks can each hold a different override for the same provider without interfering, and they nest cleanly: an `override()` inside another `override()` for the same provider restores the outer one on exit, not the original.

```python
from stratae.depends import overrides

with overrides({get_database: fake_db, get_config: fake_config}):
    ...
```

`overrides` applies every entry atomically — if one fails partway through, whichever already applied are unwound before the exception propagates.

## Errors

| Exception | Raised when |
|---|---|
| `CircularDependencyError` | Providers depend on each other in a cycle |
| `InjectionSignatureError` | An injected parameter has a default value, or a sync function depends on an async provider |
| `DependencyNotFoundError` | `override`/`overrides` references a callable never passed to `Depends(...)` |

All decoration-time errors (`CircularDependencyError`, `InjectionSignatureError`) surface when `@inject` runs — at import time, in practice — rather than on the first call, so a broken dependency graph fails fast.

Full signatures and every exported name: {doc}`stratae.depends API reference <../apidocs/stratae.depends/stratae.depends>`.
