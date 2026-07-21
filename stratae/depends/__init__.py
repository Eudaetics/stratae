"""
Dependency injection via annotated parameters and the `inject` decorator.

Mark a parameter with `Annotated[T, Depends(provider)]` and decorate the
function with {py:func}`inject <stratae.depends.inject.inject>`. The
parameter is resolved by calling its provider at call time, and callers
no longer pass it. Providers are plain callables and may declare
injected parameters of their own, resolved recursively when the injected
function is decorated. {py:func}`Depends <stratae.depends.inject.Depends>`
marks a callable as a provider.

{py:data}`Injected <stratae.depends.inject.Injected>` is provided as an
alias of `Annotated` to highlight injected parameters. Using that alias
is not required. Defining a shared Annotated type is supported, such as
`type UserDep = Annotated[User, Depends(get_user)]`.

Sync functions may only depend on sync providers, validated at
decoration time. Async functions may mix sync and async providers.
Generator and async generator functions are supported.

{py:func}`override <stratae.depends.override.override>` and
{py:func}`overrides <stratae.depends.override.overrides>` temporarily
replace providers' values within a scope, e.g. substituting a mock
in tests. Overrides are context-local, so concurrent tasks do
not interfere with each other.

```{rubric} Example:
```
```{code-block} python
:caption: Injecting a database dependency and swapping it for a fake within a scope

from stratae.depends import Depends, Injected, inject, override

class Database:
    def __init__(self, name: str):
        self.name = name

def get_db() -> Database:
    return Database("production")

@inject
def get_db_name(db: Injected[Database, Depends(get_db)]) -> str:
    return db.name

assert get_db_name() == "production"  # db resolved by calling get_db()

with override(get_db, Database("test")):
    assert get_db_name() == "test"  # db is the fake within this scope

assert get_db_name() == "production"
```

See {py:func}`Depends <stratae.depends.inject.Depends>`,
{py:func}`inject <stratae.depends.inject.inject>`,
{py:func}`override <stratae.depends.override.override>`, and
{py:func}`overrides <stratae.depends.override.overrides>` for additional
examples.

"""

from .inject import Depends, Injected, inject
from .override import override, overrides

__all__ = [
    "Depends",
    "override",
    "overrides",
    "Injected",
    "inject",
]
