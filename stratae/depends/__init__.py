"""
Dependency injection via annotated parameters and the `inject` decorator.

Mark a parameter with `Annotated[T, Depends(provider)]` and decorate the
function with `inject`. The parameter is resolved by calling its provider
at call time, and callers no longer pass it. Providers are plain
callables and may declare injected parameters of their own, resolved
recursively when the injected function is decorated.

`Injected` is provided as an alias of `Annotated` to highlight injected
parameters. Using that alias is not required. Defining a shared Annotated
type is supported, such as`type UserDep = Annotated[User, Depends(get_user)]`.

Sync functions may only depend on sync providers, validated at
decoration time. Async functions may mix sync and async providers.
Generator and async generator functions are supported.

`override` and `overrides` temporarily replace providers' values within
a scope, e.g. substituting a fake connection in tests. Overrides are
context-local, so concurrent tasks do not interfere with each other.

Example:
    .. code-block:: python

        from stratae.depends import Depends, Injected, inject, override

        def get_db() -> Database:
            return Database()

        @inject
        def list_users(db: Injected[Database, Depends(get_db)]) -> list[User]:
            return db.query(User)

        list_users()  # db resolved by calling get_db()

        with override(get_db, FakeDatabase()):
            list_users()  # db is the fake within this scope

"""

from .inject import Depends, DependsWrapper, Injected, inject
from .override import override, overrides

__all__ = [
    "Depends",
    "DependsWrapper",
    "override",
    "overrides",
    "Injected",
    "inject",
]
