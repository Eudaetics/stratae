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

````{example} Injecting a repository and swapping in a test mock
```{code-block} python
from stratae.depends import Depends, Injected, inject, override

class UserRepository:
    def __init__(self, users: dict[int, str]):
        self._users = users

    def get_name(self, user_id: int) -> str:
        return self._users[user_id]

def get_user_repository() -> UserRepository:
    return UserRepository({1: "Jane Doe"})

@inject
def greet(
    user_id: int, repo: Injected[UserRepository, Depends(get_user_repository)]
) -> str:
    print(f"Hello, {repo.get_name(user_id)}!")

greet(user_id=1)

mock_repo = UserRepository({1: "Test User"})
with override(get_user_repository, mock_repo):
    greet(user_id=1)

greet(user_id=1)
```
```{output}
Hello, Jane Doe!
Hello, Test User!
Hello, Jane Doe!
```
````

See {py:func}`Depends <stratae.depends.inject.Depends>`,
{py:func}`inject <stratae.depends.inject.inject>`,
{py:func}`override <stratae.depends.override.override>`, and
{py:func}`overrides <stratae.depends.override.overrides>` for the rest
of the module's API.

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
