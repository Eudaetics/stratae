"""
Dependency injection via annotated parameters and the `inject` decorator.

Annotate a parameter with `Annotated[T, Depends(provider)]` and decorate the function
with {py:func}`inject <stratae.depends.injection.inject>`; callers no longer pass it.
{py:func}`override <stratae.depends.overriding.override>` and
{py:func}`overrides <stratae.depends.overriding.overrides>` temporarily replace providers,
e.g. with test mocks.

````{example} Injecting a repository and swapping in a test mock
```{code-block} python
from typing import Annotated
from stratae.depends import Depends, inject, override

class UserRepository:
    def __init__(self, users: dict[int, str]):
        self._users = users

    def get_name(self, user_id: int) -> str:
        return self._users[user_id]

def get_user_repository() -> UserRepository:
    return UserRepository({1: "Jane Doe"})

@inject
def greet(
    user_id: int, repo: Annotated[UserRepository, Depends(get_user_repository)]
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
"""

from .injection import Depends, inject
from .overriding import override, overrides

__all__ = [
    "Depends",
    "override",
    "overrides",
    "inject",
]
