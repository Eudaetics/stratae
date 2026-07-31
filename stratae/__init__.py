"""
Stratae: Composable tools for building applications in Python.

Stratae provides lightweight, high-performance building blocks for composing
applications: dependency injection, lifecycle-scoped caching and cleanup,
guard checks, typed events, context variables, and serialization. Built on
Python's native features, it works anywhere: APIs, CLIs, workers, and tests.

```{code-block} python
:caption: Cache a database connection

import pytest
from typing import Annotated
from stratae.depends import Depends, inject
from stratae.lifecycle import Scope

class Database:
    def __init__(self, url: str):
        self.url = url
        self.users: set[str] = set()

    def create_user(self, name: str) -> str:
        self.users.add(name)
        return name

application = Scope("application", isolation="shared")
request = Scope("request", isolation="shared")

@application.cache()
def get_database() -> Database:
    return Database(url="postgresql://localhost/app")

@inject
def create_user(name: str, db: Annotated[Database, Depends(get_database)]):
    return db.create_user(name)

@inject
def get_user(name: str, db: Annotated[Database, Depends(get_database)]) -> str:
    assert name in db.users, f"no such user: {name}"
    return name

with application.activate():
    with request.activate():
        user = create_user("Alice")
    assert get_user("Alice") == "Alice"

    with pytest.raises(AssertionError, match="no such user"):
        get_user("Bob")

with application.activate(), pytest.raises(AssertionError, match="no such user"):
    get_user("Alice")
```
"""
