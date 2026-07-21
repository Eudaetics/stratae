"""
Stratae: Composable tools for building applications in Python.

Stratae provides lightweight, high-performance building blocks for composing
applications: dependency injection, lifecycle-scoped caching and cleanup,
guard checks, typed events, context variables, and serialization. Built on
Python's native features, it works anywhere: APIs, CLIs, workers, and tests.

```{code-block} python
:caption: Cache a database connection

import pytest
from stratae.depends import Depends, Injected, inject
from stratae.lifecycle import Lifecycle, Scope

class Database:
    def __init__(self, url: str):
        self.url = url
        self.users: set[str] = set()

    def create_user(self, name: str) -> str:
        self.users.add(name)
        return name

lifecycle = Lifecycle([Scope("application", "shared"), Scope("request", "shared")])

@lifecycle.cache("application")
def get_database() -> Database:
    return Database(url="postgresql://localhost/app")

@inject
def create_user(name: str, db: Injected[Database, Depends(get_database)]):
    return db.create_user(name)

@inject
def get_user(name: str, db: Injected[Database, Depends(get_database)]) -> str:
    assert name in db.users, f"no such user: {name}"
    return name

with lifecycle.start("application"):
    with lifecycle.start("request"):
        user = create_user("Alice")
    assert get_user("Alice") == "Alice"

    with pytest.raises(AssertionError, match="no such user"):
        get_user("Bob")

with lifecycle.start("application"), pytest.raises(AssertionError, match="no such user"):
    get_user("Alice")
```
"""

from . import checks, context, depends, events, integrations, lifecycle, serde

__all__ = ["depends", "lifecycle", "context", "events", "checks", "integrations", "serde"]
