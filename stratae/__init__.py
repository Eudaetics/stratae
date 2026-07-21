"""
Stratae: Composable tools for building applications in Python.

Stratae provides lightweight, high-performance tools for dependency injection,
lifecycle management, and context variables. Built on Python's native features,
it works anywhere: APIs, CLIs, workers, and tests.

```python
from stratae.depends import Depends, Injected, inject
from stratae.lifecycle import Lifecycle, Scope

lifecycle = Lifecycle([Scope("application", "shared"), Scope("request", "shared")])


@lifecycle.cache("application")
def get_database():
    return Database(url="postgresql://...")


@inject
def create_user(name: str, db: Injected[Database, Depends(get_database)]):
    return db.users.create(name=name)


with lifecycle.start("application"):
    with lifecycle.start("request"):
        user = create_user("Alice")
```

Modules:
    check: Guard checks for controlling behavior
    depends: Dependency injection and resolution
    lifecycle: Scope-based caching and resource management
    context: Context variables with nested scopes
    events: Typed events with pub/sub and request/reply dispatch
    serde: Serialization and deserialization
    integrations: Bridges between Stratae modules and third party tools
"""

from . import checks, context, depends, events, integrations, lifecycle, serde

__all__ = ["depends", "lifecycle", "context", "events", "checks", "integrations", "serde"]
