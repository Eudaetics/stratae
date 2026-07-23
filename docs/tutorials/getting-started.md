# Getting Started

Install Stratae:

```bash
pip install stratae
```

Wire up a cached dependency and inject it into a function:

```python
from stratae.depends import Depends, Injected, inject
from stratae.lifecycle import Lifecycle, Scope

lifecycle = Lifecycle([Scope("application", "shared")])

type Database = dict[str, list[dict[str, str]]]

# Simple database connection (just a dict for demo)
@lifecycle.cache('application')
def get_database() -> Database:
    return {"users": []}

@inject
def create_user(name: str, db: Injected[Database, Depends(get_database)]):
    user = {"name": name}
    db["users"].append(user)
    return user

with lifecycle.start('application'):
    user = create_user("Alice")
    print(f"Created user: {user['name']}")
```

From here, the [Dependency Injection](dependency-injection) and [Lifecycle](lifecycle) guides go deeper into `stratae.depends` and `stratae.lifecycle`; the [API reference](../api-reference) has the full signature-level detail.
