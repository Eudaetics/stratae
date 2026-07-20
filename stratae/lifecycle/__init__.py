"""
Hierarchical, scope-based caching and resource lifecycles for applications.

Declare `Scope` objects (e.g. "application", "request") and register them with a
`Lifecycle` (sync) or `AsyncLifecycle` (async). Activate a scope as a context
manager via ``lifecycle.start(name)``. While active, functions decorated with
``@lifecycle.cache(name)`` have their results cached for the lifetime of that
activation, computed once per scope activation rather than once per process.

Each `Scope` chooses its own isolation and storage independently:

* ``isolation="shared"``: one cache visible to every thread/task while the
  scope is active, suited to application-wide state such as connection pools.
* ``isolation="context"``: a cache isolated per execution context, backed by
  a `contextvars.ContextVar`, suited to request- or session-scoped state.
* ``storage``: ``"dense"`` (default) or ``"sparse"``; see {py:class}`Scope`
  for the tradeoff between them

`resource` and `async_resource` mark a generator function as a context manager
to auto-enter when cached. Exiting the associated lifecycle scope causes
its cleanup (closing a connection, committing changes) to run without every
caller having to handle it explicitly.

For example::

```python
from stratae.lifecycle import Lifecycle, Scope, resource

lifecycle = Lifecycle([Scope("application", "shared"), Scope("request", "context")])

@lifecycle.cache("application")
@resource
def get_db():
    conn = Database.connect()
    try:
        yield conn
    finally:
        conn.close()

with lifecycle.start("application"):
    with lifecycle.start("request"):
        conn = get_db()  # opened once per "application" activation
```
"""

from .lifecycle import AsyncLifecycle, Lifecycle
from .resource import async_resource, resource
from .scope import Scope

__all__ = ["AsyncLifecycle", "Lifecycle", "Scope", "async_resource", "resource"]
