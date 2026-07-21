"""
Hierarchical, scope-based caching and resource lifecycles for applications.

Declare {py:class}`Scope <stratae.lifecycle.scope.Scope>` objects (e.g. "application",
"request") and register them with a {py:class}`Lifecycle <stratae.lifecycle.lifecycle.Lifecycle>`
(sync) or {py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>` (async).
Activate a scope as a context manager via
{py:meth}`Lifecycle.start <stratae.lifecycle.lifecycle.Lifecycle.start>`. While active,
functions decorated with
{py:meth}`Lifecycle.cache <stratae.lifecycle.lifecycle.Lifecycle.cache>` have their
results cached for the lifetime of that activation, computed once per scope activation
rather than once per process.

Each {py:class}`Scope <stratae.lifecycle.scope.Scope>` chooses its own isolation and
storage independently:

* `isolation="shared"`: one cache visible to every thread/task while the
  scope is active, suited to application-wide state such as connection pools.
* `isolation="context"`: a cache isolated per execution context, backed by
  a `contextvars.ContextVar`, suited to request- or session-scoped state.
* `storage`: `"dense"` (default) or `"sparse"`; see
  {py:class}`Scope <stratae.lifecycle.scope.Scope>` for the tradeoff between them.

{py:func}`resource <stratae.lifecycle.resource.resource>` and
{py:func}`async_resource <stratae.lifecycle.resource.async_resource>` mark a generator
function as a context manager to auto-enter when cached. Exiting the associated
lifecycle scope causes its cleanup (closing a connection, committing changes) to run
without every caller having to handle it explicitly.

```{rubric} Example:
```
```{code-block} python
:caption: Open a database connection once per application, closing it when the scope exits

from stratae.lifecycle import Lifecycle, Scope, resource

class Connection:
    open_count = 0

    def __init__(self):
        Connection.open_count += 1
        self.closed = False

    def close(self):
        self.closed = True

lifecycle = Lifecycle([Scope("application", "shared"), Scope("request", "context")])

@lifecycle.cache("application")
@resource
def get_db():
    conn = Connection()
    try:
        yield conn
    finally:
        conn.close()

with lifecycle.start("application"):
    with lifecycle.start("request"):
        conn = get_db()
    with lifecycle.start("request"):
        conn_again = get_db()  # same connection, not reopened

    assert conn is conn_again
    assert Connection.open_count == 1
    assert not conn.closed

assert conn.closed  # closed once the "application" activation ends
```

See {py:class}`Lifecycle <stratae.lifecycle.lifecycle.Lifecycle>`,
{py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>`,
{py:class}`Scope <stratae.lifecycle.scope.Scope>`,
{py:func}`resource <stratae.lifecycle.resource.resource>`, and
{py:func}`async_resource <stratae.lifecycle.resource.async_resource>` for additional
examples.
"""

from .lifecycle import AsyncLifecycle, Lifecycle
from .resource import async_resource, resource
from .scope import Scope

__all__ = ["AsyncLifecycle", "Lifecycle", "Scope", "async_resource", "resource"]
