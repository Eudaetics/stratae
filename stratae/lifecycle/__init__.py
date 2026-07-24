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

````{example} Application- and request-scoped resources
```{code-block} python
from stratae.lifecycle import Lifecycle, Scope, resource

class Connection:
    def __init__(self):
        print("Opening connection")

    def close(self):
        print("Closing connection")

class Transaction:
    def __init__(self, conn):
        print("begin transaction")

    def close(self):
        print("commit")

lifecycle = Lifecycle(
    [Scope("application", "shared"), Scope("request", "context")]
)

@lifecycle.cache("application")
@resource
def get_db():
    conn = Connection()
    try:
        yield conn
    finally:
        conn.close()

@lifecycle.cache("request")
@resource
def get_transaction():
    txn = Transaction(get_db())
    try:
        yield txn
    finally:
        txn.close()

with lifecycle.start("application"):
    with lifecycle.start("request"):
        get_transaction()
        get_transaction()  # same transaction within this request

    with lifecycle.start("request"):
        get_transaction()  # new request, new transaction, same connection

```
```{container} example-output
Opening connection
begin transaction
commit
begin transaction
commit
Closing connection
```
````

See {py:class}`Lifecycle <stratae.lifecycle.lifecycle.Lifecycle>`,
{py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>`,
{py:class}`Scope <stratae.lifecycle.scope.Scope>`,
{py:func}`resource <stratae.lifecycle.resource.resource>`, and
{py:func}`async_resource <stratae.lifecycle.resource.async_resource>` for the
rest of the module's API.
"""

from .lifecycle import AsyncLifecycle, Lifecycle
from .resource import async_resource, resource
from .scope import Scope

__all__ = ["AsyncLifecycle", "Lifecycle", "Scope", "async_resource", "resource"]
