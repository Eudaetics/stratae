"""
Hierarchical, scope-based caching and resource lifecycles for applications.

Declare {py:class}`Scope <stratae.lifecycle.scope.Scope>` (sync) or
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` (async) objects directly -
e.g. "application", "request" - and activate one as a context manager via
{py:meth}`Scope.activate <stratae.lifecycle.scope.Scope.activate>` (`async with` for
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`). While active, functions
decorated with {py:meth}`Scope.cache <stratae.lifecycle.scope.Scope.cache>` have their
results cached for the lifetime of that activation, computed once per scope activation
rather than once per process. A scope can declare another scope as a parent with
`requires`; activating it then raises unless that parent scope is already active.

Each {py:class}`Scope <stratae.lifecycle.scope.Scope>` chooses its own isolation and
storage independently:

* `isolation="shared"`: one cache visible to every thread/task while the
  scope is active, suited to application-wide state such as connection pools.
* `isolation="context"` (default): a cache isolated per execution context, backed by
  a `contextvars.ContextVar`, suited to request- or session-scoped state.
* `storage`: `"dense"` (default) or `"sparse"`; see
  {py:class}`Scope <stratae.lifecycle.scope.Scope>` for the tradeoff between them.

{py:func}`resource <stratae.lifecycle.resource.resource>` and
{py:func}`async_resource <stratae.lifecycle.resource.async_resource>` mark a generator
function as a context manager to auto-enter when cached. Exiting the associated scope's
activation causes its cleanup (closing a connection, committing changes) to run without
every caller having to handle it explicitly.

````{example} Application- and request-scoped resources
```{code-block} python
from stratae.lifecycle import Scope, resource

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

application = Scope("application", isolation="shared")
request = Scope("request", requires=application)

@application.cache()
@resource
def get_db():
    conn = Connection()
    try:
        yield conn
    finally:
        conn.close()

@request.cache()
@resource
def get_transaction():
    txn = Transaction(get_db())
    try:
        yield txn
    finally:
        txn.close()

with application.activate():
    with request.activate():
        get_transaction()
        get_transaction()  # same transaction within this request

    with request.activate():
        get_transaction()  # new request, new transaction, same connection

```
```{output}
Opening connection
begin transaction
commit
begin transaction
commit
Closing connection
```
````

See {py:class}`Scope <stratae.lifecycle.scope.Scope>`,
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>`,
{py:func}`resource <stratae.lifecycle.resource.resource>`, and
{py:func}`async_resource <stratae.lifecycle.resource.async_resource>` for the
rest of the module's API.
"""

from .resource import async_resource, resource
from .scope import AsyncScope, Scope

__all__ = ["AsyncScope", "Scope", "async_resource", "resource"]
