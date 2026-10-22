"""
Hierarchical, scope-based caching and resource lifecycles for applications.

Create {py:class}`Scope <stratae.lifecycle.scope.Scope>` or
{py:class}`AsyncScope <stratae.lifecycle.scope.AsyncScope>` objects, such as "application"
and "request", and activate them with `with`. Functions decorated with
{py:meth}`Scope.cache <stratae.lifecycle.scope.Scope.cache>` are computed once per
activation. {py:func}`resource <stratae.lifecycle.resource.resource>` and
{py:func}`async_resource <stratae.lifecycle.resource.async_resource>` mark generator
functions whose cleanup runs when the scope deactivates.

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

with application:
    with request:
        get_transaction()
        get_transaction()  # same transaction within this request

    with request:
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
"""

from .resource import async_resource, resource
from .scope import AsyncScope, Scope

__all__ = ["AsyncScope", "Scope", "async_resource", "resource"]
