# Getting Started


Stratae is made of three primary independent modules: [Dependency Injection](dependency-injection), [Lifecycle](lifecycle.md), and [Events](events.md), plus [Checks](checks.md), [Context](context.md), and [Serde](serde.md) utilities.

## Install

::::{tab-set}

:::{tab-item} uv
```bash
uv add stratae
```
:::

:::{tab-item} pip
```bash
pip install stratae
```
:::

::::

## Example

The example below builds a small script that stores and lists users in a sqlite database, introducing Lifecycle, Dependency Injection, and Events in turn.

### Lifecycle

First, set up a connection that commits when the job succeeds and rolls back automatically when it doesn't.

````{example} Committing on success, rolling back on error
```{code-block} python
import sqlite3
from stratae.lifecycle import Scope, resource

job = Scope("job")

@job.cache()
@resource
def get_connection():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("CREATE TABLE audit_log (admin TEXT, name TEXT)")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
```
````

`Scope("job")` declares a named unit of work. "job" is just a name picked for this example. You can choose anything appropriate to your requirements. Here, it's one activation of the whole script. The scope itself is what starts and stops it, later on, via `job.activate()`.

`@job.cache()` ties `get_connection` to that scope. Call it once inside a `job` activation and it runs. Call it again inside the same activation and it returns the same connection instead of opening a new one.

`resource` serves two purposes. It's an alias for creating a `contextmanager` and noting it should be entered automatically. Wrapped this way, the function returns the actual cached value instead of a generator. Cleanup doesn't have to be called by hand. If the scope ends cleanly, that's a commit. If something raised instead, that's a rollback. Either way, the connection closes.

````{example} A failing job rolls back instead of committing
```{code-block} python
try:
    with job.activate():
        conn = get_connection()
        conn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        raise RuntimeError("something failed before the job finished")
except RuntimeError as e:
    print(f"job failed, rolled back: {e}")
```
```{output}
job failed, rolled back: something failed before the job finished
```
````

The insert never gets committed. `get_connection`'s `except` clause rolls back instead, then closes the connection.

### Injection

Next, set up dependencies so functions don't have to create their own resources.

````{example} Injecting a cursor into functions that use it
```{code-block} python
from typing import Annotated
from stratae.depends import Depends, inject

@inject
def get_cursor(
    conn: Annotated[sqlite3.Connection, Depends(get_connection)]
) -> sqlite3.Cursor:
    return conn.cursor()

type Cursor = Annotated[sqlite3.Cursor, Depends(get_cursor)]

@inject
def add_user(name: str, cursor: Cursor) -> None:
    cursor.execute("INSERT INTO users (name) VALUES (?)", (name,))

@inject
def list_users(cursor: Cursor) -> list[tuple[int, str]]:
    return cursor.execute("SELECT id, name FROM users").fetchall()

with job.activate():
    add_user("Alice")
    add_user("Bob")
    print(list_users())
```
```{output}
[(1, 'Alice'), (2, 'Bob')]
```
````

`Depends(provider)` marks a parameter as injected. `Annotated[T, Depends(provider)]` says what type it resolves to and which provider produces it. `@inject` is what wires it up at call time.

`get_cursor` is a provider itself. It depends on `get_connection`, and hands back a fresh cursor from that connection. `type Cursor = Annotated[sqlite3.Cursor, Depends(get_cursor)]` names that dependency once. Using that type means `add_user` and `list_users` can just write `cursor: Cursor` instead of repeating the full `Annotated[...]` each time. Changing the dependency within that type also updates every place using `Cursor` automatically.

`add_user` and `list_users` depend on `get_cursor` the same way. Neither one takes a connection as a parameter, or calls `get_connection` directly. They only ever see a `sqlite3.Cursor`.

### Events

So far `add_user` decides how a user gets stored. It is coupled to a Cursor object, and only runs the `INSERT` itself. `stratae.events` can separate those concerns. `add_user` becomes a command, add this user, dispatched as a `Request`: exactly one handler, `persist_user`, answers it, and `add_user` blocks until `persist_user` returns. `persist_user` emits a separate `PubSub` event, `UserAdded`, once the write succeeds, and anything that needs to react to a user having been added, like the audit log, subscribes to that.

````{example} Separating the command to add a user from the fact that one was added
```{code-block} python
from stratae.context import Context
from stratae.events import DirectBus, Event, PubSub, Request

class AddUser:
    def __init__(self, name: str) -> None:
        self.name = name

class UserAdded:
    def __init__(self, name: str) -> None:
        self.name = name

add_user_event = Event(Request[None], AddUser)
user_added_event = Event(PubSub, UserAdded)
users_requested = Event(Request[list[tuple[int, str]]])

bus = DirectBus()
add_user = bus.bind(add_user_event, factory=AddUser)
notify_user_added = bus.bind(user_added_event, factory=UserAdded)
list_users = bus.bind(users_requested)

current_user = Context[str]("cur_user")

@bus.handle(add_user_event)
@inject
def persist_user(cmd: AddUser, cursor: Cursor) -> None:
    cursor.execute("INSERT INTO users (name) VALUES (?)", (cmd.name,))
    notify_user_added(name=cmd.name)

@bus.handle(user_added_event)
@inject
def record_audit(
    added: UserAdded, cursor: Cursor, admin: Annotated[str, Depends(current_user)]
) -> None:
    cursor.execute(
        "INSERT INTO audit_log (admin, name) VALUES (?, ?)", (admin, added.name)
    )

@bus.handle(users_requested)
@inject
def fetch_users(cursor: Cursor) -> list[tuple[int, str]]:
    return cursor.execute("SELECT id, name FROM users").fetchall()

with job.activate(), current_user.use("Steve"):
    add_user(name="Alice")
    add_user(name="Bob")
    print(list_users())
```
```{output}
[(1, 'Alice'), (2, 'Bob')]
```
````

Calling `add_user` blocks until `persist_user` returns, so the INSERT has already run against the connection by the time it comes back, though it isn't durable until the job commits at the end of `job.activate()`. `persist_user` only emits `UserAdded` once that INSERT has run, so `record_audit` can never fire for a write that raised instead. Because `record_audit` is reacting to that event instead of being called directly, adding another listener later, a welcome email, say, means registering a new handler, not editing `persist_user`.

## Conclusion

With Lifecycle, Injection, and Events in place, extending this script is a matter of adding, not editing: a new handler can subscribe to `user_added_event` without touching `persist_user`, and a new provider can replace `get_connection` without touching `add_user` or `list_users`.

For a longer worked example that grows from a plain script into one using each of these modules, see the [Project Walkthrough](walkthrough). The [API reference](../api-reference) has the full signature-level detail.
