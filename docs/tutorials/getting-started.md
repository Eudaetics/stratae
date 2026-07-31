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

`resource` marks `get_connection` as a generator instead of a plain function. Wrapped this way, whatever comes after `yield` runs automatically when the `"job"` scope ends. Cleanup doesn't have to be called by hand. If the scope ends cleanly, that's a commit. If something raised instead, that's a rollback. Either way, the connection closes.

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

The insert never gets committed. `get_connection`'s `except` clause rolls back instead, then closes the connection. `raise` lets the error keep going after that, so the caller still sees it too.

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

So far `add_user` and `list_users` run the query themselves. `stratae.events` moves that. `add_user` and `list_users` become the event, and a handler registered separately is what actually touches the cursor. Neither one calls `get_cursor` anymore:

````{example} Driving the writes and reads through events
```{code-block} python
from stratae.events import DirectBus, Event, PubSub, Request

class UserAdded:
    def __init__(self, name: str) -> None:
        self.name = name

class UsersRequested:
    pass

user_added = Event(PubSub, UserAdded)
users_requested = Event(Request[list[tuple[int, str]]], UsersRequested)

bus = DirectBus()
add_user = bus.bind(user_added, factory=UserAdded)
list_users = bus.bind(users_requested, factory=UsersRequested)

@bus.handle(user_added)
@inject
def _(e: UserAdded, cursor: Cursor) -> None:
    cursor.execute("INSERT INTO users (name) VALUES (?)", (e.name,))

@bus.handle(users_requested)
@inject
def _(request: UsersRequested, cursor: Cursor) -> list[tuple[int, str]]:
    return cursor.execute("SELECT id, name FROM users").fetchall()

with job.activate():
    add_user(name="Alice")
    add_user(name="Bob")
    print(list_users())
```
```{output}
[(1, 'Alice'), (2, 'Bob')]
```
````

`add_user(name="Alice")` no longer runs an `INSERT` inline. It constructs a `UserAdded` payload and hands it to the bus, which calls whatever's registered for it. `PubSub` fans a fire-and-forget write out to any number of handlers; `Request[list[tuple[int, str]]]` requires exactly one responder and blocks for its return value, which is how `list_users()` still gets rows back. Swapping `DirectBus` for a real broker, or adding a second `user_added` handler, doesn't touch `add_user` or `list_users` at all.

For a longer worked example that grows from a plain script into one using each of these modules, plus `Checks`, see the [Project Walkthrough](walkthrough). The [API reference](../api-reference) has the full signature-level detail.
