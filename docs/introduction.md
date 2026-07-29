# Introduction

Most frameworks optimize for *easy*. They get you moving fast by deciding a lot upfront, and that speed comes from tangling your application code together with their way of doing things. For some projects, that tangle never matters. However, if your business needs change enough, or you need the same code to run somewhere else, unwinding it gets expensive.

Stratae optimizes for *simple* instead, in the sense Rich Hickey draws out in "Simple Made Easy": not tangled together, not necessarily the fastest thing to pick up on day one. Dependency injection, lifecycle management, and events are three small, independent tools. None of them know about each other. You choose whether and how to combine them.

Combined well, they can make A/B tests, feature flags, and per-tenant behavior straightforward. Say a `FetchFile` request is answered by reading from whichever tenant's storage backend is active for the call. Every call site that calls `fetch_file` stays the same no matter which tenant it's for, no facade or adapter layer to maintain, and no branch anywhere in the handler for which tenant it is. Dependency injection is what lets that handler receive whatever client it needs without constructing it by hand. Lifecycle management scopes that client to the right unit of work, a request, a job, an application run, so it opens once and tears down automatically.

## The three core tools

`stratae.depends` is dependency injection. Mark a parameter as injected and it gets resolved from a provider at call time. There's no container to configure, or registration steps.

`stratae.lifecycle` is lifecycle management. Scope caching and cleanup to a unit of work, so a resource opens once per scope and tears down automatically when the scope ends.

`stratae.events` is event definitions. Define pub/sub and request/reply messages as plain Python, independent of whatever bus eventually delivers them or whatever handler answers them. The same event definition works whether you bind it to an in-process `DirectBus` today or a distributed broker like RabbitMQ later. Growing from one to the other means updating the bind and the handler registration with the new adapter's config, not rewriting the event definition or the code inside the handler.

A handful of supporting modules solve smaller, adjacent problems. `stratae.context` gives you scoped values usable directly as DI providers. `stratae.checks` runs declarative preconditions. `stratae.serde` handles encode/decode primitives. `stratae.integrations` is what makes that growth path real. RabbitMQ turns the same events into a distributed bus, FastAPI and Starlette scope lifecycles to actual requests, and msgspec speeds up serialization.

## Design philosophy

Each tool works on its own. You can use `stratae.lifecycle` for scoped caching without touching `stratae.depends`. You can use `stratae.depends` for injection without any lifecycle scoping at all. None of the core modules import each other. Where they do compose, it's because ordinary Python composes that way, not because of special glue code. A cached provider can be resolved through injection. A handler can be both an event responder and an injected function. `stratae.events` has zero code-level dependency on `stratae.depends` or `stratae.lifecycle`. An injected function is just a plain callable, and a plain callable is all a bus needs to register a handler.

Here's all three working together end to end. Once it's wired up, the calling code stays simple. There's no conditional branching for which tenant or which user, no framework request object anywhere in sight. The same setup runs identically in an API, CLI script, a worker, or a REPL.

````{example} Wiring events, injection, and lifecycle together
```{code-block} python
from typing import Annotated, Protocol
from stratae.context import Context
from stratae.depends import Depends, inject
from stratae.events import DirectBus, Event, PubSub, Request
from stratae.lifecycle import Lifecycle, Scope, resource

class FetchFile:
    def __init__(self, filename: str) -> None:
        self.filename = filename

class FileAccessed:
    def __init__(self, filename: str, source: str) -> None:
        self.filename = filename
        self.source = source

bus = DirectBus()
fetch_file_event = Event(Request[str], FetchFile)
file_accessed_event = Event(PubSub, FileAccessed)
fetch_file = bus.bind(fetch_file_event, factory=FetchFile)
notify_accessed = bus.bind(file_accessed_event, factory=FileAccessed)

lifecycle = Lifecycle([Scope("application", "shared")])

class AuditLog:
    def __init__(self) -> None:
        self.entries: list[str] = []

    def record(self, filename: str, user: str, source: str) -> None:
        self.entries.append(f"{user}: {source}://{filename}")

@lifecycle.cache("application")
@resource
def get_audit_log():
    yield AuditLog()

class Storage(Protocol):
    source: str

    def read(self, filename: str) -> str: ...

class LocalDisk:
    source = "file"

    def read(self, filename: str) -> str:
        return f"[{filename} from local disk] Lorem ipsum dolor sit amet"

class S3:
    source = "s3"

    def read(self, filename: str) -> str:
        return f"[{filename} from s3] consectetur adipiscing elit"

storage = Context[Storage]("storage", default=LocalDisk())
current_user = Context("current_user", default="guest")

@bus.handle(fetch_file_event)
@inject
def handle_fetch(
    cmd: FetchFile, backend: Annotated[Storage, Depends(storage)]
) -> str:
    content = backend.read(cmd.filename)
    notify_accessed(filename=cmd.filename, source=backend.source)
    return content

@bus.handle(file_accessed_event)
@inject
def record_audit_entry(
    accessed: FileAccessed,
    audit_log: Annotated[AuditLog, Depends(get_audit_log)],
    user: Annotated[str, Depends(current_user)],
) -> None:
    audit_log.record(accessed.filename, user, accessed.source)

file = "report.csv"
with lifecycle.start("application"):
    print(fetch_file(filename=file))

    with storage.use(S3()), current_user.use("alice"):
        print(fetch_file(filename=file))

    print(fetch_file(filename=file))
    print(get_audit_log().entries)
```
```{output}
[report.csv from local disk] Lorem ipsum dolor sit amet
[report.csv from s3] consectetur adipiscing elit
[report.csv from local disk] Lorem ipsum dolor sit amet
['guest: file://report.csv', 'alice: s3://report.csv', 'guest: file://report.csv']
```
````

## How these guides are organized

Each guide covers one module end to end. It walks through the core concepts and the way its pieces compose in practice. The guides build loosely on each other. Read the dependency injection and lifecycle guides before the events guide, since the richest examples combine all three. Each guide still stands on its own if you only need one tool.

Start with [Getting Started](tutorials/getting-started) to install Stratae, then the [Project Walkthrough](tutorials/walkthrough) for a five-minute working example. The [API reference](api-reference) has the full signature-level detail for everything covered here.
