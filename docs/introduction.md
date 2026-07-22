# Introduction

Stratae is a set of small, focused developer tools for Python 3.12+, covering three problems that show up in almost every non-trivial application:

- **Dependency injection** (`stratae.depends`) — wire a function's inputs to providers instead of constructing them inline.
- **Lifecycle management** (`stratae.lifecycle`) — scope caching and cleanup to a unit of work, like a request, a job, or an application run.
- **Events** (`stratae.events`) — define pub/sub and request/reply messages independently of whatever bus delivers them.

Around these three, a handful of supporting modules solve smaller, adjacent problems: `stratae.context` (scoped values usable directly as DI providers), `stratae.checks` (declarative precondition running), `stratae.serde` (encode/decode primitives), and `stratae.integrations` (bridges to ASGI, msgspec, and RabbitMQ).

## Design philosophy

Each tool works on its own. You can use `stratae.lifecycle` for scoped caching without ever touching `stratae.depends`, or use `stratae.depends` for injection without any lifecycle scoping at all. None of the core modules import each other. Where they do compose — a cached provider resolved through injection, a handler that's both an event responder and an injected function — it's because ordinary Python composes that way, not because of special glue code. `stratae.events`, for instance, has zero code-level dependency on `stratae.depends` or `stratae.lifecycle`: an injected function is just a plain callable, and a plain callable is all a bus needs to register a handler.

## How these guides are organized

Each guide covers one module end to end: the core concepts, the way its pieces compose in practice, and the mistakes that are easy to make. They build loosely on each other — the dependency injection and lifecycle guides are worth reading before the events guide, since the richest examples combine all three — but each is written to stand alone if you only need one tool.

Start with [Getting Started](getting-started) for a five-minute working example, then read the guides in whatever order matches what you're building. The [API reference](api-reference) has the full signature-level detail for everything covered here.
