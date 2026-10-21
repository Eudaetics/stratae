# Introduction

Most frameworks optimize for *easy*. They are approachable, and they get you moving fast by deciding a lot upfront. Often that speed comes from tangling your application code together with their way of doing things. For some projects, that never matters. However, if requirements change enough, or vendors change policies or pricing, or the code needs to run in a new environment, then modifying or untangling can get complex or expensive.

Stratae aims for easy, but optimizes for *simple*[^simple-made-easy]: components not tangled together, instead they are composable. Dependency injection, lifecycle management, and events are three small, independent tools. None of them know about each other. You choose whether and how to combine them. You choose how they integrate into your code instead of integrating your code into a framework. Composed well, they can make A/B tests, feature flags, and per-tenant behavior straightforward.

## The three core tools

`stratae.depends` is function-based dependency injection rather than container-based. Many DI systems in Python borrow the concept from other languages: a container you configure, abstract interfaces with registered concrete implementations, and an object graph wired together. Instead, `stratae.depends` takes marked parameters and resolves them at call time. There's no container, registration step, or graph that's constructed. Users of FastAPI `Depends` will find this familiar because it is a similar mechanism. It is just generalized here beyond request handlers. Any function can declare an injected parameter.

`stratae.lifecycle` is lifecycle management. Scope caching and cleanup to a unit of work so that a resource opens once per scope and tears down automatically when the scope ends.

`stratae.events` is event definitions. Define pub/sub and request/reply messages independent of whatever bus eventually delivers them or whatever handler answers them. The same event definition works whether you bind it to an in-process `DirectBus` today or a distributed broker like RabbitMQ later. Growing from one to the other only requires updating to the new adapter's configurations, not rewriting the event definition, call sites, or the code inside the handler.

A handful of supporting modules solve smaller, adjacent problems. `stratae.context` gives you scoped values usable directly as DI providers. `stratae.checks` runs declarative preconditions. `stratae.serde` handles encode/decode primitives. `stratae.integrations` is what makes that growth path real. RabbitMQ turns the same events into a distributed bus, FastAPI and Starlette scope lifecycles to actual requests, and msgspec speeds up serialization.

## Design philosophy

Each tool works on its own. You can use `stratae.lifecycle` for scoped caching without touching `stratae.depends`. You can use `stratae.depends` for injection without any lifecycle scoping at all. None of the core modules import each other. Where they do compose, it's because ordinary Python composes that way, not because of special glue code. A cached provider can be resolved through injection. A handler can be both an event responder and an injected function even though `stratae.events` has zero code-level dependency on `stratae.depends` or `stratae.lifecycle`. An injected function is just a plain callable, and a plain callable is all a bus needs to register a handler.

## How these guides are organized

Each guide covers one module end to end. It walks through the core concepts and the way its pieces compose in practice. The guides build loosely on each other. Read the dependency injection and lifecycle guides before events. However, since each core module is independent, you can focus on only the small parts you need.

Start with [Getting Started](tutorials/getting-started) to install Stratae and see examples. The [API reference](api-reference) has the full signature-level detail for everything covered here.

[^simple-made-easy]: Rich Hickey, "Simple Made Easy" (talk), 2011.
