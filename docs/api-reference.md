# API Reference

Stratae is organized around three core tools — dependency injection, lifecycle management, and events — plus a few supporting modules used across all three.

## Core

### Dependency Injection

Mark a parameter with `Injected[T, Depends(provider)]` and decorate the function with `inject`: the parameter is resolved by calling its provider at call time. Supports sync and async providers, and temporary overrides for testing.

{doc}`Full reference <apidocs/stratae.depends/stratae.depends>`

### Lifecycle

Manage hierarchical, scoped contexts for caching and cleanup — `Lifecycle`/`AsyncLifecycle`, `Scope`, and `resource`/`async_resource` for resources that need teardown.

{doc}`Full reference <apidocs/stratae.lifecycle/stratae.lifecycle>`

### Events

Event definitions, bound-event facades, and dispatch protocols for pub/sub and request/reply messaging.

{doc}`Full reference <apidocs/stratae.events/stratae.events>`

## Supporting modules

### Context

Callable, injectable values backed by `contextvars` — set once with `.set()` or `with ctx.use(value):`, read anywhere within that scope, and usable directly as a `Depends()` provider.

{doc}`Full reference <apidocs/stratae.context/stratae.context>`

### Checks

Run collections of zero-argument checks, raising or gathering their failures — `check`, `check_async`, and the `require` decorator.

{doc}`Full reference <apidocs/stratae.checks/stratae.checks>`

### Serde

Serialization and deserialization tools for encoding/decoding data — `encode`, `pack`, `unpack_json`.

{doc}`Full reference <apidocs/stratae.serde/stratae.serde>`

### Integrations

Bridges between Stratae modules and third-party tools: event adapters (RabbitMQ), lifecycle integrations (ASGI), and serde integrations (msgspec).

{doc}`Full reference <apidocs/stratae.integrations/stratae.integrations>`

```{toctree}
:hidden:

depends <apidocs/stratae.depends/stratae.depends>
lifecycle <apidocs/stratae.lifecycle/stratae.lifecycle>
events <apidocs/stratae.events/stratae.events>
checks <apidocs/stratae.checks/stratae.checks>
context <apidocs/stratae.context/stratae.context>
serde <apidocs/stratae.serde/stratae.serde>
integrations <apidocs/stratae.integrations/stratae.integrations>
```
