# Tutorials

Guides to each piece of Stratae, from installation through the individual modules.

```{toctree}
:maxdepth: 1

getting-started
dependency-injection
lifecycle
events
context
checks
serde
integrations/index
```

- **[Getting Started](getting-started)** — install Stratae and get oriented.
- **[Dependency Injection](dependency-injection)** — `stratae.depends` wires a function's parameters to provider callables, resolved at call time.
- **[Lifecycle](lifecycle)** — `stratae.lifecycle` scopes caching and cleanup to a unit of work — a request, a background job, an application run.
- **[Events](events)** — `stratae.events` separates *what a message is* from *how it gets delivered*.
- **[Context](context)** — `stratae.context` wraps a `contextvars.ContextVar` in a small, named, callable object.
- **[Checks](checks)** — `stratae.checks` runs a set of preconditions and raises if they don't hold.
- **[Serde](serde)** — `stratae.serde` turns arbitrary Python objects into bytes and back.
- **[Integrations](integrations/index)** — bridges the core modules to specific third-party tools.
