# Stratae

Stratae is a set of developer tools for Python 3.12+. It covers dependency injection, lifecycle-scoped caching and cleanup, and events. Each tool works on its own: use lifecycle management without injection, or dependency injection by itself.

::::{grid} 1 2 3 3
:gutter: 3

:::{grid-item-card} 🔌 Dependency Injection
:link: dependency-injection.html

Wires a function's parameters to provider callables, resolved at call time.
:::

:::{grid-item-card} ♻️ Lifecycle
:link: lifecycle.html

Scopes caching and cleanup to a unit of work — a request, a job, a run.
:::

:::{grid-item-card} 📬 Events
:link: events.html

Separates what a message is from how it gets delivered.
:::

:::{grid-item-card} 🧵 Context
:link: context.html

A named, callable wrapper around `contextvars.ContextVar`.
:::

:::{grid-item-card} ✅ Checks
:link: checks.html

Runs a set of preconditions and raises if they don't hold.
:::

:::{grid-item-card} 📦 Serde
:link: serde.html

Turns arbitrary Python objects into bytes and back.
:::

:::{grid-item-card} 🔗 Integrations
:link: integrations/index.html

Bridges the core modules to specific third-party tools.
:::

::::

```{toctree}
:maxdepth: 2
:caption: Guides
:hidden:

introduction
getting-started
dependency-injection
lifecycle
events
context
checks
serde
integrations/index
```

```{toctree}
:maxdepth: 2
:caption: Reference
:hidden:

api-reference
```
