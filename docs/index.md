# Stratae

A toolkit for Python 3.12+, not a framework: dependency injection, lifecycle-scoped caching/cleanup, and event handling. Each piece is simple enough to use alone, or compose them together to enable powerful features.

::::{container} hero-buttons

:::{button-ref} getting-started
:color: primary
:ref-type: doc

Get Started
:::

:::{button-ref} api-reference
:color: secondary
:outline:
:ref-type: doc

API Reference
:::

:::{button-link} https://github.com/Eudaetics/stratae
:color: secondary
:outline:

GitHub
:::

::::

::::{grid} 1 2 3 3
:gutter: 4

:::{grid-item-card}
:link: dependency-injection.html

{octicon}`plug;1.5em` **Dependency Injection**
^^^
Works like a normal Python function. No container to configure, no registration step. Swap a dependency out for tests without restructuring your code.
:::

:::{grid-item-card}
:link: lifecycle.html

{octicon}`sync;1.5em` **Lifecycle**
^^^
Cache expensive calls and clean up resources automatically, scoped to blocks you define. You decide where a scope starts and ends; nothing gets inferred from global state or a request object.
:::

:::{grid-item-card}
:link: events.html

{octicon}`mail;1.5em` **Events**
^^^
Decouple your code with events, in-process by default. Both pub/sub and request/reply are built in. The same code can send distributed messages later, no rewrite needed.
:::

:::{grid-item-card}
:link: context.html

{octicon}`package;1.5em` **Zero Dependencies**
^^^
Pure Python, no external packages. Nothing else gets added to your dependency tree along with Stratae, and the core modules don't depend on each other either. Dependency injection doesn't pull in lifecycle or events.
:::

:::{grid-item-card}
:link: checks.html

{octicon}`terminal;1.5em` **Use Anywhere**
^^^
No framework required. Every piece is a plain callable, so it drops into a script, a CLI, a worker, or whatever framework you're already using with minimal integration.
:::

:::{grid-item-card}
:link: serde.html

{octicon}`unlock;1.5em` **No Lock-In**
^^^
There is no container or registry to stand up first. Add dependency injection, a cached scope, or an event to only where it is needed. Back out the same way by inlining or reverting to the previous design.
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
