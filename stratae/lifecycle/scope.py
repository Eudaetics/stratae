"""
Scope definitions for hierarchical lifecycle scoping and cache isolation.

A {py:class}`Scope` is a plain configuration value. It performs no caching itself. Pass
a sequence of them to {py:class}`Lifecycle <stratae.lifecycle.lifecycle.Lifecycle>` or
{py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>` to build the
storage and activation machinery each scope's `isolation` and `storage` choices
describe.

```{rubric} Example:
```
```{code-block} python
:caption: A shared scope caches once for the process; a context scope caches once per activation

from stratae.lifecycle import Lifecycle, Scope

lifecycle = Lifecycle(
    [
        Scope("application", isolation="shared"),
        Scope("request", storage="sparse"),
    ]
)

@lifecycle.cache("application")
def get_connection_pool() -> object:
    return object()

@lifecycle.cache("request")
def get_request_context() -> object:
    return object()

with lifecycle.start("application"):
    with lifecycle.start("request"):
        pool_first, request_first = get_connection_pool(), get_request_context()
    with lifecycle.start("request"):
        pool_second, request_second = get_connection_pool(), get_request_context()

# "shared": same cached pool across both "request" activations
assert pool_first is pool_second

# "context": a fresh instances per "request" activation
assert request_first is not request_second
```

See {py:class}`Scope` for additional examples.
"""

from dataclasses import dataclass
from typing import Literal, get_args

from stratae.lifecycle.exceptions import LifecycleConfigurationError

IsolationType = Literal["shared", "context"]
StorageType = Literal["dense", "sparse"]


@dataclass(frozen=True, slots=True)
class Scope:
    """
    Configuration for a lifecycle scope's cache isolation and storage behavior.

    A `Scope` is a plain configuration object consumed by lifecycle managers -
    {py:class}`Lifecycle <stratae.lifecycle.lifecycle.Lifecycle>` and
    {py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>` - to
    determine how cached values are stored and shared while that scope is active.

    Storage defaults to dense. Below ~50 registered functions, dense wins outright
    regardless of touched count - allocating a dict already costs more than copying the
    whole list. Above that, it's the touched/registered ratio that decides: dense and
    sparse roughly break even around 1-4% touched (2% at 1,000 registered / 20 touched),
    sparse pulling ahead below it (~4x faster at 1,000 registered / 0 touched) and dense
    pulling ahead above it (~1.5x faster at 1,000 registered / 90 touched).

    :param name: Identifier for the scope (e.g. `"request"`, `"application"`), used by
        lifecycle managers to look up and manage this scope. Must be a valid Python
        identifier.
    :param isolation: Cache isolation strategy for this scope, one of the
        {py:data}`IsolationType` values. `"shared"` uses a single cache visible to all
        concurrent tasks/threads while the scope is active, regardless of execution
        context - suitable for application-wide state such as database pools.
        `"context"` (the default) isolates the cache per execution context, backed by
        a `contextvars.ContextVar`, so concurrent contexts (e.g. concurrent requests)
        each see their own cache - suitable for request- or session-scoped state.
    :param storage: Slot storage strategy for this scope's cached values, one of the
        {py:data}`StorageType` values. `"dense"` (the default) indexes slots directly
        by position - the cheapest per-access cost, but every activation pays to
        copy/reset the full slot list, so it fits scopes with few registered
        functions or where most of them get used per activation. `"sparse"` allocates
        slots lazily and resets in O(touched) rather than O(registered) - the fit for
        scopes registering many functions where a given activation only touches a
        handful, e.g. a large API's per-resource caches.
    :raises LifecycleConfigurationError: If `name` is not a valid Python identifier, or
        `isolation`/`storage` is not one of their allowed values.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Declare an application-wide shared scope and a per-request context-isolated scope

    from stratae.lifecycle import Lifecycle, Scope

    lifecycle = Lifecycle(
        [
            Scope("application", isolation="shared"),
            Scope("request", isolation="context", storage="sparse"),
        ]
    )

    with lifecycle.start("application"):
        with lifecycle.start("request"):
            pass  # both scopes active, each isolating/storing per its own configuration
    ```

    """

    name: str
    isolation: IsolationType = "context"
    storage: StorageType = "dense"

    def __post_init__(self):
        """Validate the name, isolation, and storage values are acceptable for scoping."""
        if not self.name.isidentifier():
            raise LifecycleConfigurationError("All scopes must be valid Python identifiers.")
        if self.isolation not in frozenset(get_args(IsolationType)):
            raise LifecycleConfigurationError(f"Invalid scope isolation given for {self.name}.")
        if self.storage not in frozenset(get_args(StorageType)):
            raise LifecycleConfigurationError(f"Invalid scope storage given for {self.name}.")
