"""
Scope definitions for hierarchical lifecycle scoping and cache isolation.

A `Scope` is a plain configuration value; it performs no caching itself.
Pass a sequence of them to `Lifecycle`/`AsyncLifecycle`, which builds the
storage and activation machinery each scope's `isolation` and `storage`
choices describe.
"""

from dataclasses import dataclass
from typing import Literal, get_args

from stratae.lifecycle.exceptions import LifecycleConfigurationError

IsolationType = Literal["shared", "context"]
StorageType = Literal["dense", "sparse"]


@dataclass(frozen=True, slots=True)
class Scope:
    """
    Definition of a scope and configuration for its cache isolation behavior.

    A Scope is a configuration object consumed by lifecycle managers to
    determine how cached values are stored and shared while that scope is
    active.

    Args:
        name: Identifier for the scope (e.g. "request", "application"),
              used by lifecycle managers to look up and manage this scope.
        isolation: Cache isolation strategy for this scope. "shared" uses a
                   single cache visible to all concurrent tasks/threads while
                   the scope is active, regardless of execution context -
                   suitable for application-wide state such as database pools.
                   "context" isolates the cache per execution context, backed
                   by a ContextVar, so concurrent contexts (e.g. concurrent
                   requests) each see their own cache - suitable for request-
                   or session-scoped state.
        storage: Slot storage strategy for this scope's cached values. "dense"
                  indexes slots directly by position - the cheapest per-access
                  cost, but every activation pays to copy/reset the full slot
                  list, so it fits scopes with few registered functions or
                  where most of them get used per activation. "sparse"
                  allocates slots lazily and resets in O(touched) rather than
                  O(registered) - the fit for scopes registering many
                  functions where a given activation only touches a handful,
                  e.g. a large API's per-resource caches.

    Choosing storage:
        Storage defaults to dense. Below ~50 registered functions, dense wins outright
        regardless of touched count - allocating a dict already costs more than copying
        the whole list. Above that, it's the touched/registered ratio that decides: dense
        and sparse roughly break even around 1-4% touched (2% at 1,000 registered / 20
        touched), sparse pulling ahead below it (~4x faster at 1,000 registered / 0
        touched) and dense pulling ahead above it (~1.5x faster at 1,000 registered / 90
        touched).

    For example::

    ```python
    from stratae.lifecycle import Lifecycle, Scope

    lifecycle = Lifecycle([
        Scope("application", isolation="shared"),
        Scope("request", isolation="context", storage="sparse"),
    ])
    ```

    """

    name: str
    isolation: IsolationType
    storage: StorageType = "dense"

    def __post_init__(self):
        """
        Validate the name, isolation, and storage values are acceptable for scoping.

        Raises:
            LifecycleConfigurationError: If `name` is not a valid Python
                identifier, or `isolation`/`storage` is not one of their
                allowed values.

        """
        if not self.name.isidentifier():
            raise LifecycleConfigurationError("All scopes must be valid Python identifiers.")
        if self.isolation not in frozenset(get_args(IsolationType)):
            raise LifecycleConfigurationError(f"Invalid scope isolation given for {self.name}.")
        if self.storage not in frozenset(get_args(StorageType)):
            raise LifecycleConfigurationError(f"Invalid scope storage given for {self.name}.")
