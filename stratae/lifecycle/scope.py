"""
Scope definitions for hierarchical lifecycle scoping and cache isolation.

A {py:class}`Scope` is a plain configuration value. It performs no caching itself. Pass
a sequence of them to {py:class}`Lifecycle <stratae.lifecycle.lifecycle.Lifecycle>` or
{py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>` to build the
storage and activation machinery each scope's `isolation` and `storage` choices
describe.

````{example} Shared vs. context scope isolation
```{code-block} python
import asyncio
from uuid import UUID, uuid4
from stratae.lifecycle import AsyncLifecycle, Scope

lifecycle = AsyncLifecycle(
    [
        Scope("application", isolation="shared"),
        Scope("request", storage="sparse"),
    ]
)

@lifecycle.cache("application")
async def get_connection_pool_id() -> UUID:
    return uuid4()

@lifecycle.cache("request")
async def get_request_id() -> UUID:
    return uuid4()

async def handle_request() -> tuple[UUID, UUID]:
    async with lifecycle.start("request"):
        return await get_connection_pool_id(), await get_request_id()

async def main() -> None:
    async with lifecycle.start("application"):
        # two requests handled concurrently, not one after another
        (pool1, req1), (pool2, req2) = await asyncio.gather(
            handle_request(), handle_request()
        )

    print("same pool id across concurrent requests:", pool1 is pool2)
    print("same request id across concurrent requests:", req1 is req2)

asyncio.run(main())
```
```{output}
same pool id across concurrent requests: True
same request id across concurrent requests: False
```
````
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
