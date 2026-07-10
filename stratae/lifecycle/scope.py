"""Scope definition for hierarchical lifecycle scoping and cache behavior configuration."""

from dataclasses import dataclass, field
from typing import Literal

from stratae.cache import Cache, MemoryCache

IsolationType = Literal["none", "context"]


@dataclass(frozen=True, slots=True)
class Scope:
    """
    Definition of a scope and configuration for cache isolation behavior.

    A Scope is a configuration object consumed by lifecycle managers to
    determine how cached values are stored and shared while that scope is
    active.

    Args:
        name: Identifier for the scope (e.g. "request", "application"),
              used by lifecycle managers to look up and manage this scope.
        isolation: Cache isolation strategy for this scope. "none" shares
                   a single cache across all concurrent tasks/threads (suitable
                   for application-wide state). "context" isolates the cache per
                   execution context, backed by a ContextVar, so concurrent
                   contexts (e.g. concurrent requests) each see their own cache
                   (suitable for request- or session-scoped state).
        cache: The Cache instance used to store values for this scope.
               Defaults to a new MemoryCache.

    """

    name: str
    isolation: IsolationType
    cache: Cache = field(default_factory=MemoryCache)
