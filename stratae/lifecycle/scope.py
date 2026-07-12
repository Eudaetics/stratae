"""Scope definition for hierarchical lifecycle scoping and cache isolation configuration."""

from dataclasses import dataclass
from typing import Literal, get_args

from stratae.lifecycle.exceptions import LifecycleConfigurationError

IsolationType = Literal["shared", "context"]


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

    """

    name: str
    isolation: IsolationType

    def __post_init__(self):
        """Validate the name and isolation values are acceptable for scoping."""
        if not self.name.isidentifier():
            raise LifecycleConfigurationError("All scopes must be valid Python identifiers.")
        if self.isolation not in frozenset(get_args(IsolationType)):
            raise LifecycleConfigurationError(f"Invalid scope isolation given for {self.name}.")
