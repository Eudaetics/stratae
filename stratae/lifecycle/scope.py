"""Scope definition for hierarchical lifecycle scoping and cache behavior configuration."""

from dataclasses import dataclass, field
from typing import Literal

from stratae.cache import Cache, MemoryCache

IsolationType = Literal["none", "context"]


@dataclass(frozen=True)
class Scope:
    """Definition of a scope and configuration for cache isolation behavior."""

    name: str
    isolation: IsolationType
    cache: Cache = field(default_factory=MemoryCache)
