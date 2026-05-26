"""Validation logic for Lifecycle and AsyncLifecycle constructor arguments."""

from typing import Sequence

from stratae.cache.cache import Cache
from stratae.lifecycle.exceptions import LifecycleConfigurationError


def validate_config(scopes: Sequence[str], caches: dict[str, Cache] | None) -> None:
    """Validate scopes and caches for a lifecycle configuration."""
    _validate_scopes(scopes)
    _validate_caches(scopes, caches)


def _validate_scopes(scopes: Sequence[str]) -> None:
    """Validate that scopes are non-empty, unique, and valid Python identifiers."""
    if not scopes:
        raise LifecycleConfigurationError("At least one scope must be defined.")
    if any(not scope.isidentifier() for scope in scopes):
        raise LifecycleConfigurationError("All scopes must be valid Python identifiers.")
    if len(set(scopes)) != len(scopes):
        raise LifecycleConfigurationError("All scopes must be unique.")


def _validate_caches(scopes: Sequence[str], caches: dict[str, Cache] | None) -> None:
    """Validate that all cache keys correspond to defined scopes."""
    if caches and any(key not in scopes for key in caches.keys()):
        raise LifecycleConfigurationError("All caches must correspond to defined scopes.")
