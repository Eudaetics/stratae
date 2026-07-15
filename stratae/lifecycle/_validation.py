"""Validation logic for Lifecycle and AsyncLifecycle constructor arguments."""

from typing import Sequence

from stratae.lifecycle.exceptions import LifecycleConfigurationError
from stratae.lifecycle.scope import Scope


def validate_config(scopes: Sequence[Scope]) -> None:
    """Validate scopes for a lifecycle configuration."""
    if not scopes:
        raise LifecycleConfigurationError("At least one scope must be defined.")
    if len({scope.name for scope in scopes}) != len(scopes):
        raise LifecycleConfigurationError("All scopes must be unique.")
