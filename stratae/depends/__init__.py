"""Dependency Injection Module."""

from .inject import Depends, DependsWrapper, Injected, inject
from .override import override, overrides

__all__ = [
    "Depends",
    "DependsWrapper",
    "override",
    "overrides",
    "Injected",
    "inject",
]
