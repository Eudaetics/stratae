"""Dependency Injection Module."""

from .inject import Depends, Injected, inject
from .override import override, overrides

__all__ = [
    "Depends",
    "override",
    "overrides",
    "Injected",
    "inject",
]
