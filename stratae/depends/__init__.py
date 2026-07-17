"""Dependency Injection Module."""

from stratae.depends.depends import Depends, DependsWrapper
from stratae.depends.inject import Injected, inject
from stratae.depends.override import override, overrides

__all__ = [
    "Depends",
    "DependsWrapper",
    "override",
    "overrides",
    "Injected",
    "inject",
]
