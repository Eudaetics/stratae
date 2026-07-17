"""Dependency Injection Module."""

from stratae.depends.inject import Depends, DependsWrapper, Injected, inject
from stratae.depends.override import override, overrides

__all__ = [
    "Depends",
    "DependsWrapper",
    "override",
    "overrides",
    "Injected",
    "inject",
]
