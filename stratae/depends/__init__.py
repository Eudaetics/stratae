"""Dependency Injection Module."""

from stratae.depends.depends import Depends, DependsWrapper
from stratae.depends.inject import Injected, inject
from stratae.depends.resolver import Resolver

__all__ = [
    "Resolver",
    "Depends",
    "DependsWrapper",
    "Injected",
    "inject",
]
