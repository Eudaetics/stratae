"""Dependency Injection Module."""

from stratae.depends.depends import AUTO, Depends, DependsWrapper
from stratae.depends.inject import Injected, inject
from stratae.depends.resolver import Resolver

__all__ = [
    "Resolver",
    "AUTO",
    "Depends",
    "DependsWrapper",
    "Injected",
    "inject",
]
