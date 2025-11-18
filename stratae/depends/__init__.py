"""Dependency Injection Module."""

from stratae.depends.depends import AUTO, Depends, DependsWrapper
from stratae.depends.inject import inject
from stratae.depends.resolver import Resolver

__all__ = [
    "Resolver",
    "AUTO",
    "Depends",
    "DependsWrapper",
    "inject",
]
