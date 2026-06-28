"""
Inject decorator to resolve and inject dependencies into functions.

This module provides:
- A global Resolver instance for managing dependencies.
- The `inject` decorator for resolving and injecting dependencies into functions.
"""

from __future__ import annotations

from typing import Callable, overload

from stratae.depends.resolver import Resolver

_resolver = Resolver()


def get_resolver() -> Resolver:
    """Get the global dependency resolver."""
    return _resolver


@overload
def inject[**P, R](func: Callable[P, R]) -> Callable[P, R]: ...


@overload
def inject[**P, R]() -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def inject[**P, R](
    func: Callable[P, R] | None = None,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Inject decorator to resolve dependencies for a function."""

    def decorator(f: Callable[P, R]) -> Callable[P, R]:
        return get_resolver().resolve_function(f)

    if func is None:
        return decorator
    return decorator(func)
