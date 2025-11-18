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


@overload
def inject[**P, R](func: Callable[P, R], *, allow_override: bool) -> Callable[P, R]: ...


@overload
def inject[**P, R](*, allow_override: bool) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def inject[**P, R](
    func: Callable[P, R] | None = None, *, allow_override: bool = True
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Inject decorator to resolve dependencies for a function."""

    def decorator(f: Callable[P, R]) -> Callable[P, R]:
        return get_resolver().resolve_function(f, allow_override=allow_override)

    if func is None:
        return decorator
    return decorator(func)
