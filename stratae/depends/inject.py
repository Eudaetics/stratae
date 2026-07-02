"""
Inject decorator to resolve and inject dependencies into functions.

This module provides:
- A global Resolver instance for managing dependencies.
- The `inject` decorator for resolving and injecting dependencies into functions.
"""

from typing import Annotated, Any, Callable, overload

from stratae.depends.resolver import Resolver

_resolver = Resolver()

Injected = Annotated
"""Alias Annotated for clearer semantics when typing a parameter that will be injected."""


def get_resolver() -> Resolver:
    """Get the global dependency resolver."""
    return _resolver


@overload
def inject[F: Callable[..., Any]](*, sig: F) -> Callable[[Callable[..., Any]], F]: ...


@overload
def inject[R](func: Callable[..., R]) -> Callable[..., R]: ...


@overload
def inject[R]() -> Callable[[Callable[..., R]], Callable[..., R]]: ...


def inject(
    func: Callable[..., Any] | None = None,
    *,
    sig: Callable[..., Any] | None = None,
) -> Any:
    """
    Inject decorator to resolve dependencies for a function.

    By default the decorated function is typed as `Callable[..., R]` — its
    parameters are not preserved. Injected parameters are stripped at
    resolution time and there's no way to describe that generically. The
    sig option is provided as a fallback to correct the signature for use
    in type checking and IDE support. To give the decorated function a
    precise signature, pass a stub function via `sig`. The decorated
    function is typed as `sig`, not as the original function:

        def _foo_sig(a: int) -> int: ...

        @inject(sig=_foo_sig)
        def foo(a: int, b: Injected[int, Depends(get_b)]) -> int: ...
    """

    def decorator(f: Callable[..., Any]) -> Any:
        return get_resolver().resolve_function(f)

    if sig is not None or func is None:
        return decorator
    return decorator(func)
