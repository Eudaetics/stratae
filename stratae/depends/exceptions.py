"""Exceptions for errors in dependency injection, subclassing {py:exc}`DependencyInjectionError`."""

from collections.abc import Callable
from typing import Any


class DependencyInjectionError(Exception):
    """Base class for all dependency injection exceptions."""


class ResolutionError(DependencyInjectionError):
    """Base class for failures resolving a function's injected dependencies."""


class CircularDependencyError(ResolutionError):
    """Raised when providers depend on each other in a cycle."""

    def __init__(self, func: Callable[..., Any]) -> None:
        """:param func: The provider that depends on itself, directly or not."""
        super().__init__(f"Circular dependency detected for {func}.")


class InjectionSignatureError(ResolutionError, ValueError):
    """Raised when an injected function's signature can't be injected."""


class UnhashableDependencyError(InjectionSignatureError):
    """Raised when a dependency can't be registered because it's unhashable."""

    def __init__(self, dependency: object) -> None:
        """:param dependency: The unhashable dependency."""
        super().__init__(
            f"Cannot use {dependency!r} as a dependency: it's unhashable. "
            "Wrap it in a function instead."
        )


class UninjectableCallableError(InjectionSignatureError):
    """Raised when injecting into a callable that isn't a function, method, or class."""

    def __init__(self, func: object) -> None:
        """:param func: The callable."""
        super().__init__(
            f"Cannot inject into {func!r}: inject supports functions, methods, and classes. "
            "Decorate its __call__ method with @inject instead, or wrap it in a function."
        )


class PostponedAnnotationsError(InjectionSignatureError):
    """Raised when injecting into a function from a module with postponed annotations."""

    def __init__(self, func: Callable[..., Any]) -> None:
        """:param func: The function."""
        super().__init__(
            f"{func.__qualname__} is from a module using 'from __future__ import annotations', "
            "which inject doesn't support: its annotations are strings, so injected "
            "parameters can't be found."
        )


class StringAnnotationError(InjectionSignatureError):
    """Raised when an injected parameter's dependency is in a string annotation."""

    def __init__(self, func: Callable[..., Any], parameter: str) -> None:
        """
        Name the function and its quoted parameter.

        :param func: The function.
        :param parameter: The parameter with the string annotation.
        """
        super().__init__(
            f"{func.__qualname__} parameter {parameter!r} has a string annotation, which "
            "inject doesn't support: it reads annotation objects, so the dependency "
            "can't be found. Unquote it, or remove 'from __future__ import annotations'."
        )


class InjectedDefaultError(InjectionSignatureError):
    """Raised when an injected parameter also has a default."""

    def __init__(self, parameter: str) -> None:
        """:param parameter: The parameter."""
        super().__init__(f"Cannot use a default with injected parameter {parameter}")


class AsyncDependencyError(InjectionSignatureError):
    """Raised when a sync function has async dependencies."""

    def __init__(self, func: Callable[..., Any]) -> None:
        """:param func: The sync function."""
        super().__init__(f"Sync function '{func.__name__}' cannot have async dependencies.")


class DependencyNotFoundError(DependencyInjectionError, ValueError):
    """Raised when a callable was never registered with `Depends`."""

    def __init__(self, func: Callable[..., Any]) -> None:
        """:param func: The callable."""
        super().__init__(f"No Dependency found for {func}")


class EmptyOverridesError(DependencyInjectionError, ValueError):
    """Raised when {py:func}`overrides <stratae.depends.overrides>` is given no overrides."""

    def __init__(self) -> None:
        """Explain that at least one override is needed."""
        super().__init__("overrides() requires at least one entry.")
