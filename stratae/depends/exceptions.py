"""Exceptions for errors in dependency injection, subclassing {py:exc}`DependencyInjectionError`."""


class DependencyInjectionError(Exception):
    """Base class for all dependency injection exceptions."""


class ResolutionError(DependencyInjectionError):
    """Base class for failures resolving a function's injected dependencies."""


class CircularDependencyError(ResolutionError):
    """Raised when providers depend on each other in a cycle."""


class InjectionSignatureError(ResolutionError, ValueError):
    """Raised when an injected function's signature can't be injected."""


class DependencyNotFoundError(DependencyInjectionError, ValueError):
    """Raised when a callable was never registered with `Depends`."""
