"""Exceptions for errors in dependency injection."""


class DependencyInjectionError(Exception):
    """Base class for all dependency injection related exceptions."""


class ResolutionError(DependencyInjectionError):
    """Exception raised when resolving a function's injected dependencies fails."""


class CircularDependencyError(ResolutionError):
    """Exception raised when a circular dependency is detected."""


class InjectionSignatureError(ResolutionError, ValueError):
    """Exception raised when an injected function's signature is invalid."""


class DependencyNotFoundError(DependencyInjectionError, ValueError):
    """Raised when attempting to reference a non-existent dependency."""
