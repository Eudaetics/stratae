"""Exceptions for errors in dependency injection."""


class DependencyInjectionError(Exception):
    """Base class for all dependency injection related exceptions."""


class DIResolutionError(DependencyInjectionError):
    """Exception raised when dependency resolution fails."""


class CircularDependencyError(DependencyInjectionError):
    """Exception raised when a circular dependency is detected."""


class RegistrationError(DependencyInjectionError, ValueError):
    """Exception raised when a registration error occurs in dependency resolution."""


class OverrideNotAllowedError(DependencyInjectionError, RuntimeError):
    """Exception raised when an override is attempted but not allowed."""
