"""
Exceptions for errors in dependency injection.

All inherit from {py:exc}`DependencyInjectionError`. {py:exc}`ResolutionError`
is the shared base for failures encountered while resolving a function's
injected dependencies: a dependency cycle
({py:exc}`CircularDependencyError`) or an invalid injected signature
({py:exc}`InjectionSignatureError`). {py:exc}`DependencyNotFoundError` is
raised separately, for a reference to a provider that was never
registered with `Depends`.
"""


class DependencyInjectionError(Exception):
    """
    Base class for all dependency injection related exceptions.

    Catch this to handle any dependency-injection failure without
    distinguishing the specific cause.
    """


class ResolutionError(DependencyInjectionError):
    """
    Exception raised when resolving a function's injected dependencies fails.

    Shared base for {py:exc}`CircularDependencyError` and
    {py:exc}`InjectionSignatureError`; catch this to handle either case
    the same way.
    """


class CircularDependencyError(ResolutionError):
    """
    Exception raised when a circular dependency is detected.

    Raised while resolving injected parameters, when a provider depends
    on itself, directly or through a chain of other providers.
    """


class InjectionSignatureError(ResolutionError, ValueError):
    """
    Exception raised when an injected function's signature is invalid.

    Covers an injected parameter that also declares a default value, and
    a sync function that depends on an async provider.
    """


class DependencyNotFoundError(DependencyInjectionError, ValueError):
    """
    Raised when attempting to reference a non-existent dependency.

    The callable was never wrapped in `Depends`, so no provider is
    registered for it to override or resolve.
    """
