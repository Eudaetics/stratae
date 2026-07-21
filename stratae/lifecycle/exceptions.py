"""
Exceptions for errors in lifecycle management.

All inherit from {py:exc}`LifecycleException`. {py:exc}`LifecycleConfigurationError`
is raised for invalid scope configuration, such as a duplicate or malformed
scope declaration. {py:exc}`ScopeNotFoundError` and {py:exc}`ScopeInactiveError`
are raised when looking up a scope that was never declared, or one that has
no active activation in the calling context, respectively.
{py:exc}`ScopeActivationError` is raised when deactivating a scope whose
activation has already ended.
"""


class LifecycleException(Exception):
    """Base class for all lifecycle related exceptions."""


class LifecycleConfigurationError(LifecycleException, ValueError):
    """
    Exception raised for configuration errors in the lifecycle management.

    Covers scope declarations that are invalid on their own terms - a name
    that isn't a valid Python identifier, an unrecognized isolation or
    storage kind - as well as configuration errors that only surface across
    the full set of scopes, such as duplicate names or an empty scope list.
    """


class ScopeNotFoundError(LifecycleException, ValueError):
    """
    Exception raised when a requested scope is not found in the lifecycle manager.

    Raised when a scope name is referenced that was never declared on the
    lifecycle manager, as distinct from {py:exc}`ScopeInactiveError`, which
    covers a scope that was declared but has no active activation.
    """


class ScopeActivationError(LifecycleException, RuntimeError):
    """
    Exception raised when there is an error activating or deactivating a scope.

    Raised when deactivating a scope using a token whose activation is no
    longer the current one - for example, deactivating out of order relative
    to how the scopes were activated.
    """


class ScopeInactiveError(LifecycleException, RuntimeError):
    """
    Exception raised when attempting to access an inactive scope.

    Raised when a scope is declared on the lifecycle manager but has no
    active activation in the calling context, as distinct from
    {py:exc}`ScopeNotFoundError`, which covers a scope name that was never
    declared at all.
    """
