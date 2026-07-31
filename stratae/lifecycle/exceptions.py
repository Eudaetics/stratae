"""
Exceptions for errors in lifecycle management.

All inherit from {py:exc}`LifecycleException`. {py:exc}`LifecycleConfigurationError`
is raised for invalid scope configuration, such as an unrecognized isolation or
storage value, or a `requires` combination that isn't allowed.
{py:exc}`ScopeInactiveError` is raised when accessing a scope that has no
active activation in the calling context. {py:exc}`ScopeActivationError` is
raised when deactivating a scope whose activation has already ended, or one a
dependent scope still requires active.
"""


class LifecycleException(Exception):
    """Base class for all lifecycle related exceptions."""


class LifecycleConfigurationError(LifecycleException, ValueError):
    """
    Exception raised for configuration errors in a scope's declaration.

    Covers a name that isn't a valid Python identifier, an unrecognized
    isolation or storage kind, and a `requires` combination that isn't
    allowed - a `"shared"` scope declaring `requires` on a
    `"context"`-isolated scope.
    """


class ScopeNotFoundError(LifecycleException, ValueError):
    """
    Exception raised when a requested scope cannot be found.

    Distinct from {py:exc}`ScopeInactiveError`, which covers a scope that
    exists but has no active activation.
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

    Raised when a scope has no active activation in the calling context, as
    distinct from {py:exc}`ScopeNotFoundError`, which covers a scope
    reference that can't be found at all.
    """
