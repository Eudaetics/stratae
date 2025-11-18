"""Exceptions related to lifecycle management."""


class LifecycleException(Exception):
    """Base class for all lifecycle related exceptions."""


class LifecycleConfigurationError(LifecycleException, ValueError):
    """Exception raised for configuration errors in the lifecycle management."""


class ScopeNotFoundError(LifecycleException, ValueError):
    """Exception raised when a requested scope is not found in the lifecycle manager."""


class ScopeActivationError(LifecycleException, RuntimeError):
    """Exception raised when there is an error activating or deactivating a scope."""


class ScopeInactiveError(LifecycleException, RuntimeError):
    """Exception raised when attempting to access an inactive scope."""
