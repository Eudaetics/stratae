"""Exceptions for errors in lifecycle management, all subclassing {py:exc}`LifecycleException`."""


class LifecycleException(Exception):
    """Base class for all lifecycle exceptions."""


class LifecycleConfigurationError(LifecycleException, ValueError):
    """Raised for an invalid scope declaration."""


class ScopeActivationError(LifecycleException, RuntimeError):
    """Raised when a scope can't be activated or deactivated."""


class ScopeInactiveError(LifecycleException, RuntimeError):
    """Raised when using a scope that isn't active in the calling context."""


class CacheCycleError(LifecycleException, RuntimeError):
    """Raised when a cached call would wait on its own unfinished computation."""
