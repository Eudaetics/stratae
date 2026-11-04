"""
Exceptions for errors in lifecycle management, all subclassing {py:exc}`LifecycleException`.

Each kind of failure has its own class, under one of these bases:

- {py:exc}`LifecycleConfigurationError`: an invalid declaration.
- {py:exc}`ScopeActivationError`: a scope that can't be activated or deactivated.
- {py:exc}`ScopeInactiveError`: a scope used outside its activation.
- {py:exc}`CacheCycleError`: a cached call waiting on itself.
- {py:exc}`ResourceGeneratorError`: a resource generator that doesn't yield exactly once.

Failures closing a scope are raised together as a {py:exc}`CleanupErrors` group. It's a
{py:exc}`BaseCleanupErrors` group instead if any is a `BaseException` such as
`CancelledError`.
"""

from collections.abc import Callable, Sequence
from typing import Any, Literal, Self


class LifecycleException(Exception):
    """Base class for all lifecycle exceptions."""


class LifecycleConfigurationError(LifecycleException, ValueError):
    """Raised for an invalid scope declaration."""


class InvalidScopeOptionError(LifecycleConfigurationError):
    """Raised when a scope is given an isolation or storage it doesn't support."""

    def __init__(self, name: str, option: Literal["isolation", "storage"]) -> None:
        """
        Name the scope and the invalid option.

        :param name: The scope's name.
        :param option: Which option is invalid.
        """
        super().__init__(f"Invalid scope {option} given for {name}.")


class SharedScopeThreadsError(LifecycleConfigurationError):
    """Raised when a shared scope is given `threads=True`."""

    def __init__(self, name: str) -> None:
        """:param name: The scope's name."""
        super().__init__(
            f"Shared scope {name!r} cannot take threads=True: it's already thread-safe."
        )


class SharedRequiresContextError(LifecycleConfigurationError):
    """Raised when a shared scope requires a context-isolated one."""

    def __init__(self, name: str, required: str) -> None:
        """
        Name the shared scope and the scope it requires.

        :param name: The shared scope's name.
        :param required: The context-isolated scope's name.
        """
        super().__init__(
            f"Shared scope {name!r} cannot require context-isolated scope {required!r}."
        )


class AlreadyCachedError(LifecycleConfigurationError):
    """Raised when caching a function that's already cached in another scope."""

    def __init__(self, func: Callable[..., Any], cached_in: str, scope: str) -> None:
        """
        Name the function and both scopes.

        :param func: The function.
        :param cached_in: The scope it's cached in.
        :param scope: The scope it was being cached in too.
        """
        super().__init__(
            f"{func!r} is already cached in scope {cached_in!r}; cannot also cache it in {scope!r}."
        )


class UncacheableError(LifecycleConfigurationError):
    """Raised when caching an object that can't hold attributes, such as a bound method."""

    def __init__(self, func: Callable[..., Any], scope: str) -> None:
        """
        Name the object and the scope.

        :param func: The object.
        :param scope: The scope it was being cached in.
        """
        super().__init__(
            f"Cannot cache {func!r} in scope {scope!r}: builtins, bound methods, and other "
            "objects that can't hold attributes can't be cached. Wrap it in a function instead."
        )


class CacheKeyConflictError(LifecycleConfigurationError):
    """Raised when a cache decorator is given both `cache_key` and `ignore_params`."""

    def __init__(self) -> None:
        """Explain the conflict."""
        super().__init__("Cannot use both ignore_params and cache_key together.")


class CrossLoopWaitError(LifecycleConfigurationError):
    """Raised when awaiting a cached call another thread's event loop is computing."""

    def __init__(self) -> None:
        """Explain the scope it needs."""
        super().__init__(
            "Cached call awaited on a different event loop than the one computing it. "
            "Event loops on other threads sharing an activation need a scope created with "
            "threads=True."
        )


class AsyncHookError(LifecycleConfigurationError, TypeError):
    """Raised when a sync scope is given an async hook or callback, which it can't await."""

    def __init__(self, name: str, hook: Callable[..., Any]) -> None:
        """
        Name the scope and the hook.

        :param name: The scope's name.
        :param hook: The async hook or callback.
        """
        super().__init__(
            f"Scope {name!r} runs its hooks synchronously and can't await {hook!r}; "
            "use an AsyncScope instead."
        )


class ScopeActivationError(LifecycleException, RuntimeError):
    """Raised when a scope can't be activated or deactivated."""


class ScopeAlreadyActiveError(ScopeActivationError):
    """Raised when activating a shared scope that's already active."""

    def __init__(self, name: str) -> None:
        """:param name: The scope's name."""
        super().__init__(f"Cannot activate shared scope {name!r}: already active.")


class RequiredScopeInactiveError(ScopeActivationError):
    """Raised when activating a scope whose required scope isn't active."""

    def __init__(self, name: str, required: str) -> None:
        """
        Name the scope and the scope it requires.

        :param name: The scope's name.
        :param required: The required scope's name.
        """
        super().__init__(f"Cannot activate {name!r}: required scope {required!r} is not active.")


class NothingToDeactivateError(ScopeActivationError):
    """Raised when deactivating a scope that isn't active."""

    def __init__(self, name: str) -> None:
        """:param name: The scope's name."""
        super().__init__(f"Cannot deactivate {name!r}: not active.")


class AlreadyDeactivatedError(ScopeActivationError):
    """Raised when deactivating an activation that was already deactivated."""

    def __init__(self, name: str) -> None:
        """:param name: The scope's name."""
        super().__init__(f"Cannot deactivate {name!r}: already deactivated.")


class DependentScopeActiveError(ScopeActivationError):
    """Raised when deactivating a scope while a scope requiring it is still active."""

    def __init__(self, name: str) -> None:
        """:param name: The scope's name."""
        super().__init__(f"Cannot deactivate {name!r}: a scope requiring it is still active.")


class WrongContextDeactivationError(ScopeActivationError):
    """Raised when deactivating a scope from a context other than the one that activated it."""

    def __init__(self, name: str) -> None:
        """:param name: The scope's name."""
        super().__init__(f"Cannot deactivate {name!r} outside the context that activated it.")


class ScopeInactiveError(LifecycleException, RuntimeError):
    """Raised when using a scope that isn't active in the calling context."""


class NoActivationError(ScopeInactiveError):
    """Raised when using a scope with no activation in the calling context."""

    def __init__(self, name: str) -> None:
        """:param name: The scope's name."""
        super().__init__(f"Scope {name!r} is not active.")


class ActivationEndedError(ScopeInactiveError):
    """Raised when using a scope whose activation ended, e.g. from a task that outlived it."""

    def __init__(self, name: str) -> None:
        """:param name: The scope's name."""
        super().__init__(f"Scope {name!r} is not active: its activation ended.")


class EndedDuringEntryError(ScopeInactiveError):
    """Raised when a resource's activation ends while the resource is being entered."""

    def __init__(self) -> None:
        """Explain that the resource was exited at once."""
        super().__init__("Cannot enter a resource: its scope activation has ended.")


class CacheCycleError(LifecycleException, RuntimeError):
    """Raised when a cached call would wait on its own unfinished computation."""

    def __init__(self) -> None:
        """Explain the cycle."""
        super().__init__("Cached call depends on its own in-progress computation.")


class ResourceGeneratorError(LifecycleException, RuntimeError):
    """Raised when a resource generator doesn't yield exactly once."""


class ResourceDidNotYieldError(ResourceGeneratorError):
    """Raised when a resource generator returns without yielding."""

    def __init__(self) -> None:
        """Describe the generator's failure, as `contextlib` does."""
        super().__init__("generator didn't yield")


class ResourceDidNotStopError(ResourceGeneratorError):
    """Raised when a resource generator yields again when it should finish."""

    def __init__(self, after: Literal["throw()", "athrow()"] | None = None) -> None:
        """
        Describe the generator's failure, as `contextlib` does.

        :param after: What was thrown into it, if it yielded again after an error.
        """
        super().__init__(
            "generator didn't stop" if after is None else f"generator didn't stop after {after}"
        )


class BaseCleanupErrors(BaseExceptionGroup[BaseException]):
    """Raised when several exits fail closing a scope, when any is a `BaseException`."""

    def __new__(cls, errors: Sequence[BaseException]) -> Self:
        """:param errors: The exits' failures."""
        return super().__new__(cls, "Multiple exceptions raised during scope cleanup", errors)

    def __init__(self, errors: Sequence[BaseException]) -> None:
        """:param errors: The exits' failures."""
        super().__init__("Multiple exceptions raised during scope cleanup", errors)


class CleanupErrors(BaseCleanupErrors, ExceptionGroup[Exception]):
    """Raised when several exits fail closing a scope."""
