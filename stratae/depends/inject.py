"""
Inject decorator to resolve and inject dependencies into functions.

This module provides:
- `Depends`, marking a callable as the provider for an injected parameter.
- `Injected`, an alias of `Annotated` for typing injected parameters.
- The `inject` decorator, resolving marked parameters at decoration time.
- `DependsWrapper`, the per-provider singleton that tracks resolution and
  override state.
"""

from contextvars import ContextVar
from inspect import (
    Parameter,
    Signature,
    isasyncgenfunction,
    iscoroutinefunction,
    isgeneratorfunction,
    signature,
)
from threading import Lock
from typing import Annotated, Any, Awaitable, Callable, Hashable, Self, get_origin, overload
from weakref import WeakValueDictionary

from stratae.depends._wrappers import (
    create_async_gen_wrapper,
    create_async_wrapper,
    create_sync_gen_wrapper,
    create_sync_wrapper,
)
from stratae.depends.exceptions import (
    CircularDependencyError,
    DependencyNotFoundError,
    InjectionSignatureError,
)

_UNSET = object()

Injected = Annotated
"""Alias Annotated for clearer semantics when typing a parameter that will be injected."""


class DependsWrapper:
    """
    Per-provider singleton tracking a dependency and its override state.

    Constructing a `DependsWrapper` for the same callable twice returns
    the same instance, so every injection site of a provider shares one
    registration. The wrapper holds the provider (replaced by its
    resolved form once its own injected parameters are processed),
    whether it is async, and the context-local override slot used by
    `override`/`overrides`. Instances are held weakly and can be
    collected once nothing references their provider.
    """

    __slots__ = {
        "dependency",
        "provide",
        "is_async",
        "override",
        "override_count",
        "lock",
        "resolved",
        "__weakref__",
    }

    _registry: WeakValueDictionary[Hashable, Self] = WeakValueDictionary()
    dependency: Callable[[], Any]
    provide: Callable[[], Any]
    is_async: bool
    override: ContextVar[Any]
    override_count: int
    lock: Lock
    resolved: bool

    def __new__(cls, dependency: Callable[..., Any]) -> Self:
        """
        Return the wrapper registered for a provider, creating it if new.

        Args:
            dependency: The provider callable to wrap.

        Returns:
            The existing wrapper for `dependency` if one is registered,
            otherwise a newly registered wrapper.

        """
        existing = cls._registry.get(dependency)
        if existing is not None:
            return existing
        instance = super().__new__(cls)
        instance.dependency = dependency
        instance.provide = dependency
        instance.is_async = iscoroutinefunction(dependency)
        instance.override = ContextVar[Any](f"{dependency}_dep", default=_UNSET)
        instance.override_count = 0
        instance.lock = Lock()
        instance.resolved = False
        cls._registry[dependency] = instance
        return instance

    def provide_override(self):
        """
        Return the context-local override if set, otherwise call the provider.

        Returns:
            The overriding value for the current context, or the result
            of evaluating the provider when no override is active.

        """
        ctx = self.override.get()
        if ctx is _UNSET:
            return self.dependency()
        return ctx

    def update(self, dependency: Callable[..., Any]):
        """
        Replace the provider, keeping any active overrides in effect.

        Args:
            dependency: The new provider callable, typically the resolved
                form of the original one.

        """
        with self.lock:
            self.dependency = dependency
            if self.override_count == 0:
                self.provide = self.dependency

    @classmethod
    def find(cls, func: Callable[..., Any]):
        """
        Find the wrapper registered for a provider callable.

        Args:
            func: The provider callable that was passed to `Depends`.

        Returns:
            The `DependsWrapper` registered for `func`.

        Raises:
            DependencyNotFoundError: If `func` was never passed to
                `Depends`.

        """
        try:
            return cls._registry[func]
        except KeyError:
            raise DependencyNotFoundError(f"No Dependency found for {func}") from None


@overload
def Depends[**P, R](dependency: Callable[P, Awaitable[R]]) -> DependsWrapper: ...


@overload
def Depends[**P, R](dependency: Callable[P, R]) -> DependsWrapper: ...


def Depends[**P, R](dependency: Callable[P, R | Awaitable[R]]) -> DependsWrapper:
    """
    Mark a callable as the provider for an injected parameter.

    Used inside an `Injected` annotation, e.g.
    ``Injected[int, Depends(get_value)]``. Passing the same callable
    twice returns the same wrapper, so every injection site of a
    provider shares one registration.

    Args:
        dependency: Callable invoked to produce the parameter's value.
            It may declare injected parameters of its own, resolved
            recursively when the using function is decorated.

    Returns:
        The `DependsWrapper` registered for the callable.

    """
    return DependsWrapper(dependency=dependency)


@overload
def inject[F: Callable[..., Any]](*, sig: F) -> Callable[[Callable[..., Any]], F]: ...


@overload
def inject[R](func: Callable[..., R]) -> Callable[..., R]: ...


@overload
def inject[R]() -> Callable[[Callable[..., R]], Callable[..., R]]: ...


def inject(
    func: Callable[..., Any] | None = None,
    *,
    sig: Callable[..., Any] | None = None,
) -> Any:
    """
    Resolve a function's `Injected` parameters and strip them from calls.

    Each parameter annotated ``Injected[T, Depends(provider)]`` is filled
    by its provider when the function is called; callers do not pass it.
    The dependency graph, including providers' own injected parameters,
    is resolved once at decoration time.

    By default the decorated function is typed as `Callable[..., R]`; its
    parameters are not preserved. Injected parameters are stripped at
    resolution time and there's no way to describe that generically. The
    sig option is provided as a fallback to correct the signature for use
    in type checking and IDE support. To give the decorated function a
    precise signature, pass a stub function via `sig`. The decorated
    function is typed as `sig`, not as the original function. It is
    recommended to create type stubs instead, although this is provided
    as a fallback.

    Args:
        func: The function to wrap, when used as a bare ``@inject``.
        sig: Stub function whose signature is presented to type checkers
            and IDEs in place of the decorated function's. Has no runtime
            effect.

    Returns:
        The wrapped function, or a decorator when called with arguments.
        A function with no injected parameters is returned unchanged.

    Raises:
        InjectionSignatureError: If an injected parameter has a default
            value, or a sync function depends on an async provider.
        CircularDependencyError: If providers depend on each other in a
            cycle.

    Example:
        .. code-block:: python

            def _foo_sig(a: int) -> int: ...

            @inject(sig=_foo_sig)
            def foo(a: int, b: Injected[int, Depends(get_b)]) -> int: ...

    """

    def decorator(f: Callable[..., Any]) -> Any:
        return _resolve_function(f)

    if sig is not None or func is None:
        return decorator
    return decorator(func)


def _resolve_function[**P, R](
    func: Callable[P, R],
    _resolving: set[Callable[..., Any]] | None = None,
) -> Callable[..., R]:
    """Resolve a function to its dependencies."""
    if getattr(func, "__stratae_resolved__", False):
        return func
    if _resolving is None:
        _resolving = set()
    if func in _resolving:
        raise CircularDependencyError(f"Circular dependency detected for {func}.")

    _resolving.add(func)
    resolved_deps: dict[str, DependsWrapper] = _resolve_parameters(signature(func), _resolving)

    _validate_sync_async_constraint(func, resolved_deps)
    return _create_wrapper(func, resolved_deps)


def _resolve_parameters(
    sig: Signature, _resolving: set[Callable[..., Any]]
) -> dict[str, DependsWrapper]:
    """Resolve a list of parameters."""
    return {
        name: value
        for name, param in sig.parameters.items()
        if (value := _resolve_parameter(param, _resolving)) is not None
    }


def _get_annotated_info(annotation: Annotated[Any, ...]) -> DependsWrapper | None:
    """Extract the DependsWrapper from an Annotated parameter."""
    depends_wrapper = next(
        (x for x in reversed(annotation.__metadata__) if isinstance(x, DependsWrapper)),
        None,
    )
    return depends_wrapper


def _unwrap_type(annotation: Any) -> Any:
    """Unwrap Annotated types to get the actual type."""
    return getattr(annotation, "__value__", annotation)


def _resolve_parameter(
    param: Parameter, _resolving: set[Callable[..., Any]]
) -> DependsWrapper | None:
    """Resolve a single parameter to its dependency, if it has one."""
    annotation = _unwrap_type(param.annotation)
    if get_origin(annotation) is not Annotated:
        return None

    depends = _get_annotated_info(annotation)
    if depends is None:
        return None
    elif param.default is not Parameter.empty:
        raise InjectionSignatureError(f"Cannot use a default with injected parameter {param.name}")

    if not depends.resolved:
        depends.update(_resolve_function(depends.dependency, _resolving))
        depends.resolved = True
    return depends


def _validate_sync_async_constraint(
    func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
) -> None:
    """Check if a function has async dependencies."""
    if iscoroutinefunction(func) or isasyncgenfunction(func):
        return

    if any(v.is_async for v in resolved_deps.values()):
        raise InjectionSignatureError(
            f"Sync function '{func.__name__}' cannot have async dependencies."
        )


def _create_wrapper(
    func: Callable[..., Any],
    resolved_deps: dict[str, DependsWrapper],
) -> Callable[..., Any]:
    """Create a wrapper function that injects resolved dependencies."""
    if not resolved_deps:
        return func

    return _wrapper_factory(func)(func, resolved_deps)


def _wrapper_factory(func: Callable[..., Any]) -> Callable[..., Callable[..., Any]]:
    """Determine the correct create wrapper function based on the function type."""
    if iscoroutinefunction(func):
        return create_async_wrapper
    elif isasyncgenfunction(func):
        return create_async_gen_wrapper
    elif isgeneratorfunction(func):
        return create_sync_gen_wrapper
    else:
        return create_sync_wrapper
