"""
Inject decorator to resolve and inject dependencies into functions.

This module provides:
- A global Resolver instance for managing dependencies.
- The `inject` decorator for resolving and injecting dependencies into functions.
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
    RegistrationError,
)

_UNSET = object()

Injected = Annotated
"""Alias Annotated for clearer semantics when typing a parameter that will be injected."""


class DependsWrapper:
    """Class used to wrap the dependency injection."""

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
        """Singleton factory for dependency wrappers."""
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
        """Return override if set, otherwise evaluate the dependency."""
        ctx = self.override.get()
        if ctx is _UNSET:
            return self.dependency()
        return ctx

    def update(self, dependency: Callable[..., Any]):
        """Update the dependency while also correcting the provide."""
        with self.lock:
            self.dependency = dependency
            if self.override_count == 0:
                self.provide = self.dependency

    @classmethod
    def find(cls, func: Callable[..., Any]):
        """Find the associated DependsWrapper for the injected dependency."""
        try:
            return cls._registry[func]
        except KeyError:
            raise DependencyNotFoundError(f"No Dependency found for {func}") from None


@overload
def Depends[**P, R](dependency: Callable[P, Awaitable[R]]) -> DependsWrapper: ...


@overload
def Depends[**P, R](dependency: Callable[P, R]) -> DependsWrapper: ...


def Depends[**P, R](dependency: Callable[P, R | Awaitable[R]]) -> DependsWrapper:
    """Marker function used to denote a dependency injection."""
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
    Inject decorator to resolve dependencies for a function.

    By default the decorated function is typed as `Callable[..., R]` — its
    parameters are not preserved. Injected parameters are stripped at
    resolution time and there's no way to describe that generically. The
    sig option is provided as a fallback to correct the signature for use
    in type checking and IDE support. To give the decorated function a
    precise signature, pass a stub function via `sig`. The decorated
    function is typed as `sig`, not as the original function:

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
        raise RegistrationError(f"Cannot use a default with injected parameter {param.name}")

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
        raise RegistrationError(f"Sync function '{func.__name__}' cannot have async dependencies.")


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
