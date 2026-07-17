"""Resolver for the dependency injection system."""

from inspect import (
    Parameter,
    Signature,
    isasyncgenfunction,
    iscoroutinefunction,
    isgeneratorfunction,
    signature,
)
from typing import Annotated, Any, Callable, get_origin

from stratae.depends import DependsWrapper
from stratae.depends._wrappers import (
    create_async_gen_wrapper,
    create_async_wrapper,
    create_sync_gen_wrapper,
    create_sync_wrapper,
)
from stratae.depends.exceptions import CircularDependencyError, RegistrationError


def resolve_function[**P, R](
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
        depends.update(resolve_function(depends.dependency, _resolving))
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
