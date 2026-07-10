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


class Resolver:
    """Dependency Injection resolver with registration-time resolution."""

    def __init__(self):
        """Initialize the resolver with empty registries."""
        self._functions: set[Callable[..., Any]] = set()

    def resolve_function[**P, R](
        self,
        func: Callable[P, R],
        _resolving: set[Callable[..., Any]] | None = None,
    ) -> Callable[..., R]:
        """Resolve a function to its dependencies."""
        if func in self._functions:
            return func
        if _resolving is None:
            _resolving = set()
        if func in _resolving:
            raise CircularDependencyError(f"Circular dependency detected for {func}.")

        _resolving.add(func)
        resolved_deps: dict[str, DependsWrapper] = self._resolve_parameters(
            signature(func), _resolving
        )

        self._validate_sync_async_constraint(func, resolved_deps)
        resolved_func = self._create_wrapper(func, resolved_deps)
        self._functions.add(resolved_func)
        return resolved_func

    def _resolve_parameters(
        self, sig: Signature, _resolving: set[Callable[..., Any]]
    ) -> dict[str, DependsWrapper]:
        """Resolve a list of parameters."""
        return {
            name: value
            for name, param in sig.parameters.items()
            if (value := self._resolve_parameter(param, _resolving)) is not None
        }

    def clear(self) -> None:
        """Clear all registered functions."""
        self._functions.clear()

    def _get_annotated_info(self, annotation: Annotated[Any, ...]) -> DependsWrapper | None:
        """Extract the DependsWrapper from an Annotated parameter."""
        depends_wrapper = next(
            (x for x in reversed(annotation.__metadata__) if isinstance(x, DependsWrapper)),
            None,
        )
        return depends_wrapper

    def _unwrap_type(self, annotation: Any) -> Any:
        """Unwrap Annotated types to get the actual type."""
        return getattr(annotation, "__value__", annotation)

    def _resolve_parameter(
        self, param: Parameter, _resolving: set[Callable[..., Any]]
    ) -> DependsWrapper | None:
        """Resolve a single parameter to its dependency, if it has one."""
        annotation = self._unwrap_type(param.annotation)
        if get_origin(annotation) is not Annotated:
            return None

        depends = self._get_annotated_info(annotation)
        if depends is None:
            return None
        elif param.default is not Parameter.empty:
            raise RegistrationError(f"Cannot use a default with injected parameter {param.name}")

        depends.update(self.resolve_function(depends.dependency, _resolving))
        return depends

    @staticmethod
    def _validate_sync_async_constraint(
        func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
    ) -> None:
        """Check if a function has async dependencies."""
        if iscoroutinefunction(func) or isasyncgenfunction(func):
            return

        if any(v.is_async for v in resolved_deps.values()):
            raise RegistrationError(
                f"Sync function '{func.__name__}' cannot have async dependencies."
            )

    def _create_wrapper(
        self,
        func: Callable[..., Any],
        resolved_deps: dict[str, DependsWrapper],
    ) -> Callable[..., Any]:
        """Create a wrapper function that injects resolved dependencies."""
        if not resolved_deps:
            return func

        return self._wrapper_factory(func)(func, resolved_deps)

    def _wrapper_factory(self, func: Callable[..., Any]) -> Callable[..., Callable[..., Any]]:
        """Determine the correct create wrapper function based on the function type."""
        if iscoroutinefunction(func):
            return create_async_wrapper
        elif isasyncgenfunction(func):
            return create_async_gen_wrapper
        elif isgeneratorfunction(func):
            return create_sync_gen_wrapper
        else:
            return create_sync_wrapper
