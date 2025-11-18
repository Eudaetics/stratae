"""Resolver for the dependency injection system."""

from inspect import (
    Parameter,
    Signature,
    isasyncgenfunction,
    iscoroutinefunction,
    isgeneratorfunction,
    signature,
    unwrap,
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
        self._functions: dict[Callable[..., Any], Callable[..., Any]] = {}

    def resolve_function[**P, R](
        self,
        func: Callable[P, R],
        _resolving: set[Callable[..., Any]] | None = None,
        *,
        allow_override: bool = True,
    ) -> Callable[P, R]:
        """Resolve a function to its dependencies."""
        original_func = unwrap(func)

        if original_func in self._functions:
            return self._functions[original_func]
        if _resolving is None:
            _resolving = set()
        elif original_func in _resolving:
            raise CircularDependencyError(f"Circular dependency detected for {func}.")

        _resolving.add(original_func)
        resolved_deps: dict[str, DependsWrapper] = self._resolve_parameters(
            signature(func), _resolving
        )

        self._validate_sync_async_constraint(func, resolved_deps)
        resolved_func = self._create_wrapper(func, resolved_deps, allow_override)
        self._functions[original_func] = resolved_func
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

    def _get_annotated_info(
        self, annotation: Annotated[Any, ...]
    ) -> tuple[type, DependsWrapper | None]:
        """Extract the actual type and DependsWrapper from an Annotated parameter."""
        actual_type = annotation.__args__[0]
        depends_wrapper = next(
            (x for x in reversed(annotation.__metadata__) if isinstance(x, DependsWrapper)),
            None,
        )
        return actual_type, depends_wrapper

    def _resolve_type(self, param: Parameter, annotation: Annotated[Any, ...]) -> Any:
        """Resolve a type to its instance or factory."""
        if annotation is param.empty:
            raise RegistrationError(f"Parameter '{param.name}' has no type annotation.")

        actual_type, depends_wrapper = self._get_annotated_info(annotation)
        if depends_wrapper is not None:
            modified_param = Parameter(
                name=param.name, kind=param.kind, annotation=actual_type, default=param.default
            )
            return self._resolve_depends(modified_param, depends_wrapper, set())
        return None

    def _unwrap_type(self, annotation: Any) -> Any:
        """Unwrap Annotated types to get the actual type."""
        return getattr(annotation, "__value__", annotation)

    def _resolve_parameter(
        self, param: Parameter, _resolving: set[Callable[..., Any]]
    ) -> Any | None:
        """Resolve a single parameter based on its type and dependencies."""
        if isinstance(param.default, DependsWrapper):
            return self._resolve_depends(param, param.default, _resolving)
        elif get_origin(self._unwrap_type(param.annotation)) is Annotated:
            return self._resolve_type(param, self._unwrap_type(param.annotation))
        return None

    def _resolve_depends(
        self, param: Parameter, depends: DependsWrapper, _resolving: set[Callable[..., Any]]
    ) -> Any:
        """Resolve a Depends instance."""
        annotation = self._unwrap_type(param.annotation)
        if get_origin(annotation) is Annotated:
            _, inner_depends = self._get_annotated_info(annotation)
            if inner_depends is not None:
                raise RegistrationError(
                    f"Parameter '{param.name}' cannot use both Annotated and default Depends(...)"
                )
        depends.dependency = self.resolve_function(depends.dependency, _resolving)
        return depends

    @staticmethod
    def _validate_sync_async_constraint(
        func: Callable[..., Any], resolved_deps: dict[str, DependsWrapper]
    ) -> None:
        """Check if a function has async dependencies."""
        if iscoroutinefunction(func):
            return

        if any(v.is_async for v in resolved_deps.values()):
            raise RegistrationError(
                f"Sync function '{func.__name__}' cannot have async dependencies."
            )

    def _create_wrapper(
        self,
        func: Callable[..., Any],
        resolved_deps: dict[str, DependsWrapper],
        allow_override: bool,
    ) -> Callable[..., Any]:
        """Create a wrapper function that injects resolved dependencies."""
        # Pre-compute sets for fast lookups
        if not resolved_deps:
            return func

        no_override: set[str] = (
            {k for k, v in resolved_deps.items() if not v.allow_override}
            if allow_override
            else set(resolved_deps.keys())
        )

        if iscoroutinefunction(func):
            return create_async_wrapper(func, resolved_deps, no_override)
        elif isasyncgenfunction(func):
            return create_async_gen_wrapper(func, resolved_deps, no_override)
        elif isgeneratorfunction(func):
            return create_sync_gen_wrapper(func, resolved_deps, no_override)
        else:
            return create_sync_wrapper(func, resolved_deps, no_override)
