"""
Core dependency injection module.

{py:func}`Depends` marks a callable as the provider for an injected
parameter. Wrapping a callable in {py:func}`Depends` registers it as a
{py:class}`Provider <stratae.depends._provide.Provider>`, the singleton
that tracks that provider's resolution and override state. Passing the
same callable twice returns the same
{py:class}`Provider <stratae.depends._provide.Provider>`. Every
injection site sharing a provider shares one registration.
{py:data}`Injected` is provided as an alias of `Annotated` to make
injected parameters easier to spot in a signature, but using it is not
required since {py:func}`Depends` also works directly inside `Annotated`.

The {py:func}`inject` decorator inspects a function's signature at
decoration time, finds parameters annotated with {py:func}`Depends`, and
wraps the function. The wrapped function's parameters are resolved by calling
their providers at call time rather than being passed in by the caller.
Providers may themselves declare injected parameters, which are resolved
recursively when the using function is decorated. Sync functions may only
depend on sync providers, which is validated at decoration time, raising
{py:exc}`InjectionSignatureError <stratae.depends.exceptions.InjectionSignatureError>`.
Async functions may mix sync and async providers.

```{note}
This module only handles wiring: calling a provider and passing its
result to whatever declared it as a dependency. A provider runs again on
every call unless it caches its own result. For singleton or
scoped values (computed once per request, once per process, etc.),
layer this with {py:mod}`stratae.lifecycle`: decorate the provider with
{py:meth}`Lifecycle.cache <stratae.lifecycle.lifecycle.Lifecycle.cache>`
(or its {py:class}`AsyncLifecycle <stratae.lifecycle.lifecycle.AsyncLifecycle>`
counterpart) so it is computed once per scope activation, then pass that
cached provider to {py:func}`Depends` as usual.
```

```{rubric} Example:
```
```{code-block} python
:caption: Injecting a database dependency into a function that queries it

from typing import Annotated
from stratae.depends import Depends, inject

class Database:
    def query(self, table: str) -> list[str]:
        return [f"row from {table}"]

def get_db() -> Database:
    return Database()

type DatabaseDep = Annotated[Database, Depends(get_db)]

@inject
def list_users(db: DatabaseDep) -> list[str]:
    return db.query("users")

assert list_users() == ["row from users"]  # db resolved by calling get_db()
```

See {py:func}`Depends` and {py:func}`inject` for additional examples.

"""

from inspect import (
    Parameter,
    Signature,
    isasyncgenfunction,
    iscoroutinefunction,
    isgeneratorfunction,
    signature,
)
from typing import Annotated, Any, Awaitable, Callable, get_origin, overload

from stratae.depends._provide import Provider
from stratae.depends._wrappers import (
    create_async_gen_wrapper,
    create_async_wrapper,
    create_sync_gen_wrapper,
    create_sync_wrapper,
)
from stratae.depends.exceptions import (
    CircularDependencyError,
    InjectionSignatureError,
)

Injected = Annotated
"""Alias `Annotated` for clearer semantics when typing a parameter that will be injected."""


@overload
def Depends[**P, R](dependency: Callable[P, Awaitable[R]]) -> Provider: ...  # noqa: S1542


@overload
def Depends[**P, R](dependency: Callable[P, R]) -> Provider: ...  # noqa: S1542


def Depends[**P, R](dependency: Callable[P, R | Awaitable[R]]) -> Provider:  # noqa: S1542
    """
    Mark a callable as the provider for an injected parameter.

    Used inside an annotation, e.g. `Injected[int, Depends(get_value)]`
    or `Annotated[int, Depends(get_value)]`. Passing the same callable
    twice returns the same {py:class}`Provider <stratae.depends._provide.Provider>`,
    so every injection site of a provider shares one registration.

    :param dependency: Callable invoked to produce the parameter's value. It
        may declare injected parameters of its own, resolved recursively
        when the using function is decorated.
    :returns: The {py:class}`Provider <stratae.depends._provide.Provider>`
        registered for the callable.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Registering the same provider twice returns the same Provider

    from stratae.depends import Depends

    def get_value() -> int:
        return 42

    assert Depends(get_value) is Depends(get_value)
    ```

    """
    return Provider(dependency=dependency)


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
    Resolve a function's Injected parameters and strip them from calls.

    Each parameter annotated `Injected[T, Depends(provider)]` is filled by
    its provider when the function is called; callers do not pass it. The
    dependency graph, including providers' own injected parameters, is
    resolved once at decoration time.

    By default the decorated function is typed as `Callable[..., R]`; its
    parameters are not preserved. Injected parameters are stripped at
    resolution time and there's no way to describe that generically. The
    `sig` option is provided as a fallback to correct the signature for
    use in type checking and IDE support. To give the decorated function
    a precise signature, pass a stub function via `sig`. The decorated
    function is typed as `sig`, not as the original function. It is
    recommended to create type stubs instead, although this is provided
    as a fallback.

    :param func: The function to wrap, when used as a bare `@inject`.
    :param sig: Stub function whose signature is presented to type checkers
        and IDEs in place of the decorated function's. Has no runtime
        effect.
    :returns: The wrapped function, or a decorator when called with
        arguments. A function with no injected parameters is returned
        unchanged.
    :raises InjectionSignatureError: An injected parameter has a default
        value, or a sync function depends on an async provider.
    :raises CircularDependencyError: Providers depend on each other in a
        cycle.

    ```{rubric} Examples:
    ```
    ```{code-block} python
    :caption: Decorating a function with a bare @inject

    from stratae.depends import Depends, Injected, inject

    def get_tax_rate() -> float:
        return 0.08

    @inject
    def total_with_tax(
        subtotal: float, tax_rate: Injected[float, Depends(get_tax_rate)]
    ) -> float:
        return subtotal * (1 + tax_rate)

    assert total_with_tax(100) == 108.0  # tax_rate resolved by calling get_tax_rate()
    ```

    ```{code-block} python
    :caption: Correcting the decorated signature seen by type checkers with sig

    from stratae.depends import Depends, Injected, inject

    def get_tax_rate() -> float:
        return 0.08

    def _total_with_tax_sig(subtotal: float) -> float: ...

    @inject(sig=_total_with_tax_sig)
    def total_with_tax(
        subtotal: float, tax_rate: Injected[float, Depends(get_tax_rate)]
    ) -> float:
        return subtotal * (1 + tax_rate)

    assert total_with_tax(100) == 108.0
    # Pylance shows (function) def total_with_tax(subtotal: float) -> float
    ```

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
    resolved_deps: dict[str, Provider] = _resolve_parameters(signature(func), _resolving)

    _validate_sync_async_constraint(func, resolved_deps)
    return _create_wrapper(func, resolved_deps)


def _resolve_parameters(sig: Signature, _resolving: set[Callable[..., Any]]) -> dict[str, Provider]:
    """Resolve a list of parameters."""
    return {
        name: value
        for name, param in sig.parameters.items()
        if (value := _resolve_parameter(param, _resolving)) is not None
    }


def _get_annotated_info(annotation: Annotated[Any, ...]) -> Provider | None:
    """Extract the Provider from an Annotated parameter."""
    depends_wrapper = next(
        (x for x in reversed(annotation.__metadata__) if isinstance(x, Provider)),
        None,
    )
    return depends_wrapper


def _unwrap_type(annotation: Any) -> Any:
    """Unwrap Annotated types to get the actual type."""
    return getattr(annotation, "__value__", annotation)


def _resolve_parameter(param: Parameter, _resolving: set[Callable[..., Any]]) -> Provider | None:
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
    func: Callable[..., Any], resolved_deps: dict[str, Provider]
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
    resolved_deps: dict[str, Provider],
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
