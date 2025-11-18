"""Test suite for the Dependency Injection Resolver."""

import asyncio
from functools import wraps
from inspect import signature
from typing import Any, Callable
from unittest.mock import Mock

import pytest
from typing_extensions import Annotated

from stratae.depends import AUTO, Depends, Resolver
from stratae.depends.exceptions import (
    CircularDependencyError,
    OverrideNotAllowedError,
    RegistrationError,
)


class Dependency:
    """A simple dependency class for testing purposes."""

    def __init__(self, value: int = 42):
        """Initialize the Dependency instance."""
        self.value = value


def get_dep() -> int:
    """
    Create a Dependency instance for testing.

    This is used to demonstrate dependency injection in the Resolver.
    """
    return 1


def factory_function() -> Dependency:
    """Create an instance of SampleType."""
    return Dependency(100)


type DependencyDep = Annotated[Dependency, Depends(factory_function)]
type IntDependency = Annotated[int, Depends(get_dep)]


def test_initialization():
    """
    Verify that Resolver can be initialized without errors.

    Given: an empty Resolver,
    When: it is created,
    Then: it should not raise any exceptions.
    """
    # Arrange & Act
    resolver = Resolver()

    # Assert
    assert isinstance(resolver, Resolver)
    assert resolver._functions == {}  # pyright: ignore[reportPrivateUsage]


def test_clear():
    """
    Verify that clear method empties the Resolver.

    Given: a Resolver with registered singletons and factories,
    When: clear is called,
    Then: it should remove all registrations.
    """
    # Arrange
    resolver = Resolver()
    resolver._functions["TestFunction"] = lambda: "function_instance"  # pyright: ignore

    # Act
    resolver.clear()

    # Assert
    assert resolver._functions == {}  # pyright: ignore[reportPrivateUsage]


def test_resolve_function():
    """
    Verify that a function can be resolved with its dependencies.

    Given: a Resolver and a function with dependencies,
    When: resolve_function is called,
    Then: it should return a resolved function with dependencies injected.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(dep: int = Depends(get_dep)) -> int:
        return dep

    # Act
    resolved_function = resolver.resolve_function(sample_function)

    # Assert
    assert callable(resolved_function)
    assert resolved_function() == 1


def test_resolve_function_twice():
    """
    Verify that resolving the same function multiple times returns the same result.

    Given: a Resolver and a function with dependencies,
    When: resolve_function is called multiple times,
    Then: it should return the same resolved function each time.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(dep: int = Depends(get_dep)) -> int:
        return dep

    # Act
    resolved_function1 = resolver.resolve_function(sample_function)
    resolved_function2 = resolver.resolve_function(sample_function)

    # Assert
    assert resolved_function1 is resolved_function2
    assert resolved_function1() == 1


def test_resolve_function_with_nested_wrapped_dependencies():
    """
    Verify that a function with nested and wrapped dependencies can be resolved correctly.

    Given: a Resolver and a function with nested dependencies,
    When: resolve_function is called,
    Then: it should return a resolved function resolved once with all dependencies injected.
    """
    # Arrange
    resolver = Resolver()

    def some_wrapper(func: Callable[[], int]) -> Callable[[Any], Any]:
        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> int:
            return func(*args, **kwargs)

        return wrapped

    def first_dependency() -> int:
        return 2

    @some_wrapper
    def second_dependency(dep: int = Depends(first_dependency)) -> int:
        return dep + 1

    second_dependency = resolver.resolve_function(second_dependency)

    def sample_function(
        dep: int = Depends(second_dependency),
        other: int = Depends(first_dependency),
    ) -> int:
        return dep + other

    # Act

    resolved_function = resolver.resolve_function(sample_function)

    # Assert
    assert callable(resolved_function)
    assert resolved_function() == 5
    original_functions = [
        getattr(func, "__wrapped__", func)
        for func in resolver._functions  # pyright: ignore[reportPrivateUsage]
    ]
    assert len(original_functions) == len(set(original_functions))


def test_resolve_function_with_multiple_dependencies():
    """
    Verify that a function with multiple dependencies can be resolved correctly.

    Given: a Resolver and a function with multiple dependencies,
    When: resolve_function is called,
    Then: it should return a resolved function with all dependencies injected.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(
        dep1: int = Depends(get_dep),
        dep2: int = Depends(get_dep),
    ) -> int:
        return dep1 + dep2

    # Act
    resolved_function = resolver.resolve_function(sample_function)

    # Assert
    assert callable(resolved_function)
    assert resolved_function() == 2


async def test_resolve_function_with_async_dependency():
    """
    Verify that a function with an async dependency can be resolved correctly.

    Given: a Resolver and a function with an async dependency,
    When: resolve_function is called,
    Then: it should elevate and return a resolved function with the async dependency injected.
    """
    # Arrange
    resolver = Resolver()

    async def async_dependency() -> Dependency:
        await asyncio.sleep(0)
        return Dependency(3)

    async def sample_function(dep: Dependency = Depends(async_dependency)) -> int:
        await asyncio.sleep(0)
        return dep.value

    # Act
    resolved_function = resolver.resolve_function(sample_function)

    # Assert
    assert callable(resolved_function)
    assert await resolved_function() == 3


def test_resolve_sync_function_with_async_dependency():
    """
    Verify that a sync function with an async dependency raises an error.

    Given: a Resolver and a sync function with an async dependency,
    When: resolve_function is called,
    Then: it should raise an error.
    """
    # Arrange
    resolver = Resolver()

    async def async_dependency() -> int:
        await asyncio.sleep(0)
        return 3

    def sync_function(dep: int = Depends(async_dependency)) -> int:
        return dep

    # Act & Assert
    with pytest.raises(
        RegistrationError, match="Sync function '.*' cannot have async dependencies"
    ):
        resolver.resolve_function(sync_function)


def test_resolve_function_with_dependency_chain():
    """
    Verify that a function with a dependency chain can be resolved correctly.

    Given: a Resolver and a function with a dependency chain,
    When: resolve_function is called,
    Then: it should return a resolved function with all dependencies injected.
    """
    # Arrange
    resolver = Resolver()

    def first_dependency() -> int:
        return 4

    def second_dependency(dep: int = Depends(first_dependency)) -> int:
        return dep + 1

    def sample_function(non_dep: int, dep: int = Depends(second_dependency)) -> int:
        return dep + non_dep

    # Act
    resolved_function = resolver.resolve_function(sample_function)

    # Assert
    assert callable(resolved_function)

    assert resolved_function(2) == 7


def test_resolve_function_with_chain_and_factory():
    """
    Verify a function with a dependency chain and factory resolves correctly.

    Given: a Resolver and a function with a dependency chain using a factory,
    When: resolve_function is called,
    Then: it should return a resolved function with all dependencies injected and factories
            freshly ran.
    """
    # Arrange
    resolver = Resolver()

    def factory_function() -> int:
        return 5

    def first_dependency() -> int:
        return 4

    changing_val = 10

    def second_dependency(dep: int = Depends(first_dependency)) -> int:
        return dep + changing_val

    def sample_function(
        non_dep: int,
        dep: int = Depends(second_dependency),
        factory: int = Depends(factory_function),
    ) -> int:
        return dep + non_dep + factory

    # Act
    resolved_function = resolver.resolve_function(sample_function)
    result_1 = resolved_function(2)
    changing_val = 20  # Change the value to see if it affects the result
    result_2 = resolved_function(2)

    # Assert
    assert callable(resolved_function)
    assert result_1 == 21
    assert result_2 == 31


def test_resolve_function_with_chain_and_mixed():
    """
    Verify a function with a mixed dependency chain resolves correctly.

    Given: a Resolver and a function with a mixed dependency chain (singletons and factory),
    When: resolve_function is called,
    Then: it should return a resolved function with all dependencies injected and factories
            freshly ran.
    """
    # Arrange
    resolver = Resolver()

    class Intresolver:
        def __init__(self, value: int = 4):
            self.value = value

    def factory_function() -> int:
        return 5

    def first_dependency(something: Intresolver = Depends(Intresolver)) -> int:
        return something.value

    changing_val = 10

    def second_dependency(dep: int = Depends(first_dependency)) -> int:
        return dep + changing_val

    def sample_function(
        non_dep: int,
        dep: int = Depends(second_dependency),
        factory: int = Depends(factory_function),
    ) -> int:
        return dep + non_dep + factory

    # Act
    resolved_function = resolver.resolve_function(sample_function)
    result_1 = resolved_function(2)
    changing_val = 8  # Change the value to see if it affects the result
    result_2 = resolved_function(2)

    # Assert
    assert callable(resolved_function)
    assert result_1 == 21
    assert result_2 == 19


def test_resolve_function_with_annotated():
    """
    Verify that a function with Annotated dependencies can be resolved correctly.

    Given: a Resolver and a function with Annotated dependencies,
    When: resolve_function is called,
    Then: it should return a resolved function with all dependencies injected.
    """
    # Arrange
    resolver = Resolver()

    def get_integer() -> int:
        return 7

    def get_string() -> str:
        return "test"

    def sample_function(
        dep1: Annotated[str, Depends(get_string)] = AUTO,
        dep2: Annotated[int, Depends(get_integer)] = AUTO,
    ) -> str:
        return f"{dep1}-{dep2}"

    # Act
    resolved_function = resolver.resolve_function(sample_function)

    # Assert
    assert callable(resolved_function)
    assert resolved_function() == "test-7"


def test_resolve_function_with_annotated_type():
    """
    Verify that a function with an Annotated type dependency can be resolved correctly.

    Given: a Resolver and a function with an Annotated type dependency,
    When: resolve_function is called,
    Then: it should return a resolved function with the dependency injected.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(dep: IntDependency = AUTO) -> int:
        return dep

    # Act
    resolved_function = resolver.resolve_function(sample_function)

    # Assert
    assert resolved_function() == 1


def test_resolve_function_with_no_dependencies():
    """
    Verify that a function with no dependencies can be resolved correctly.

    Given: a Resolver and a function with no dependencies,
    When: resolve_function is called,
    Then: it should return the original function unchanged.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(a: int, b: int) -> int:
        return a + b

    # Act
    resolved_function = resolver.resolve_function(sample_function)

    # Assert
    assert resolved_function is sample_function
    assert resolved_function(3, 4) == 7


def test_resolve_function_with_circular_dependency():
    """
    Verify that a function with a circular dependency raises an error.

    Given: a Resolver and a function with a circular dependency,
    When: resolve_function is called,
    Then: it should raise a CircularDependencyError.
    """
    # Arrange
    resolver = Resolver()

    def dep1(dep: int = 0) -> int:
        return dep + 1

    def dep2(dep: int = Depends(dep1)) -> int:
        return dep + 1

    dep1.__defaults__ = (Depends(dep2),)

    # Act & Assert
    with pytest.raises(CircularDependencyError, match="Circular dependency detected for .*dep1.*"):
        resolver.resolve_function(dep1)


def test_resolved_function_with_manual_kwargs():
    """
    Verify that manually passing kwargs to a resolved function overrides injected dependencies.

    Given: a Resolver and a function with dependencies,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should use the manual kwargs instead of injecting dependencies.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(dep1: int = Depends(get_dep), dep2: int = Depends(get_dep)) -> int:
        return dep1 + dep2

    resolved_function = resolver.resolve_function(sample_function)

    # Act
    result = resolved_function(dep1=10, dep2=20)

    # Assert
    assert result == 30


def test_resolved_function_with_manual_kwargs_bypasses_dependswrapper():
    """
    Verify that manually passing kwargs to a resolved function bypasses DependsWrapper calls.

    Given: a Resolver and a function with dependencies wrapped in DependsWrapper,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should not call the DependsWrapper and use the manual kwargs instead.
    """
    # Arrange
    resolver = Resolver()
    mock = Mock()

    def counting_dependency() -> int:
        mock()
        return 5

    def sample_function(dep: int = Depends(counting_dependency)) -> int:
        return dep

    resolved_function = resolver.resolve_function(sample_function)

    # Act
    result = resolved_function(dep=10)

    # Assert
    assert result == 10
    assert mock.call_count == 0


def test_resolved_function_with_manual_kwargs_no_override():
    """
    Verify that manually passing kwargs to a resolved function when allow_override is False.

    Given: a Resolver and a function with dependencies,
    When: resolve_function is called with allow_override=False and manual kwargs are provided,
    Then: it should raise a OverrideNotAllowedError.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(dep1: int = Depends(get_dep), dep2: int = Depends(get_dep)) -> int:
        return dep1 + dep2

    resolved_function = resolver.resolve_function(sample_function, allow_override=False)

    # Act & Assert
    with pytest.raises(
        OverrideNotAllowedError,
        match="Overriding these dependencies is not allowed: dep1, dep2",
    ):
        resolved_function(dep1=10, dep2=20)


async def test_resolved_async_function_with_manual_kwargs_no_override():
    """
    Verify that manually passing kwargs to an async function when allow_override is False.

    Given: a Resolver and an async function with dependencies,
    When: resolve_function is called with allow_override=False and manual kwargs are provided,
    Then: it should raise a OverrideNotAllowedError.
    """
    # Arrange
    resolver = Resolver()

    async def async_get_dep() -> int:
        await asyncio.sleep(0)
        return 1

    async def sample_function(
        dep1: int = Depends(async_get_dep), dep2: int = Depends(async_get_dep)
    ) -> int:
        await asyncio.sleep(0)
        return dep1 + dep2

    resolved_function = resolver.resolve_function(sample_function, allow_override=False)

    # Act & Assert
    with pytest.raises(
        OverrideNotAllowedError,
        match="Overriding these dependencies is not allowed: dep1, dep2",
    ):
        await resolved_function(dep1=10, dep2=20)


def test_resolved_function_with_partial_manual_kwargs_no_override():
    """
    Verify manually passing some kwargs to a resolved function when allow_override is False.

    Given: a Resolver and a function with dependencies,
    When: resolve_function is called with allow_override=False and manual kwargs are provided,
    Then: it should raise a OverrideNotAllowedError.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(
        non_dep: int = 0, dep1: int = Depends(get_dep), dep2: int = Depends(get_dep)
    ) -> int:
        return non_dep + dep1 + dep2

    resolved_function = resolver.resolve_function(sample_function, allow_override=False)

    # Act & Assert
    with pytest.raises(
        OverrideNotAllowedError,
        match="Overriding these dependencies is not allowed: dep1",
    ):
        resolved_function(dep1=10)


def test_resolved_function_with_kwargs_and_override():
    """
    Verify manually passing some kwargs to a resolved function when allow_override is True.

    Given: a Resolver and a function with dependencies,
    When: resolve_function is called with allow_override=True and manual kwargs are provided,
    Then: it should use the manual kwargs and inject the rest of the dependencies.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(
        non_dep: int = 0, dep1: int = Depends(get_dep), dep2: int = Depends(get_dep)
    ) -> int:
        return non_dep + dep1 + dep2

    resolved_function = resolver.resolve_function(sample_function, allow_override=True)

    # Act
    result = resolved_function(non_dep=4, dep1=10)

    # Assert
    assert result == 15


def test_resolved_function_with_no_dependencies_and_kwargs():
    """
    Verify manually passing kwargs to a resolved function with no dependencies.

    Given: a Resolver and a function with no dependencies,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should use the manual kwargs.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(non_dep1: int, non_dep2: int) -> int:
        return non_dep1 + non_dep2

    resolved_function = resolver.resolve_function(sample_function)

    # Act
    result = resolved_function(non_dep1=7, non_dep2=8)

    # Assert
    assert result == 15


def test_resolved_function_depends_override_sync():
    """
    Verify that a sync function with a dependency that allows override works correctly.

    Given: a Resolver and a sync function with a dependency that allows override,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should use the manual kwargs for the dependency.
    """
    # Arrange
    resolver = Resolver()

    def dependency() -> int:
        return 3

    def sample_function(dep: int = Depends(dependency, allow_override=True)) -> int:
        return dep

    resolved_function = resolver.resolve_function(sample_function)

    # Act
    result = resolved_function(dep=10)

    # Assert
    assert result == 10


async def test_resolved_function_depends_override_async():
    """
    Verify that an async function with a dependency that allows override works correctly.

    Given: a Resolver and an async function with a dependency that allows override,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should use the manual kwargs for the dependency.
    """
    # Arrange
    resolver = Resolver()

    async def dependency() -> int:
        await asyncio.sleep(0)
        return 3

    async def sample_function(dep: int = Depends(dependency, allow_override=True)) -> int:
        await asyncio.sleep(0)
        return dep

    resolved_function = resolver.resolve_function(sample_function)

    # Act
    result = await resolved_function(dep=10)

    # Assert
    assert result == 10


def test_resolved_function_depends_no_override_sync():
    """
    Verify that a sync function with a dependency that does not allow override works correctly.

    Given: a Resolver and a sync function with a dependency that does not allow override,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should raise a OverrideNotAllowedError.
    """
    # Arrange
    resolver = Resolver()

    def dependency() -> int:
        return 3

    def sample_function(dep: int = Depends(dependency, allow_override=False)) -> int:
        return dep

    resolved_function = resolver.resolve_function(sample_function)

    # Act & Assert
    with pytest.raises(
        OverrideNotAllowedError,
        match="Overriding these dependencies is not allowed: dep",
    ):
        resolved_function(dep=10)


async def test_resolved_function_depends_no_override_async():
    """
    Verify an async function with a dependency that does not allow override works correctly.

    Given: a Resolver and an async function with a dependency that does not allow override,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should raise a OverrideNotAllowedError.
    """
    # Arrange
    resolver = Resolver()

    async def dependency() -> int:
        await asyncio.sleep(0)
        return 3

    async def sample_function(dep: int = Depends(dependency, allow_override=False)) -> int:
        await asyncio.sleep(0)
        return dep

    resolved_function = resolver.resolve_function(sample_function)

    # Act & Assert
    with pytest.raises(
        OverrideNotAllowedError,
        match="Overriding these dependencies is not allowed: dep",
    ):
        await resolved_function(dep=10)


def test_resolved_function_no_override_with_depends_override_true_sync():
    """
    Verify case when function override is False, but Dependency is marked true.

    Given: a Resolver and a sync function with a dependency that allows override,
    When: resolve_function is called with allow_override=False,
    Then: it should raise a OverrideNotAllowedError.
    """
    # Arrange
    resolver = Resolver()

    def dependency() -> int:
        return 3

    def sample_function(dep: int = Depends(dependency, allow_override=True)) -> int:
        return dep

    # Act
    func = resolver.resolve_function(sample_function, allow_override=False)

    # Assert
    with pytest.raises(
        OverrideNotAllowedError,
        match="Overriding these dependencies is not allowed: dep",
    ):
        func(dep=10)


async def test_resolved_function_no_override_with_depends_override_true_async():
    """
    Verify case when function override is False, but Dependency is marked true.

    Given: a Resolver and an async function with a dependency that allows override,
    When: resolve_function is called with allow_override=False,
    Then: it should raise a OverrideNotAllowedError.
    """
    # Arrange
    resolver = Resolver()

    async def dependency() -> int:
        await asyncio.sleep(0)
        return 3

    async def sample_function(dep: int = Depends(dependency, allow_override=True)) -> int:
        await asyncio.sleep(0)
        return dep

    # Act
    func = resolver.resolve_function(sample_function, allow_override=False)

    # Assert
    with pytest.raises(
        OverrideNotAllowedError,
        match="Overriding these dependencies is not allowed: dep",
    ):
        await func(dep=10)


def test_mixed_override_sync():
    """
    Verify mixed case of dependencies with different allow_override settings.

    Given: a Resolver and a sync function with mixed dependencies with different allow_override
           settings,
    When: resolve_function is called with allow_override=True,
    Then: it should raise a OverrideNotAllowedError for dependencies that do not allow override.
    """
    # Arrange
    resolver = Resolver()

    def dependency1() -> int:
        return 3

    def dependency2() -> int:
        return 4

    def sample_function(
        dep1: int = Depends(dependency1, allow_override=True),
        dep2: int = Depends(dependency2, allow_override=False),
    ) -> int:
        return dep1 + dep2

    resolved_function = resolver.resolve_function(sample_function, allow_override=True)

    # Act
    result = resolved_function(dep1=10)

    # Assert
    assert result == 14
    with pytest.raises(
        OverrideNotAllowedError,
        match="Overriding these dependencies is not allowed: dep2",
    ):
        resolved_function(dep2=20)


async def test_mixed_override_async():
    """
    Verify mixed case of dependencies with different allow_override settings.

    Given: a Resolver and an async function with mixed dependencies with different allow_override
           settings,
    When: resolve_function is called with allow_override=True,
    Then: it should raise a OverrideNotAllowedError for dependencies that do not allow override.
    """
    # Arrange
    resolver = Resolver()

    async def dependency1() -> int:
        await asyncio.sleep(0)
        return 3

    async def dependency2() -> int:
        await asyncio.sleep(0)
        return 4

    async def sample_function(
        dep1: int = Depends(dependency1, allow_override=True),
        dep2: int = Depends(dependency2, allow_override=False),
    ) -> int:
        await asyncio.sleep(0)
        return dep1 + dep2

    resolved_function = resolver.resolve_function(sample_function, allow_override=True)

    # Act
    result = await resolved_function(dep1=10)

    # Assert
    assert result == 14
    with pytest.raises(
        OverrideNotAllowedError,
        match="Overriding these dependencies is not allowed: dep2",
    ):
        await resolved_function(dep2=20)


def test_manual_kwargs_with_no_dependencies():
    """
    Verify that providing manual kwargs to a function with no dependencies works correctly.

    Given: a Resolver and a function with no dependencies,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should use the manual kwargs without errors.
    """
    # Arrange
    resolver = Resolver()

    def sample_function(a: int, b: int) -> int:
        return a + b

    resolved_function = resolver.resolve_function(sample_function)

    # Act
    result = resolved_function(a=5, b=10)

    # Assert
    assert result == 15


def test_manual_args_with_partial_dependencies_sync():
    """
    Verify providing manual args to a sync function with partial dependencies works correctly.

    Given: a Resolver and a sync function with some dependencies,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should use the manual kwargs and inject the rest of the dependencies.
    """
    # Arrange
    resolver = Resolver()

    def dependency() -> int:
        return 7

    def sample_function(a: int, dep: int = Depends(dependency)) -> int:
        return a + dep

    resolved_function = resolver.resolve_function(sample_function)

    # Act
    result = resolved_function(3)

    # Assert
    assert result == 10


async def test_manual_args_with_partial_dependencies_async():
    """
    Verify providing manual args to an async function with partial dependencies works correctly.

    Given: a Resolver and an async function with some dependencies,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should use the manual kwargs and inject the rest of the dependencies.
    """
    # Arrange
    resolver = Resolver()

    async def dependency() -> int:
        await asyncio.sleep(0)
        return 7

    async def sample_function(a: int, dep: int = Depends(dependency)) -> int:
        await asyncio.sleep(0)
        return a + dep

    resolved_function = resolver.resolve_function(sample_function)

    # Act
    result = await resolved_function(3)

    # Assert
    assert result == 10


async def test_manual_kwargs_with_partial_dependencies_async():
    """
    Verify providing manual kwargs to an async function with dependencies works correctly.

    Given: a Resolver and an async function with some dependencies,
    When: resolve_function is called and manual kwargs are provided,
    Then: it should use the manual kwargs and inject the rest of the dependencies.
    """
    # Arrange
    resolver = Resolver()

    async def dependency() -> int:
        await asyncio.sleep(0)
        return 8

    async def sample_function(a: int = 0, dep: int = Depends(dependency)) -> int:
        await asyncio.sleep(0)
        return a + dep

    resolved_function = resolver.resolve_function(sample_function)

    # Act
    result = await resolved_function(a=4)

    # Assert
    assert result == 12


def test_resolve_forward_reference():
    """
    Verify that a forward-referenced type can be resolved correctly.

    Given: a Resolver and a function with a forward-referenced type,
    When: resolve_function is called,
    Then: it should return a resolved function with the forward-referenced type injected.
    """
    # Arrange
    resolver = Resolver()

    def some_dep() -> Dependency:
        """Test dependency that returns SampleType."""
        return Dependency(42)

    def test_dep(val: Dependency = Depends(some_dep)) -> Dependency:
        """Test dependency that uses SampleType."""
        return val

    # Act
    resolved_function = resolver.resolve_function(test_dep)
    result = resolved_function()

    # Assert
    assert isinstance(result, Dependency)
    assert result.value == 42


def test_resolve_with_annotated_no_depends():
    """
    Verify that a type with Annotated but no Depends can be resolved correctly.

    Given: a Resolver and a function with an Annotated type without Depends,
    When: resolve_function is called,
    Then: it should return the original function unchanged.
    """
    # Arrange
    resolver = Resolver()

    def test_dep(val: Annotated[int, "SomeMetadata"]) -> int:
        """Test dependency that uses Annotated int."""
        return val + 1

    # Act
    resolved_function = resolver.resolve_function(test_dep)

    # Assert
    assert resolved_function is test_dep
    assert resolved_function(5) == 6


def test_resolve_with_mixed_annotations():
    """
    Verify that a function with mixed annotations can be resolved correctly.

    Given: a Resolver and a function with mixed annotations (some with Depends, some without),
    When: resolve_function is called,
    Then: it should return a resolved function with dependencies injected where applicable.
    """
    # Arrange
    resolver = Resolver()

    def get_integer() -> int:
        return 10

    def test_dep(
        val1: Annotated[int, Depends(get_integer)] = AUTO,
        val2: Annotated[str, "SomeMetadata"] = AUTO,
    ) -> str:
        """Test dependency that uses mixed annotations."""
        return f"{val2}-{val1}"

    # Act
    resolved_function = resolver.resolve_function(test_dep)

    # Assert
    assert resolved_function(val2="value") == "value-10"


def test_resolve_type_no_annotation_errors():
    """
    Verify that resolving a type without an annotation raises an error.

    Given: a Resolver and a type without an annotation,
    When: resolve_type is called,
    Then: it should raise a RegistrationError.
    """
    # Arrange
    resolver = Resolver()

    def test_dep(
        val,  # pyright: ignore[reportMissingParameterType,reportUnknownParameterType]
    ) -> int:
        """Test dependency without type annotation."""
        return 1

    sig = signature(test_dep)  # pyright: ignore[reportUnknownArgumentType]
    param = sig.parameters["val"]

    # Act & Assert
    with pytest.raises(RegistrationError, match="Parameter .*val.* has no type annotation."):
        resolver._resolve_type(param, param.annotation)  # pyright: ignore[reportPrivateUsage]


def test_resolve_type_with_inline_annotation():
    """
    Verify that a type can be resolved with an inline annotation.

    Given: a Resolver and a function with an inline annotation,
    When: resolve_type is called,
    Then: it should return an instance of the type with dependencies injected.
    """
    # Arrange
    resolver = Resolver()

    class SampleType:
        def __init__(self, val: int):
            self.val = val

    def get_sample_type(val: int = Depends(get_dep)) -> SampleType:
        """Create a factory function for SampleType."""
        return SampleType(val)

    def function_with_dependency(
        dep: Annotated[SampleType, Depends(get_sample_type)] = AUTO,
    ) -> int:
        return dep.val + 1

    # Act
    resolved_instance = resolver.resolve_function(function_with_dependency)

    # Assert
    assert resolved_instance() == get_dep() + 1


def test_resolve_type_with_annotated_type_but_no_depends():
    """
    Verify that a type can be resolved when using Annotated without Depends.

    Given: a Resolver and a function with an Annotated type without Depends,
    When: resolve_type is called,
    Then: it should return None indicating no dependency to resolve.
    """
    # Arrange
    resolver = Resolver()

    def function_with_annotated_type(dep: Annotated[int, "SomeMetadata"]) -> int:
        return dep + 1

    sig = signature(function_with_annotated_type)
    param = sig.parameters["dep"]

    # Act
    result = resolver._resolve_type(param, param.annotation)  # pyright: ignore[reportPrivateUsage]

    # Assert
    assert result is None
