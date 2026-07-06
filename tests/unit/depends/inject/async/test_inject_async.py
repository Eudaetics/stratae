"""Tests for the inject decorator in the dependency injection system."""

import asyncio
from functools import wraps
from typing import Annotated, Any, Callable

import pytest

from stratae.depends import Depends, Injected, inject
from stratae.depends.exceptions import RegistrationError


async def async_dep() -> int:
    """Define an asynchronous dependency for testing the type annotation."""
    await asyncio.sleep(0)
    return 42


type IntDependency = Annotated[int, Depends(async_dep)]


async def test_inject_with_async():
    """
    Test the inject decorator with an async dependency.

    Given: an async function with a dependency,
    When: the function is decorated with @inject,
    Then: it should resolve the dependency correctly.
    """

    # Arrange
    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    async def async_factory_function() -> SampleType:
        """Create async factory function that returns a SampleType instance."""
        await asyncio.sleep(0)
        return SampleType(10)

    @inject
    async def test_dep(val: Injected[SampleType, Depends(async_factory_function)]) -> SampleType:
        return val

    # Act
    result = await test_dep()

    # Assert
    assert isinstance(result, SampleType)
    assert result.value == 10


async def test_inject_with_async_multiple_calls():
    """
    Test the inject decorator with an async factory to ensure it returns new instances.

    Given: an async function with a factory dependency,
    When: the function is called multiple times,
    Then: it should return new instances each time.
    """

    # Arrange
    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    counter = 0

    async def async_factory_function() -> SampleType:
        """Create async factory function that returns a SampleType instance."""
        nonlocal counter
        await asyncio.sleep(0)
        return SampleType(counter := counter + 1)

    @inject
    async def test_dep(val: Injected[SampleType, Depends(async_factory_function)]) -> SampleType:
        return val

    # Act
    result_1 = await test_dep()
    result_2 = await test_dep()

    # Assert
    assert isinstance(result_1, SampleType)
    assert isinstance(result_2, SampleType)
    assert result_1 is not result_2
    assert result_1.value == 1
    assert result_2.value == 2


async def test_inject_async_mixed():
    """
    Test the inject decorator with a mix of sync and async dependencies.

    Given: a function with both sync and async dependencies,
    When: the function is decorated with @inject,
    Then: it should resolve both dependencies correctly.
    """

    # Arrange
    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    async def async_factory_function() -> SampleType:
        """Create async factory function that returns a SampleType instance."""
        await asyncio.sleep(0)
        return SampleType(20)

    def sync_factory_function() -> SampleType:
        """Create sync factory function that returns a SampleType instance."""
        return SampleType(30)

    @inject
    async def test_dep(
        val1: Injected[SampleType, Depends(async_factory_function)],
        val2: Injected[SampleType, Depends(sync_factory_function)],
    ) -> tuple[SampleType, SampleType]:
        return val1, val2

    # Act
    result1, result2 = await test_dep()
    result_other_1, result_other_2 = await test_dep()

    # Assert
    assert isinstance(result1, SampleType)
    assert isinstance(result2, SampleType)
    assert result1.value == 20
    assert result2.value == 30
    assert result1 is not result_other_1
    assert result2 is not result_other_2
    assert result_other_1.value == 20
    assert result_other_2.value == 30


def test_inject_sync_async_nested_error():
    """
    Test the inject decorator with a sync function that has async dependencies.

    Given: a sync function with an async dependency,
    When: the function is decorated with @inject,
    Then: it should raise a RegistrationError.
    """

    # Arrange
    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    def sync_after_async_factory_function() -> SampleType:
        """Create sync factory function that returns a SampleType instance after async call."""
        return SampleType(100)

    @inject
    async def async_factory_function(
        val: Injected[SampleType, Depends(sync_after_async_factory_function)],
    ) -> SampleType:
        """Create async factory function that returns a SampleType instance."""
        return SampleType(20 + val.value)

    # Act and Assert
    with pytest.raises(
        RegistrationError, match="Sync function '.*' cannot have async dependencies."
    ):

        @inject
        def _(val: Injected[SampleType, Depends(async_factory_function)]) -> SampleType:
            """Create sync factory function that returns a SampleType instance."""
            return SampleType(5 + val.value)


async def test_inject_async_sync_nested():
    """
    Test the inject decorator with an async function that has a sync dependency.

    Given: an async function with a sync dependency,
    When: the function is decorated with @inject,
    Then: it should resolve the dependency correctly.
    """

    # Arrange
    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    def sync_factory_function() -> SampleType:
        """Create async factory function that returns a SampleType instance."""
        return SampleType(20)

    # Act
    @inject
    async def async_factory_function(
        val: Injected[SampleType, Depends(sync_factory_function)],
    ) -> SampleType:
        """Create async factory function that returns a SampleType instance."""
        return SampleType(5 + val.value)

    # Assert
    result = await async_factory_function()
    assert result.value == 25


async def test_inject_with_parens_and_async():
    """
    Test the inject decorator with parentheses and async dependencies.

    Given: an async function with a dependency,
    When: the function is decorated with @inject(),
    Then: it should resolve the dependency correctly.
    """

    # Arrange
    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    async def async_factory_function() -> SampleType:
        """Create an async factory function that returns a SampleType instance."""
        await asyncio.sleep(0)
        return SampleType(25)

    @inject()
    async def test_dep(val: Injected[SampleType, Depends(async_factory_function)]) -> SampleType:
        return val

    # Act
    result = await test_dep()

    # Assert
    assert isinstance(result, SampleType)
    assert result.value == 25


async def test_inject_annotated_async_dependency():
    """
    Test the inject decorator with Annotated async dependencies.

    Given: an async function with Annotated dependencies,
    When: the function is decorated with @inject,
    Then: it should resolve the dependencies correctly.
    """
    # Arrange
    from typing import Annotated

    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    async def async_factory_function() -> SampleType:
        """Create an async factory function that returns a SampleType instance."""
        await asyncio.sleep(0)
        return SampleType(45)

    @inject
    async def test_dep(
        val: Annotated[SampleType, Depends(async_factory_function)],
    ) -> SampleType:
        return val

    # Act
    result = await test_dep()

    # Assert
    assert isinstance(result, SampleType)
    assert result.value == 45


async def test_inject_with_type_alias_async():
    """
    Test the inject decorator with a type alias for Annotated dependencies.

    Given: a function with a type alias for Annotated dependencies,
    When: the function is decorated with @inject,
    Then: it should resolve the dependencies correctly.
    """

    # Arrange
    @inject
    async def test_dep(val: IntDependency) -> int:
        await asyncio.sleep(0)
        return val

    # Act
    result = await test_dep()

    # Assert
    assert isinstance(result, int)
    assert result == 42


async def test_nested_annotations_async():
    """
    Test the inject decorator with nested Annotated dependencies.

    Given: a function with nested Annotated dependencies,
    When: the function is decorated with @inject,
    Then: it should resolve the dependencies correctly.
    """

    # Arrange
    async def get_two() -> int:
        """Create a factory function that returns a SampleType instance."""
        await asyncio.sleep(0)
        return 2

    @inject
    async def get_one(dep2: Annotated[int, Depends(get_two)]) -> int:
        """Create a factory function that returns a SampleType instance."""
        await asyncio.sleep(0)
        return 1 + dep2

    @inject
    async def test_dep(val: Annotated[int, Depends(get_one)]) -> int:
        await asyncio.sleep(0)
        return val - 3

    # Act
    result = await test_dep()

    # Assert
    assert result == 0


async def test_mixed_depends_types_async():
    """
    Test the inject decorator with a mix of Annotated and traditional Depends.

    Given: a function with mixed dependency methods,
    When: the function is decorated with @inject,
    Then: it should resolve all dependencies correctly.
    """

    # Arrange
    async def get_two() -> int:
        """Return the integer 2."""
        await asyncio.sleep(0)
        return 2

    def get_three() -> int:
        """Return the integer 3."""
        return 3

    @inject
    async def get_dep(
        no_default: int,
        type_dep: IntDependency,
        annotated_dep: Annotated[int, Depends(get_two)],
        db: Injected[int, Depends(get_three)],
        no_dep: int = -2,
    ) -> int:
        """Sum the various dependencies."""
        return no_default + no_dep + type_dep + annotated_dep + db

    # Act
    result = await get_dep(5)

    # Assert
    assert result == 5 - 2 + 42 + 2 + 3


async def test_behavior_with_annotated_and_default_async():
    """
    Injected params cannot use defaults.

    Given: a function with Annotated dependencies with defaults,
    When: the function is decorated with @inject,
    Then: A RegistrationError should be raised.
    """

    # Arrange
    async def get_forty_two() -> int:
        """Return the integer 42."""
        await asyncio.sleep(0)
        return 42

    # Act & Assert
    with pytest.raises(RegistrationError, match="Cannot use a default with injected parameter val"):

        @inject
        async def _(val: Annotated[int, Depends(get_forty_two)] = 10) -> int:
            await asyncio.sleep(0)
            return val


async def test_inject_with_outer_wrapper_async():
    """
    Test the inject decorator with an outer async wrapper function.

    Given: an async function wrapped by another async decorator,
    When: the function is decorated with @inject,
    Then: it should resolve the dependencies correctly.
    """

    # Arrange
    def outer_wrapper(func: Callable[[], Any]) -> Callable[[], Any]:
        @wraps(func)
        async def inner_wrapper() -> int:
            return await func() + 1

        return inner_wrapper

    @outer_wrapper
    @inject
    async def test_dep(val: IntDependency) -> int:
        return val

    # Act
    result = await test_dep()

    # Assert
    assert result == 43
