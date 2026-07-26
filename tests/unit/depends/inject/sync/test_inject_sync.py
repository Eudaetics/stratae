"""Tests for the inject decorator in the dependency injection system."""

from functools import wraps
from typing import Annotated, Callable

import pytest

from stratae.depends import Depends, inject
from stratae.depends.exceptions import InjectionSignatureError

type IntDependency = Annotated[int, Depends(lambda: 42)]


def test_inject():
    """
    Test the inject decorator with a simple dependency.

    Given: a function with a dependency,
    When: the function is decorated with @inject,
    Then: it should resolve the dependency correctly.
    """

    # Arrange
    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    def factory_function() -> SampleType:
        """Create a factory function that returns a SampleType instance."""
        return SampleType(5)

    @inject
    def test_dep(val: Annotated[SampleType, Depends(factory_function)]) -> SampleType:
        return val

    # Act
    result = test_dep()

    # Assert
    assert isinstance(result, SampleType)
    assert result.value == 5


def test_inject_multiple_calls():
    """
    Test the inject decorator with multiple factory calls to ensure it returns new instances.

    Given: a function with a factory dependency,
    When: the function is called multiple times,
    Then: it should return new instances each time.
    """

    # Arrange
    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    counter = 0

    def factory_function() -> SampleType:
        """Create factory function that returns a SampleType instance."""
        nonlocal counter
        return SampleType(counter := counter + 1)

    @inject
    def test_dep(val: Annotated[SampleType, Depends(factory_function)]) -> SampleType:
        return val

    # Act
    result_1 = test_dep()
    result_2 = test_dep()

    # Assert
    assert isinstance(result_1, SampleType)
    assert isinstance(result_2, SampleType)
    assert result_1 is not result_2
    assert result_1.value == 1
    assert result_2.value == 2


def test_inject_with_parens():
    """
    Test the inject decorator with parentheses to ensure it works correctly.

    Given: a function with a dependency,
    When: the function is decorated with @inject(),
    Then: it should resolve the dependency correctly.
    """

    # Arrange
    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    def factory_function() -> SampleType:
        """Create a factory function that returns a SampleType instance."""
        return SampleType(15)

    @inject()
    def test_dep(val: Annotated[SampleType, Depends(factory_function)]) -> SampleType:
        return val

    # Act
    result = test_dep()

    # Assert
    assert isinstance(result, SampleType)
    assert result.value == 15


def test_inject_annotated_dependency():
    """
    Test the inject decorator with Annotated dependencies.

    Given: a function with Annotated dependencies,
    When: the function is decorated with @inject,
    Then: it should resolve the dependencies correctly.
    """
    # Arrange
    from typing import Annotated

    class SampleType:
        """A sample type for testing."""

        def __init__(self, value: int):
            self.value = value

    def factory_function() -> SampleType:
        """Create a factory function that returns a SampleType instance."""
        return SampleType(35)

    @inject
    def test_dep(val: Annotated[SampleType, Depends(factory_function)]) -> SampleType:
        return val

    # Act
    result = test_dep()

    # Assert
    assert isinstance(result, SampleType)
    assert result.value == 35


def test_inject_with_type_alias():
    """
    Test the inject decorator with a type alias for Annotated dependencies.

    Given: a function with a type alias for Annotated dependencies,
    When: the function is decorated with @inject,
    Then: it should resolve the dependencies correctly.
    """

    # Arrange
    @inject
    def test_dep(val: IntDependency) -> int:
        return val

    # Act
    result = test_dep()

    # Assert
    assert isinstance(result, int)
    assert result == 42


def test_nested_annotations():
    """
    Test the inject decorator with nested Annotated dependencies.

    Given: a function with nested Annotated dependencies,
    When: the function is decorated with @inject,
    Then: it should resolve the dependencies correctly.
    """

    # Arrange
    def get_two() -> int:
        """Create a factory function that returns a SampleType instance."""
        return 2

    @inject
    def get_one(dep2: Annotated[int, Depends(get_two)]) -> int:
        """Create a factory function that returns a SampleType instance."""
        return 1 + dep2

    @inject
    def test_dep(val: Annotated[int, Depends(get_one)]) -> int:
        return val - 3

    # Act
    result = test_dep()

    # Assert
    assert result == 0


def test_mixed_depends_types():
    """
    Test the inject decorator with a mix of Annotated and traditional Depends.

    Given: a function with mixed dependency methods,
    When: the function is decorated with @inject,
    Then: it should resolve all dependencies correctly.
    """

    # Arrange
    def get_two() -> int:
        """Return the integer 2."""
        return 2

    def get_three() -> int:
        """Return the integer 3."""
        return 3

    @inject
    def get_dep(
        no_default: int,
        type_dep: IntDependency,
        annotated_dep: Annotated[int, Depends(get_two)],
        db: Annotated[int, Depends(get_three)],
        no_dep: int = -2,
    ) -> int:
        """Sum the various dependencies."""
        return no_default + no_dep + type_dep + annotated_dep + db

    # Act
    result = get_dep(5)

    # Assert
    assert result == 5 - 2 + 42 + 2 + 3


def test_behavior_with_annotated_and_default():
    """
    Injected functions cannot use default with dependencies.

    Given: a function with Annotated dependencies with defaults,
    When: the function is decorated with @inject,
    Then: A InjectionSignatureError should be raised.
    """

    # Arrange
    def get_forty_two() -> int:
        """Return the integer 42."""
        return 42

    # Act & Assert
    with pytest.raises(
        InjectionSignatureError, match="Cannot use a default with injected parameter val"
    ):

        @inject
        def _(val: Annotated[int, Depends(get_forty_two)] = 10) -> int:
            return val


def test_inject_with_outer_wrapper():
    """
    Test the inject decorator with an outer wrapper function.

    Given: a function wrapped by another decorator,
    When: the function is decorated with @inject,
    Then: it should resolve the dependencies correctly.
    """

    # Arrange
    def outer_wrapper(func: Callable[[], int]) -> Callable[[], int]:
        @wraps(func)
        def inner_wrapper() -> int:
            return func() + 1

        return inner_wrapper

    @outer_wrapper
    @inject
    def test_dep(val: IntDependency) -> int:
        return val

    # Act
    result = test_dep()

    # Assert
    assert result == 43
