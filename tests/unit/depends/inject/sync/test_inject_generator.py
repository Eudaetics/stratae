"""Test inject with generators."""

from typing import Annotated, Generator
from unittest.mock import Mock

from stratae.depends import Depends, inject


def test_inject_on_generator():
    """
    Test injection on a generator function.

    Given: a generator function that is decorated with inject
    When: the function is called
    Then: the injection should work correctly within the generator.
    """
    # Arrange
    mock_cleanup = Mock()

    # Act (the decorator)
    @inject
    def gen_func(dep: Annotated[int, Depends(lambda: 5)]):
        """Inject into a function that returns a generator."""
        for i in range(dep):
            yield i
        mock_cleanup()

    generator = gen_func()

    # Assert
    mock_cleanup.assert_not_called()
    assert list(generator) == [0, 1, 2, 3, 4]

    try:
        next(generator)
    except StopIteration:
        pass
    mock_cleanup.assert_called_once()


def test_inject_generator_dep():
    """
    Test injection of a generator function as a dependency.

    Given: a generator function used as a dependency
    When: another function is injected with this generator dependency
    Then: the injection should work correctly and the generator should be used.
    """
    # Arrange
    mock_cleanup = Mock()

    def gen_dep():
        for i in range(3):
            yield i
        mock_cleanup()

    @inject
    def func_with_gen(gen: Annotated[Generator[int, None, None], Depends(gen_dep)]):
        return list(gen)

    # Act
    result = func_with_gen()

    # Assert
    assert result == [0, 1, 2]
    mock_cleanup.assert_called_once()


def test_inject_nested_generators():
    """
    Test injection with nested generator functions.

    Given: a generator function that depends on another generator function
    When: the outer function is injected and called
    Then: the injection should work correctly through both generators.
    """

    # Arrange
    def inner_gen():
        for i in range(2):
            yield f"inner-{i}"

    @inject
    def outer_gen(dep: Annotated[Generator[str, None, None], Depends(inner_gen)]):
        for value in dep:
            yield f"outer-{value}"

    # Act
    result = list(outer_gen())

    # Assert
    assert result == ["outer-inner-0", "outer-inner-1"]


def test_inject_on_generator_with_args():
    """
    Test injection on a generator function that takes arguments.

    Given: a generator function that accepts parameters and is decorated with inject
    When: the function is called with arguments
    Then: the injection should work correctly alongside the provided arguments.
    """

    # Act (the decorator)
    @inject
    def gen_func_with_args(count: int, dep: Annotated[int, Depends(lambda: 2)]):
        for i in range(count):
            yield i * dep

    generator = gen_func_with_args(3)

    # Assert
    assert list(generator) == [0, 2, 4]


def test_inject_generator_with_kwargs():
    """
    Test injection on a generator function that takes keyword arguments.

    Given: a generator function that accepts keyword parameters and is decorated with inject
    When: the function is called with keyword arguments
    Then: the injection should work correctly alongside the provided keyword arguments.
    """

    # Act (the decorator)
    @inject
    def gen_func_with_kwargs(*, count: int = 3, dep: Annotated[int, Depends(lambda: 4)]):
        for i in range(count):
            yield i + dep

    generator = gen_func_with_kwargs(count=2)

    # Assert
    assert list(generator) == [4, 5]


def test_inject_generator_with_mixed_args():
    """
    Test injection on a generator function that takes both args and kwargs.

    Given: a generator function with both positional and keyword parameters, decorated with inject
    When: the function is called with a mix of arguments
    Then: the injection should work correctly alongside the provided arguments.
    """

    # Act (the decorator)
    @inject
    def gen_func_mixed_args(count: int, *, start: int = 0, dep: Annotated[int, Depends(lambda: 3)]):
        for i in range(start, count + start):
            yield i * dep

    generator = gen_func_mixed_args(2, start=4)

    # Assert
    assert list(generator) == [12, 15]
