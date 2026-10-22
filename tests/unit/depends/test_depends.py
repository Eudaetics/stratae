"""Test suite for the Depends class in the dependency injection module."""

import pytest

from stratae.depends import Depends
from stratae.depends._provide import Provider
from stratae.depends.exceptions import InjectionSignatureError


def test_depends_initialization():
    """
    Verify that Depends can be initialized with a dependency.

    Given: a dependency,
    When: Depends is initialized with that dependency,
    Then: it should store the dependency correctly.
    """

    # Arrange
    def sample_dependency():
        return "sample"

    # Act
    depends_instance = Depends(sample_dependency)

    # Assert
    assert isinstance(depends_instance, Provider)
    assert depends_instance.dependency == sample_dependency


def test_depends_with_lambda():
    """
    Verify that Depends can be initialized with a lambda dependency.

    In general, using lambdas with dependency injection is discouraged. However, there is
    no restriction in the current implementation.

    Given: a lambda dependency,
    When: Depends is initialized with that dependency,
    Then: it should store the dependency correctly.
    """
    # Arrange
    # Ignore linting warning for lambda usage in this test
    sample_lambda = lambda: "sample"  # noqa: E731

    # Act
    depends_instance = Depends(sample_lambda)

    # Assert
    assert isinstance(depends_instance, Provider)
    assert depends_instance.dependency == sample_lambda


def test_depends_rejects_unhashable_callable():
    """
    Verify that Depends rejects a callable that can't be registered.

    Given: a callable object that is unhashable,
    When: Depends is initialized with it,
    Then: it should raise InjectionSignatureError instead of a bare TypeError.
    """

    # Arrange
    class Unhashable:
        __hash__ = None  # type: ignore[assignment]

        def __call__(self) -> str:
            return "sample"

    # Act & Assert
    with pytest.raises(InjectionSignatureError, match="unhashable"):
        Depends(Unhashable())
