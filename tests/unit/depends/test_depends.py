"""Test suite for the Depends class in the dependency injection module."""

import threading
import time
from collections.abc import Callable
from typing import Annotated, Any

import pytest

from stratae.depends import Depends, inject, override
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


class _SlowHash:
    """A dependency whose hash takes a while when slow, so registry lookups and inserts overlap."""

    def __init__(self) -> None:
        self.slow = True

    def __call__(self) -> str:
        return "real"

    def __hash__(self) -> int:
        if self.slow:
            time.sleep(0.005)
        return id(self)


def _depends_at_once(dependency: _SlowHash) -> list[Provider]:
    """Call Depends on a slow dependency from eight threads at once, and return what each got."""
    start = threading.Barrier(8)
    got: list[Any] = [None] * 8

    def call(i: int) -> None:
        start.wait()
        got[i] = Depends(dependency)

    threads = [threading.Thread(target=call, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    dependency.slow = False
    return got


def test_depends_from_threads_at_once_gives_one_provider():
    """
    Verify that threads calling Depends on one dependency at once all get one Provider.

    Given: a dependency no thread has asked for yet, whose hash takes a while,
    When: eight threads call Depends on it at the same moment,
    Then: every thread gets the same Provider, and Depends returns it afterwards too.
    """
    # Arrange
    dependency = _SlowHash()

    # Act
    got = _depends_at_once(dependency)

    # Assert
    assert all(provider is got[0] for provider in got)
    assert Depends(dependency) is got[0]


def test_override_reaches_functions_injected_from_threads_at_once():
    """
    Verify that an override reaches functions injected from Depends calls made at once.

    Given: eight threads that called Depends on one dependency at the same moment,
    When: a function is injected with each thread's Provider, and the dependency is overridden,
    Then: every function sees the override.
    """
    # Arrange
    dependency = _SlowHash()
    got = _depends_at_once(dependency)
    functions: list[Callable[[], str]] = []
    for provider in got:

        @inject
        def read(value: Annotated[str, provider]) -> str:
            return value

        functions.append(read)

    # Act
    with override(dependency).to("overridden"):
        seen = [read() for read in functions]

    # Assert
    assert seen == ["overridden"] * 8
