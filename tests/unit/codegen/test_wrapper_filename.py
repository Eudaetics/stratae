"""Test suite for wrapper filename generation."""

from typing import Callable

from stratae.codegen import wrapper_filename


def _module_level_func(): ...


class _RegularClass: ...


def _make_nested_func() -> Callable[[], None]:
    def _inner(): ...

    return _inner


class _Named:
    """Callable with a raising __repr__, to catch any accidental repr() evaluation."""

    __name__: str
    __qualname__: str
    __module__: str

    def __call__(self): ...

    def __repr__(self) -> str:
        raise AssertionError("repr should not be evaluated when a name is available")


def test_wrapper_filename_uses_module_and_qualname():
    """
    wrapper_filename should embed the function's module, qualname, and identity.

    Given: A plain module-level function.
    When: Generating its wrapper filename.
    Then: The filename should contain the function's module, qualname, and id.
    """
    # Act
    result = wrapper_filename(_module_level_func)

    # Assert
    assert __name__ in result
    assert _module_level_func.__qualname__ in result
    assert f"{id(_module_level_func):#x}" in result


def test_wrapper_filename_includes_locals_for_nested_function():
    """
    wrapper_filename should reflect closures via their <locals> qualname segment.

    Given: A function defined inside another function.
    When: Generating its wrapper filename.
    Then: The filename should include the enclosing function in its qualname.
    """
    # Arrange
    inner = _make_nested_func()

    # Act
    result = wrapper_filename(inner)

    # Assert
    assert inner.__qualname__ in result
    assert "<locals>" in result


def test_wrapper_filename_supports_classes():
    """
    wrapper_filename should work for classes, not just plain functions.

    Given: A class.
    When: Generating its wrapper filename.
    Then: The filename should contain the class's module and qualname.
    """
    # Act
    result = wrapper_filename(_RegularClass)

    # Assert
    assert __name__ in result
    assert _RegularClass.__qualname__ in result
    assert f"{id(_RegularClass):#x}" in result


def test_wrapper_filename_falls_back_to_name_without_qualname():
    """
    wrapper_filename should fall back to __name__ when __qualname__ is unavailable.

    Given: A callable instance with __name__ set as an instance attribute but no __qualname__.
        Its __module__ is inherited from its class, since that dunder (unlike __qualname__)
        remains a real class-dict entry rather than being popped into a type slot.
    When: Generating its wrapper filename.
    Then: The filename should use __name__ in place of __qualname__.
    """
    # Arrange
    obj = _Named()
    obj.__name__ = "custom_name"
    assert not hasattr(obj, "__qualname__")

    # Act
    result = wrapper_filename(obj)

    # Assert
    assert _Named.__module__ in result
    assert "custom_name" in result
    assert f"{id(obj):#x}" in result


def test_wrapper_filename_falls_back_to_repr_without_name_or_qualname():
    """
    wrapper_filename should fall back to repr() when no name is available at all.

    Given: A bare object with neither __name__, __qualname__, nor __module__.
    When: Generating its wrapper filename.
    Then: The filename should embed repr(obj) and "?" for the module.
    """
    # Arrange
    obj = object()
    assert not hasattr(obj, "__qualname__")
    assert not hasattr(obj, "__name__")
    assert not hasattr(obj, "__module__")

    # Act
    result = wrapper_filename(obj)

    # Assert
    assert "?" in result
    assert repr(obj) in result
    assert f"{id(obj):#x}" in result


def test_wrapper_filename_does_not_evaluate_repr_when_qualname_present():
    """
    wrapper_filename should not evaluate repr() unless it's actually needed.

    Given: A callable instance with __qualname__ and __module__ set as instance attributes,
        and a __repr__ that raises if called.
    When: Generating its wrapper filename.
    Then: No exception should be raised, proving repr() was never evaluated.
    """
    # Arrange
    obj = _Named()
    obj.__qualname__ = "BrokenRepr"
    obj.__module__ = "broken.module"

    # Act
    result = wrapper_filename(obj)

    # Assert
    assert result == f"<stratae: broken.module.BrokenRepr@{id(obj):#x}>"


def test_wrapper_filename_is_stable_for_the_same_object():
    """
    wrapper_filename should return the same value for repeated calls on the same object.

    Given: A single function.
    When: Generating its wrapper filename twice.
    Then: Both results should be identical.
    """
    # Act
    first = wrapper_filename(_module_level_func)
    second = wrapper_filename(_module_level_func)

    # Assert
    assert first == second


def test_wrapper_filename_differs_between_distinct_functions():
    """
    wrapper_filename should distinguish between different functions.

    Given: Two distinct functions.
    When: Generating their wrapper filenames.
    Then: The filenames should differ.
    """

    # Arrange
    def _other(): ...

    # Act
    first = wrapper_filename(_module_level_func)
    second = wrapper_filename(_other)

    # Assert
    assert first != second
