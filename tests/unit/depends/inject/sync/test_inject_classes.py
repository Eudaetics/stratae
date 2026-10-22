"""Test suite for verifying dependency injection in class methods and constructors."""

from functools import partial
from typing import Annotated, Any, Callable

import pytest

from stratae.depends import Depends, inject
from stratae.depends.exceptions import InjectionSignatureError


def get_dep():
    """Dependent function returning an integer."""
    return 42


async def get_async_dep() -> int:
    """Async dependent function returning an integer."""
    return 43


def test_class_init():
    """
    Test injection in class __init__ method.

    Given: A class with an __init__ method that is injected with a dependency.
    When: An instance of the class is created.
    Then: The dependency is correctly injected into the instance.
    """

    # Arrange
    class SimpleObject:
        @inject
        def __init__(self, value: Annotated[int, Depends(get_dep)]):
            self.value = value

    # Act
    instance = SimpleObject()

    # Assert
    assert instance.value == 42


def test_nested_class_injection():
    """
    Test injecting classes as dependencies.

    Given: A class that depends on another class which itself has a dependency.
    When: An instance of the outer class is created.
    Then: The inner class is correctly instantiated with its dependency, and the outer class
          receives the correctly constructed inner class instance.
    """
    # Arrange

    class DepClass:
        @inject
        def __init__(self, value: Annotated[int, Depends(get_dep)]):
            self.value = value

        def get_value(self):
            return self.value + 8

    class SimpleObject:
        @inject
        def __init__(self, dep: Annotated[DepClass, Depends(DepClass)]):
            self.dep = dep

    # Act
    instance = SimpleObject()

    # Assert
    assert instance.dep.get_value() == 50


def test_method_injection_in_class():
    """
    Test injection in a regular method of a class.

    Given: A class with a method that is injected with a dependency.
    When: The method is called on an instance of the class.
    Then: The dependency is correctly injected into the method.
    """

    # Arrange
    class SimpleObject:
        def __init__(self):
            self.value = 0

        @inject
        def set_value(self, value: Annotated[int, Depends(get_dep)]):
            self.value = value

    # Act
    instance = SimpleObject()
    instance.set_value()

    # Assert
    assert instance.value == 42


def test_static_method_injection_in_class():
    """
    Test injection in a static method of a class.

    Given: A class with a static method that is injected with a dependency.
    When: The static method is called.
    Then: The dependency is correctly injected into the static method.
    """

    # Arrange
    class SimpleObject:
        value = 0

        @staticmethod
        @inject
        def set_value(value: Annotated[int, Depends(get_dep)]):
            SimpleObject.value = value

    # Act
    SimpleObject.set_value()

    # Assert
    assert SimpleObject.value == 42


def test_class_method_injection_in_class():
    """
    Test injection in a class method of a class.

    Given: A class with a class method that is injected with a dependency.
    When: The class method is called.
    Then: The dependency is correctly injected into the class method.
    """

    # Arrange
    class SimpleObject:
        value = 0

        @classmethod
        @inject
        def set_value(cls, value: Annotated[int, Depends(get_dep)]):
            return value

    # Act
    result = SimpleObject.set_value()

    # Assert
    assert result == 42


def test_inherited_class_injection():
    """
    Test injection in an inherited class.

    Given: A childe class inheriting from a base class with an __init__ method that is injected.
    When: An instance of the child class is created.
    Then: The dependency is correctly injected into the base class's __init__ method.
    """

    # Arrange
    class BaseClass:
        @inject
        def __init__(self, value: Annotated[int, Depends(get_dep)]):
            self.value = value

    class ChildClass(BaseClass):
        pass

    # Act
    instance = ChildClass()

    # Assert
    assert instance.value == 42


def test_inherited_class_method_injection():
    """
    Test injection in an inherited class method.

    Given: A child class inheriting from a base class with a class method that is injected.
    When: The child class's method is called.
    Then: The dependency is correctly injected into the child class's method.
    """

    # Arrange
    class BaseClass:
        @classmethod
        @inject
        def get_value(cls, value: Annotated[int, Depends(get_dep)]):
            return value

    class ChildClass(BaseClass):
        @classmethod
        @inject
        def get_value(cls, value: Annotated[int, Depends(get_dep)]):
            return value + 1

    # Act
    base = BaseClass.get_value()
    result = ChildClass.get_value()

    # Assert
    assert base == 42
    assert result == 43


def test_injection_with_other_params():
    """
    Test injection in a class __init__ method with other non-injected parameters.

    Given: A class with an __init__ method that has both injected and non-injected parameters.
    When: An instance of the class is created with a non-injected parameter.
    Then: The dependency is correctly injected and the non-injected parameter is set.
    """

    # Arrange
    class SimpleObject:
        @inject
        def __init__(self, value: Annotated[int, Depends(get_dep)], other: str = "default"):
            self.value = value
            self.other = other

    # Act
    instance = SimpleObject(other="custom")

    # Assert
    assert instance.value == 42
    assert instance.other == "custom"


def test_multiple_injections():
    """
    Test multiple injections in a class __init__ method.

    Given: A class with an __init__ method that has multiple injected parameters.
    When: An instance of the class is created.
    Then: All dependencies are correctly injected.
    """

    # Arrange
    def get_another_dep():
        return "hello"

    class SimpleObject:
        @inject
        def __init__(
            self,
            value: Annotated[int, Depends(get_dep)],
            text: Annotated[str, Depends(get_another_dep)],
        ):
            self.value = value
            self.text = text

    # Act
    instance = SimpleObject()

    # Assert
    assert instance.value == 42
    assert instance.text == "hello"


def test_no_injection():
    """
    Test class creation without any injections.

    Given: A class with an injected __init__ method that has no injected parameters.
    When: An instance of the class is created.
    Then: The instance is created correctly with the provided parameters.
    """

    # Arrange
    class SimpleObject:
        @inject
        def __init__(self, value: int):
            self.value = value

    # Act
    instance = SimpleObject(value=1)

    # Assert
    assert instance.value == 1


def test_injecting_classes():
    """
    Test injecting classes as dependencies.

    Given: A class that depends on another class which itself has a dependency.
    When: An instance of the outer class is created.
    Then: The inner class is correctly instantiated with its dependency, and the outer class
          receives the correctly constructed inner class instance.
    """
    # Arrange

    class DepClass:
        def __init__(self, value: Annotated[int, Depends(get_dep)]):
            self.value = value

        def get_value(self):
            return self.value + 8

    class SimpleObject:
        @inject
        def __init__(self, dep: Annotated[DepClass, Depends(DepClass)]):
            self.dep = dep

    # Act
    instance = SimpleObject()

    # Assert
    assert instance.dep.get_value() == 50


class _SyncHandler:
    def __call__(self, value: Annotated[int, Depends(get_dep)]) -> int:
        return value


class _AsyncHandler:
    async def __call__(self, value: Annotated[int, Depends(get_async_dep)]) -> int:
        return value


def _add(value: Annotated[int, Depends(get_dep)], other: int) -> int:
    return value + other


@pytest.mark.parametrize(
    "target",
    [_SyncHandler(), _AsyncHandler(), partial(_add, other=1)],
    ids=["sync_object", "async_object", "partial"],
)
def test_callable_object_with_injected_parameters_rejected(target: Callable[..., Any]):
    """
    Test that injecting into a callable object raises a clear error.

    Given: A callable object - an instance with __call__, or a functools.partial - with
        an injected parameter.
    When: inject is applied to it.
    Then: InjectionSignatureError is raised, pointing at decorating __call__ instead,
        since inject would replace the object with a function.
    """
    with pytest.raises(InjectionSignatureError, match="Decorate its __call__ method"):
        inject(target)


def test_callable_object_provider_with_injected_parameters_rejected():
    """
    Test that a callable object provider with injected parameters raises a clear error.

    Given: A callable object whose __call__ has an injected parameter, used as a provider.
    When: A function depending on it is injected.
    Then: InjectionSignatureError is raised for the provider.
    """
    handler = _SyncHandler()

    def consumer(result: Annotated[int, Depends(handler)]) -> int:
        return result

    with pytest.raises(InjectionSignatureError, match="Cannot inject into"):
        inject(consumer)


def test_callable_object_provider_without_injected_parameters_allowed():
    """
    Test that a callable object with no injected parameters works as a provider.

    Given: A callable object taking no injected parameters, used as a provider.
    When: A function depending on it is injected and called.
    Then: The object's return value is injected.
    """

    # Arrange
    class Settings:
        def __call__(self) -> int:
            return 7

    @inject
    def consumer(value: Annotated[int, Depends(Settings())]) -> int:
        return value

    # Act & Assert
    assert consumer() == 7


def test_injected_call_method_on_callable_object():
    """
    Test that decorating __call__ with inject injects into a callable object.

    Given: A class whose __call__ is decorated with inject.
    When: An instance is called directly, and used as a provider.
    Then: The dependency is injected both ways.
    """

    # Arrange
    class Handler:
        @inject
        def __call__(self, value: Annotated[int, Depends(get_dep)]) -> int:
            return value

    handler = Handler()

    @inject
    def consumer(result: Annotated[int, Depends(handler)]) -> int:
        return result

    # Act & Assert
    assert handler() == 42
    assert consumer() == 42
