"""Test suite for verifying dependency injection in class methods and constructors."""

import pytest

from stratae.depends import Depends, inject


def get_dep():
    """Dependent function returning an integer."""
    return 42


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
        def __init__(self, value: int = Depends(get_dep)):
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
        def __init__(self, value: int = Depends(get_dep)):
            self.value = value

        def get_value(self):
            return self.value + 8

    class SimpleObject:
        @inject
        def __init__(self, dep: DepClass = Depends(DepClass)):
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
        def set_value(self, value: int = Depends(get_dep)):
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
        def set_value(value: int = Depends(get_dep)):
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
        def set_value(cls, value: int = Depends(get_dep)):
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
        def __init__(self, value: int = Depends(get_dep)):
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
        def get_value(cls, value: int = Depends(get_dep)):
            return value

    class ChildClass(BaseClass):
        @classmethod
        @inject
        def get_value(cls, value: int = Depends(get_dep)):
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
        def __init__(self, value: int = Depends(get_dep), other: str = "default"):
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
        def __init__(self, value: int = Depends(get_dep), text: str = Depends(get_another_dep)):
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
        def __init__(self, value: int = Depends(get_dep)):
            self.value = value

        def get_value(self):
            return self.value + 8

    class SimpleObject:
        @inject
        def __init__(self, dep: DepClass = Depends(DepClass)):
            self.dep = dep

    # Act
    instance = SimpleObject()

    # Assert
    assert instance.dep.get_value() == 50


def test_circular_class_dependency():
    """
    Test handling of circular class dependencies.

    Given: Two classes that depend on each other.
    When: An attempt is made to create an instance of one of the classes.
    Then: A RecursionError is raised due to the circular dependency.
    """
    # Arrange

    class ClassA:
        @inject
        def __init__(self, b: "ClassB" = Depends(lambda: ClassB())):
            self.b = b

    class ClassB:
        @inject
        def __init__(self, a: ClassA = Depends(lambda: ClassA())):
            self.a = a

    # Act & Assert
    with pytest.raises(RecursionError):
        ClassA()
