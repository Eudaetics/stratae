"""Test suite for verifying dependency injection in class methods and constructors."""

import asyncio
from typing import Self

from stratae.depends import Depends, Injected, inject


def get_dep():
    """Dependent function returning an integer."""
    return 42


async def test_class_init_async_dep():
    """
    Test the creation of a class instance with an async dependency.

    Note that the current implementation of 'inject' does not allow async dependencies in sync
    methods. This means that a class that requires async dependencies in its __init__ method must
    have an async class method that is injected with the async dependency instead.

    Given: A class with an async class method that is injected with an async dependency.
    When: The class method is called to create an instance.
    Then: The dependency is correctly injected into the instance.
    """

    # Arrange
    async def get_async_dep():
        await asyncio.sleep(0)
        return 99

    class SimpleObject:
        def __init__(self, value: int):
            self.value = value

        @classmethod
        @inject
        async def create(cls, value: Injected[int, Depends(get_async_dep)]) -> Self:
            return cls(value)

    # Act
    instance = await SimpleObject.create()

    # Assert
    assert instance.value == 99


async def test_nested_async_class_creation():
    """
    Test the creation of a class instance with a nested async dependency.

    Note that the current implementation of 'inject' does not allow async dependencies in sync
    methods. This means that a class that requires async dependencies in its __init__ method must
    have an async class method that is injected with the async dependency instead. For nested
    dependencies, each class in the chain must follow this pattern. However, due to limitations
    with class methods and binding, we need to use an intermediate async function to properly call
    the class method.

    Given: A class with an async class method that is injected with another class that itself
           requires an async dependency.
    When: The outer class method is called to create an instance.
    Then: The nested dependency is correctly injected into the inner class, and the outer class
          receives the correctly constructed inner class instance.
    """

    # Arrange
    async def get_async_dep():
        """Async dependency function returning an integer."""
        await asyncio.sleep(0)
        return 100

    class DepClass:
        """Dependency class with async creation method."""

        def __init__(self, value: int):
            self.value = value

        @classmethod
        @inject
        async def create(cls, value: Injected[int, Depends(get_async_dep)]) -> Self:
            return cls(value)

    async def get_dep_class():
        """Async function to get DepClass instance."""
        return await DepClass.create()

    class SimpleObject:
        """Test class that depends on DepClass."""

        def __init__(self, dep: DepClass):
            self.dep = dep

        @classmethod
        @inject
        async def create(cls, dep: Injected[DepClass, Depends(get_dep_class)]) -> Self:
            return cls(dep)

    # Act
    instance = await SimpleObject.create()
    # Assert
    assert instance.dep.value == 100


async def test_method_injection_async():
    """
    Test injection in an async method of a class.

    Given: A class with an async method that is injected with a dependency.
    When: The async method is called on an instance of the class.
    Then: The dependency is correctly injected into the async method.
    """

    # Arrange
    async def get_async_dep():
        await asyncio.sleep(0)
        return 99

    class SimpleObject:
        @inject
        async def set_value(self, value: Injected[int, Depends(get_async_dep)]):
            self.value = value

    # Act
    instance = SimpleObject()
    await instance.set_value()

    # Assert
    assert instance.value == 99


async def test_static_method_injection_async():
    """
    Test injection in a static async method of a class.

    Given: A class with a static async method that is injected with a dependency.
    When: The static async method is called.
    Then: The dependency is correctly injected into the static async method.
    """

    # Arrange
    async def get_async_dep():
        await asyncio.sleep(0)
        return 88

    class SimpleObject:
        value = 0

        @staticmethod
        @inject
        async def set_value(value: Injected[int, Depends(get_async_dep)]):
            SimpleObject.value = value

    # Act
    await SimpleObject.set_value()

    # Assert
    assert SimpleObject.value == 88


async def test_class_method_injection_async():
    """
    Test injection in a class async method of a class.

    Given: A class with a class async method that is injected with a dependency.
    When: The class async method is called.
    Then: The dependency is correctly injected into the class async method.
    """

    # Arrange
    async def get_async_dep():
        await asyncio.sleep(0)
        return 77

    class SimpleObject:
        value = 0

        @classmethod
        @inject
        async def set_value(cls, value: Injected[int, Depends(get_async_dep)]):
            cls.value = value

    # Act
    await SimpleObject.set_value()

    # Assert
    assert SimpleObject.value == 77
