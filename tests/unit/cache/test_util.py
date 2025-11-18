"""Test cases for cache utility functions."""

from typing import Callable

import pytest

from stratae.cache import get_function_key


def test_get_function_key_is_hashable():
    """
    Test that get_function_key returns a hashable key for a given function.

    Given: A simple test function.
    When: get_function_key is called on it.
    Then: The returned key should be hashable.
    """

    def sample_function(): ...

    key = get_function_key(sample_function)
    try:
        hash(key)
    except TypeError:
        pytest.fail("get_function_key did not return a hashable key.")


def test_get_function_key_is_consistent():
    """
    Test that get_function_key returns the same key for multiple calls on the same function.

    Given: A simple test function.
    When: get_function_key is called multiple times on it.
    Then: The returned keys should be identical.
    """

    def sample_function(): ...

    key1 = get_function_key(sample_function)
    key2 = get_function_key(sample_function)

    assert key1 == key2, "get_function_key returned different keys for the same function."


def test_dynamically_created_functions_have_unique_keys():
    """
    Test that dynamically created functions with the same qualname get unique keys.

    Given: Two functions created dynamically with identical module and qualname.
    When: get_function_key is called on each.
    Then: The keys should be different (preventing cache collisions).
    """

    def make_function():
        def inner(): ...

        return inner

    func1 = make_function()
    func2 = make_function()

    key1 = get_function_key(func1)
    key2 = get_function_key(func2)

    assert func1.__module__ == func2.__module__
    assert func1.__qualname__ == func2.__qualname__
    assert key1 != key2


def test_wrapped_functions_have_different_keys():
    """
    Test that wrapped functions have different keys than their unwrapped counterparts.

    Given: A function wrapped by a decorator.
    When: get_function_key is called on both the original and wrapped functions.
    Then: The keys should be different.
    """

    def decorator[**P, T](func: Callable[P, T]) -> Callable[P, T]:
        def wrapper(*args: P.args, **kwargs: P.kwargs):
            return func(*args, **kwargs)

        return wrapper

    def original_function() -> int:
        return 0

    wrapped_function = decorator(original_function)

    key_original = get_function_key(original_function)
    key_wrapped = get_function_key(wrapped_function)

    assert key_original != key_wrapped
