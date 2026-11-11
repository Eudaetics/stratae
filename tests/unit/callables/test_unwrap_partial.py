"""
Tests for unwrap_partial: the callable behind any functools.partial objects.

This test suite verifies the following behaviors:

- A callable that isn't a partial is returned as it is.
- A partial gives the callable it calls.
- Nested partials give the innermost callable.
- A partial of a class gives the class.
"""

from functools import partial

from stratae.callables import unwrap_partial


def _greet(greeting: str, name: str) -> str:
    return f"{greeting}, {name}"


class _Greeter:
    def __init__(self, greeting: str) -> None:
        self.greeting = greeting


def test_callable_that_is_not_a_partial_is_returned_as_is() -> None:
    """
    A plain function should be returned unchanged.

    Given: A function
    When: It's unwrapped
    Then: The same function is returned
    """
    # Act / Assert
    assert unwrap_partial(_greet) is _greet


def test_partial_gives_its_callable() -> None:
    """
    A partial should give the callable it calls.

    Given: A partial of a function
    When: It's unwrapped
    Then: The function is returned
    """
    # Act / Assert
    assert unwrap_partial(partial(_greet, "hello")) is _greet


def test_nested_partials_give_the_innermost_callable() -> None:
    """
    Nested partials should give the callable at their center.

    Given: A partial of a partial of a function
    When: It's unwrapped
    Then: The function is returned
    """
    # Act / Assert
    assert unwrap_partial(partial(partial(_greet, "hello"), "world")) is _greet


def test_partial_of_a_class_gives_the_class() -> None:
    """
    A partial of a class should give the class.

    Given: A partial of a class
    When: It's unwrapped
    Then: The class is returned
    """
    # Act / Assert
    assert unwrap_partial(partial(_Greeter, "hello")) is _Greeter
