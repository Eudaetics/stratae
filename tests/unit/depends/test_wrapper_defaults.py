"""
Unit tests for the defaults of an injected function's wrapper.

This test suite verifies the following behaviors:

inject:
- The wrapper keeps the defaults of every parameter kind it keeps, and can be called without
  them.
- A function decorated with functools.wraps keeps the defaults its signature shows.
"""

import functools
from collections.abc import Callable
from typing import Annotated, Any

from stratae.depends import Depends, inject


def _get_db() -> str:
    return "db"


type _Db = Annotated[str, Depends(_get_db)]


def test_wrapper_keeps_defaults_of_every_kind():
    """
    An injected wrapper should keep the defaults of every parameter kind.

    Given: A function with positional-only, positional-or-keyword and keyword-only defaults,
        *args, **kwargs and an injected parameter
    When: It's injected
    Then: The wrapper's defaults are the function's, and calling it without them uses them
    """

    # Arrange
    def handler(
        a: int = 1, /, b: int = 2, *args: int, c: int = 3, db: _Db, **kwargs: int
    ) -> tuple[Any, ...]:
        return a, b, args, c, db, kwargs

    # Act
    wrapped = inject(handler)

    # Assert
    assert wrapped.__defaults__ == (1, 2)
    assert wrapped.__kwdefaults__ == {"c": 3}
    assert wrapped() == (1, 2, (), 3, "db", {})


def _logged[F: Callable[..., Any]](fn: F) -> F:
    """Decorate `fn` the usual way: a *args, **kwargs wrapper with functools.wraps."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    return wrapper  # pyright: ignore[reportReturnType]


def test_wraps_decorated_function_keeps_defaults():
    """
    A function decorated with functools.wraps should keep the defaults its signature shows.

    Its signature comes from the function it wraps, but its own __defaults__ are the
    decorator's, which has none. Copying those left the wrapper's defaults missing.

    Given: A function with defaults, decorated with a functools.wraps decorator, then injected
    When: The wrapper is called without the defaulted arguments
    Then: The defaults are used
    """

    # Arrange
    @inject
    @_logged
    def handler(name: str = "anon", *, loud: bool = False, db: _Db) -> str:
        return f"{name} {loud} {db}"

    # Act
    result = handler()

    # Assert
    assert result == "anon False db"
    assert handler.__defaults__ == ("anon",)
    assert handler.__kwdefaults__ == {"loud": False}
