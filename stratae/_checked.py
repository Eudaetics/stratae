"""The check on every tuple of callables Stratae is given, such as hooks and middleware."""

from collections.abc import Iterable


class NotATupleError(TypeError):
    """Raised when a single callable or a string is given in place of a tuple of callables."""

    def __init__(self, given_as: str) -> None:
        """:param given_as: Where it was given, such as `"Hooks before="`."""
        super().__init__(
            f"{given_as} takes a tuple of callables, not a single callable or a string"
        )


class NotCallableError(TypeError):
    """Raised when a tuple of callables holds something that isn't callable."""

    def __init__(self, given_as: str, item: object) -> None:
        """
        Name where the callables were given, and the item.

        :param given_as: Where it was given, such as `"Hooks before="`.
        :param item: The item that isn't callable.
        """
        super().__init__(f"{given_as} holds {item!r}, which isn't callable")


def checked_callables[T](items: Iterable[T], given_as: str) -> tuple[T, ...]:
    """
    Return the callables given as a tuple, refusing a single callable, a string, or a non-callable.

    :param items: The callables as given, in any iterable.
    :param given_as: Where they were given, such as `"Hooks before="`, for the error message.
    :raises TypeError: If it's a single callable or a string, or an item isn't callable.
    """
    # A string would otherwise be split into its characters.
    if callable(items) or isinstance(items, str):
        raise NotATupleError(given_as)
    checked = tuple(items)
    for item in checked:
        if not callable(item):
            raise NotCallableError(given_as, item)
    return checked
