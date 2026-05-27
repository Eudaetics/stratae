"""Event handler wrapper carrying async detection and equality semantics."""

from inspect import iscoroutinefunction
from typing import Callable

from stratae.events.event import Event


class Handler[R]:
    """
    Wraps an event handler callable with async detection and value equality.

    ``R`` is the return type of the wrapped callable.  For async handlers
    ``R`` will be an ``Awaitable`` of the resolved type.

    Stores the callable and detects at construction time whether it is a
    coroutine function, so dispatchers can branch once on ``is_async`` rather
    than inspecting the callable on every dispatch.

    Two ``Handler`` instances wrapping the same callable are considered equal
    and hash identically, preventing duplicate registrations in a set.
    """

    def __init__(self, call: Callable[[Event], R]) -> None:
        """
        Wrap a callable as an event handler.

        Args:
            call: The sync or async callable to wrap.  Must accept a
                  single ``Event`` instance as its argument.

        """
        self.call: Callable[[Event], R] = call
        self.is_async: bool = iscoroutinefunction(call)

    def __eq__(self, other: object) -> bool:
        """
        Return True if both handlers wrap the same callable.

        Args:
            other: The object to compare against.

        Returns:
            ``True`` if ``other`` is a ``Handler`` wrapping the same callable,
            ``NotImplemented`` if ``other`` is not a ``Handler``.

        """
        if not isinstance(other, Handler):
            return NotImplemented
        return self.call == other.call

    def __hash__(self) -> int:
        """
        Return a hash derived from the wrapped callable.

        Returns:
            Hash of the wrapped callable, consistent with ``__eq__``.

        """
        return hash(self.call)
