"""Event handler wrapper carrying async detection and equality semantics."""

from inspect import iscoroutinefunction
from typing import Callable

from stratae.events.event import EventSchema


class Handler[R]:
    """
    Wraps an event handler callable with async detection and value equality.

    ``R`` is the return type of the wrapped callable.  For async handlers
    ``R`` will be an ``Awaitable`` of the resolved type.

    Stores the callable and detects at construction time whether it is a
    coroutine function, so dispatchers can branch once on ``is_async`` rather
    than inspecting the callable on every dispatch.

    Calling a ``Handler`` delegates directly to the wrapped callable.  For
    async handlers, the result is a coroutine that the caller can ``await``;
    a sync wrapper around an async callable naturally returns the coroutine
    object, so ``await handler(event)`` works without ``Handler.__call__``
    itself being declared ``async``.

    A ``Handler`` compares equal to another ``Handler`` wrapping the same
    callable, or to the raw callable itself, preventing duplicate registrations
    and allowing unsubscription by passing the original callable.
    """

    def __init__(self, call: Callable[[EventSchema], R]) -> None:
        """
        Wrap a callable as an event handler.

        Args:
            call: The sync or async callable to wrap.  Must accept a
                  single ``Event`` instance as its argument.

        """
        self.call: Callable[[EventSchema], R] = call
        self.is_async: bool = iscoroutinefunction(call)

    def __call__(self, event: EventSchema) -> R:
        """
        Invoke the wrapped callable with the given event.

        Args:
            event: The event instance to pass to the handler.

        Returns:
            For sync handlers, the return value directly.  For async handlers,
            a coroutine that the caller should ``await``.

        """
        return self.call(event)

    def __eq__(self, other: object) -> bool:
        """
        Return True if both handlers wrap the same callable.

        Args:
            other: The object to compare against.  May be a ``Handler`` or a
                   raw callable.

        Returns:
            ``True`` if ``other`` is a ``Handler`` wrapping the same callable,
            or the same callable itself.  ``NotImplemented`` otherwise.

        """
        if isinstance(other, Handler):
            return self.call == other.call
        if callable(other):
            return self.call == other
        return NotImplemented

    def __hash__(self) -> int:
        """
        Return a hash derived from the wrapped callable.

        Returns:
            Hash of the wrapped callable, consistent with ``__eq__``.

        """
        return hash(self.call)
