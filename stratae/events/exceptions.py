"""
Exceptions for errors in event dispatch, all subclassing {py:exc}`EventDispatchError`.

Handlers' failures are raised together as a {py:exc}`HandlerErrors` group, or a
{py:exc}`BaseHandlerErrors` one if any is a `BaseException` such as `CancelledError`.
"""

from collections.abc import Sequence
from typing import Self


class EventDispatchError(Exception):
    """Base class for all event dispatch exceptions."""


class NoResponderError(EventDispatchError, LookupError):
    """Raised when a request event has no responder."""

    def __init__(self, event: str) -> None:
        """:param event: The request event's name."""
        super().__init__(f"no responder registered for request event '{event}'")


class MultipleRespondersError(EventDispatchError, LookupError):
    """Raised when a request event has more than one responder."""

    def __init__(self, event: str, count: int) -> None:
        """
        Name the event and how many responders it has.

        :param event: The request event's name.
        :param count: How many responders it has.
        """
        super().__init__(f"request event '{event}' has {count} responders; exactly one is required")


class BaseHandlerErrors(BaseExceptionGroup[BaseException]):
    """Raised with the failures of an event's handlers, when any is a `BaseException`."""

    def __new__(cls, errors: Sequence[BaseException]) -> Self:
        """:param errors: Each failed handler's error."""
        return super().__new__(cls, "handler errors", errors)

    def __init__(self, errors: Sequence[BaseException]) -> None:
        """:param errors: Each failed handler's error."""
        super().__init__("handler errors", errors)


class HandlerErrors(BaseHandlerErrors, ExceptionGroup[Exception]):
    """Raised with the failures of an event's handlers."""


class NotConnectedError(EventDispatchError):
    """Raised when using an adapter whose connection isn't open."""


class AlreadyConnectedError(EventDispatchError):
    """Raised when opening an adapter that's already connected."""


class UnroutableError(EventDispatchError):
    """Raised when a message that must be routed reaches no queue or handler."""
