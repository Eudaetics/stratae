"""Exceptions for errors in event dispatch, all subclassing {py:exc}`EventDispatchError`."""


class EventDispatchError(Exception):
    """Base class for all event dispatch exceptions."""


class NoResponderError(EventDispatchError, LookupError):
    """Raised when a request event has no responder."""


class MultipleRespondersError(EventDispatchError, LookupError):
    """Raised when a request event has more than one responder."""


class NotConnectedError(EventDispatchError):
    """Raised when using an adapter whose connection isn't open."""


class AlreadyConnectedError(EventDispatchError):
    """Raised when opening an adapter that's already connected."""
