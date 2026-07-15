"""Exceptions for errors in event dispatch."""


class EventDispatchError(Exception):
    """Base class for all event dispatch related exceptions."""


class NoResponderError(EventDispatchError, LookupError):
    """Raised when a request event is emitted with no registered responder."""


class MultipleRespondersError(EventDispatchError, LookupError):
    """Raised when a request event is emitted with more than one registered responder."""
