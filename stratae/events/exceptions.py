"""
Exceptions for errors in event dispatch.

All inherit from {py:exc}`EventDispatchError`. {py:exc}`NoResponderError` and
{py:exc}`MultipleRespondersError` are both raised when emitting a
{py:class}`Request <stratae.events.event.Request>` event whose registered
responder count isn't exactly one, too few and too many respectively.
{py:exc}`NotConnectedError` is raised separately, when emitting through an
adapter whose underlying connection is not open, and
{py:exc}`AlreadyConnectedError` when opening an adapter that's already open.
"""


class EventDispatchError(Exception):
    """Base class for all event dispatch related exceptions."""


class NoResponderError(EventDispatchError, LookupError):
    """
    Exception raised when a request event has no registered responder.

    Raised at emit time for a {py:class}`Request <stratae.events.event.Request>`
    event with no responder registered, as distinct from
    {py:exc}`MultipleRespondersError`, which covers more than one
    registered responder.
    """


class MultipleRespondersError(EventDispatchError, LookupError):
    """
    Exception raised when a request event has more than one registered responder.

    Raised at emit time for a {py:class}`Request <stratae.events.event.Request>`
    event, since exactly one responder is required to resolve its reply, as
    distinct from {py:exc}`NoResponderError`, which covers no responder
    being registered at all.
    """


class NotConnectedError(EventDispatchError):
    """
    Exception raised when emitting through an adapter whose connection is not open.

    Raised by event bus adapters, such as
    {py:class}`RabbitMQPublisher <stratae.integrations.rabbitmq.RabbitMQPublisher>`,
    when asked to publish before their underlying connection has been opened.
    """


class AlreadyConnectedError(EventDispatchError):
    """
    Exception raised when opening an adapter whose connection is already open.

    Raised by event bus adapters, such as
    {py:class}`RabbitMQPublisher <stratae.integrations.rabbitmq.RabbitMQPublisher>`,
    when entered again while still connected, which would otherwise orphan the open
    connection.
    """
