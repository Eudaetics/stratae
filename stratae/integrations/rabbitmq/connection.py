"""Where a RabbitMQ publisher's or consumer's connection is."""

from __future__ import annotations

from enum import StrEnum


class ConnectionState(StrEnum):
    """
    Where a RabbitMQ publisher's or consumer's connection is.

    A publisher reports it as
    {py:attr}`state <stratae.integrations.rabbitmq.publisher.RabbitMQPublisher.state>`, and a
    consumer as
    {py:attr}`state <stratae.integrations.rabbitmq.consumer.RabbitMQConsumer.state>`.
    """

    CONNECTED = "connected"
    """Connected, with an open channel."""

    RECONNECTING = "reconnecting"
    """The connection dropped unexpectedly, and is being reopened."""

    CLOSED = "closed"
    """Not connected: never entered, exited, or dropped with no reconnecting left to try."""
