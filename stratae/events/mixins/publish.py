"""Pub/sub mixins for synchronous and asynchronous event publishing."""

from abc import ABC, abstractmethod
from typing import Callable

from stratae.events.channel import Channel
from stratae.events.event import AsyncBoundEvent, BoundEvent, EventMeta, EventSchema


class Publisher[Metadata: (EventMeta | None), Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to a synchronous publish emitter.

    Subclasses must implement ``emit_publish``, which receives the channel,
    adapter-specific metadata, and a constructed ``EventSchema`` instance and
    returns ``Resp``.  Routing metadata is passed by callers via the ``meta``
    keyword argument on ``publish``.

    Example::

        class KafkaPublisher(Publisher[KafkaMeta, None]):
            def emit_publish(self, channel, payload, *, meta):
                ...  # forward to Kafka using meta.topic, meta.partition_key, etc.

        emit_order = publisher.publish(channel, OrderPlaced, meta=KafkaMeta("orders"))
        emit_order(order_id=42)
    """

    def publish[**P](
        self, channel: Channel, schema: Callable[P, EventSchema], *, meta: Metadata = None
    ) -> BoundEvent[P, Metadata, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            channel: A Channel over which to publish the event.
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
            meta:   The adapter-specific routing metadata for this binding.

        Returns:
            A ``BoundEvent`` that, when called with ``P`` arguments, constructs
            an instance of ``schema`` and forwards it to ``emit_publish``.

        """
        return BoundEvent(channel, schema, self.emit_publish, meta)

    @abstractmethod
    def emit_publish(self, channel: Channel, payload: EventSchema, *, meta: Metadata) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            channel: A Channel over which to publish the event.
            payload: The constructed ``EventSchema`` instance to dispatch.
            meta:    The adapter-specific routing metadata.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class AsyncPublisher[Metadata: (EventMeta | None), Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to an asynchronous publish emitter.

    Subclasses must implement ``emit_publish`` as a coroutine, which receives the
    channel, adapter-specific metadata, and a constructed ``EventSchema`` instance
    and returns an awaitable resolving to ``Resp``.  Routing metadata is passed by
    callers via the ``meta`` keyword argument on ``publish``.
    """

    def publish[**P](
        self,
        channel: Channel,
        schema: Callable[P, EventSchema],
        *,
        meta: Metadata = None,
    ) -> AsyncBoundEvent[P, Metadata, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            channel: A Channel over which to publish the event.
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
            meta:   The adapter-specific routing metadata for this binding.

        Returns:
            An ``AsyncBoundEvent`` that, when called and awaited with ``P``
            arguments, constructs an instance of ``schema`` and forwards it
            to ``emit_publish``.

        """
        return AsyncBoundEvent(channel, schema, self.emit_publish, meta)

    @abstractmethod
    async def emit_publish(self, channel: Channel, payload: EventSchema, *, meta: Metadata) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            channel: A Channel over which to publish the event.
            payload: The constructed ``EventSchema`` instance to dispatch.
            meta:    The adapter-specific routing metadata.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...
