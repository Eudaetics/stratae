"""Pub/sub mixins for synchronous and asynchronous event publishing."""

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from stratae.events.event import AsyncBoundEvent, BoundEvent, EventSchema


class Publisher[EventConfig: Any, Resp](ABC):
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
        self, schema: Callable[P, EventSchema], *, config: Any = None
    ) -> BoundEvent[P, EventConfig, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            channel: A Channel over which to publish the event.
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
            config:   The adapter-specific configuration for this binding.

        Returns:
            A ``BoundEvent`` that, when called with ``P`` arguments, constructs
            an instance of ``schema`` and forwards it to ``emit_publish``.

        """
        return BoundEvent(schema, self.emit_publish, config=config)

    @abstractmethod
    def emit_publish[**P](
        self, payload: EventSchema, event: BoundEvent[P, EventConfig, Resp]
    ) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            event:   The bound event being emitted.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class AsyncPublisher[EventConfig: Any, Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to an asynchronous publish emitter.

    Subclasses must implement ``emit_publish`` as a coroutine, which receives the
    channel, adapter-specific metadata, and a constructed ``EventSchema`` instance
    and returns an awaitable resolving to ``Resp``.  Routing metadata is passed by
    callers via the ``meta`` keyword argument on ``publish``.
    """

    def publish[**P](
        self,
        schema: Callable[P, EventSchema],
        *,
        config: EventConfig = None,
    ) -> AsyncBoundEvent[P, EventConfig, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            channel: A Channel over which to publish the event.
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
            config:   The adapter-specific configuration for this binding.

        Returns:
            An ``AsyncBoundEvent`` that, when called and awaited with ``P``
            arguments, constructs an instance of ``schema`` and forwards it
            to ``emit_publish``.

        """
        return AsyncBoundEvent(schema, self.emit_publish, config=config)

    @abstractmethod
    async def emit_publish[**P](
        self, payload: EventSchema, event: BoundEvent[P, EventConfig, Awaitable[Resp]]
    ) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            event:   The bound event being emitted.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...
