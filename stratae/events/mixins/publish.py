"""Pub/sub mixins for synchronous and asynchronous event publishing."""

from abc import ABC, abstractmethod
from typing import Callable

from stratae.events.event import AsyncBoundEvent, BoundEvent, EventMeta, EventSchema


class Publisher[Meta: EventMeta, Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to a synchronous publish emitter.

    Subclasses must implement ``emit_publish``, which receives the adapter-specific
    ``Meta`` and a constructed ``EventSchema`` instance and returns ``Resp``.

    Override ``publish`` to accept the routing parameters your adapter requires
    (e.g. topic, partition key), construct a ``Meta`` instance, and call the
    base implementation, which wires everything into a ``BoundEvent``.

    Example::

        class KafkaPublisher(Publisher[KafkaMeta, None]):
            def publish[**P](
                self,
                schema: Callable[P, EventSchema],
                topic: str,
                partition_key: str | None = None,
            ) -> BoundEvent[P, KafkaMeta, None]:
                return super().publish(schema, KafkaMeta(topic, partition_key))
    """

    def publish[**P](
        self,
        schema: Callable[P, EventSchema],
        meta: Meta,
    ) -> BoundEvent[P, Meta, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
            meta:   The adapter-specific routing metadata for this binding.

        Returns:
            A ``BoundEvent`` that, when called with ``P`` arguments, constructs
            an instance of ``schema`` and forwards it to ``emit_publish``.

        """
        return BoundEvent(schema, self.emit_publish, meta)

    @abstractmethod
    def emit_publish(self, meta: Meta, event: EventSchema) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            meta:  The adapter-specific routing metadata.
            event: The constructed ``EventSchema`` instance to dispatch.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class AsyncPublisher[Meta: EventMeta, Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to an asynchronous publish emitter.

    Subclasses must implement ``emit_publish`` as a coroutine, which receives the
    adapter-specific ``Meta`` and a constructed ``EventSchema`` instance and returns
    an awaitable resolving to ``Resp``.

    Override ``publish`` to accept the routing parameters your adapter requires,
    construct a ``Meta`` instance, and call the base implementation.
    """

    def publish[**P](
        self,
        schema: Callable[P, EventSchema],
        meta: Meta,
    ) -> AsyncBoundEvent[P, Meta, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
            meta:   The adapter-specific routing metadata for this binding.

        Returns:
            An ``AsyncBoundEvent`` that, when called and awaited with ``P``
            arguments, constructs an instance of ``schema`` and forwards it
            to ``emit_publish``.

        """
        return AsyncBoundEvent(schema, self.emit_publish, meta)

    @abstractmethod
    async def emit_publish(self, meta: Meta, event: EventSchema) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            meta:  The adapter-specific routing metadata.
            event: The constructed ``EventSchema`` instance to dispatch.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...
