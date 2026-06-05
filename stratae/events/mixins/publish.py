"""Pub/sub mixins for synchronous and asynchronous event publishing."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from stratae.events.event import AsyncBoundEvent, BoundEvent, EventSchema


class BasicPublisher[Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to a synchronous emitter with no routing config.

    Subclasses must implement ``emit_publish``, which receives the constructed event payload
    and the bound event, and returns ``Resp``.

    Example::

        class InMemoryBus(BasicPublisher[None]):
            def emit_publish(self, payload, event):
                ...  # dispatch to registered handlers

        emit_order = bus.publish(OrderPlaced)
        emit_order(order_id=42)
    """

    def publish[**P](self, schema: Callable[P, EventSchema]) -> BoundEvent[P, None, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.

        Returns:
            A ``BoundEvent`` that, when called with ``P`` arguments, constructs
            an instance of ``schema`` and forwards it to ``emit_publish``.

        """
        return BoundEvent(schema, self.emit_publish, config=None)

    @abstractmethod
    def emit_publish[**P](self, payload: EventSchema, event: BoundEvent[P, None, Resp]) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            event:   The bound event being emitted.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class Publisher[EventConfig: Any, Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to a synchronous emitter with routing config.

    Subclasses must implement ``emit_publish``, which receives the constructed event payload
    and the bound event (carrying the config), and returns ``Resp``.  Routing config is
    passed by callers via the ``config`` keyword argument on ``publish``.

    Example::

        class KafkaPublisher(Publisher[KafkaMeta, None]):
            def emit_publish(self, payload, event):
                ...  # forward to Kafka using event.config.topic, etc.

        emit_order = publisher.publish(OrderPlaced, config=KafkaMeta("orders"))
        emit_order(order_id=42)
    """

    def publish[**P](
        self, schema: Callable[P, EventSchema], *, config: EventConfig
    ) -> BoundEvent[P, EventConfig, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
            config: The adapter-specific routing config for this binding.

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


class AsyncBasicPublisher[Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to an asynchronous emitter with no routing config.

    Subclasses must implement ``emit_publish`` as a coroutine, which receives the constructed
    event payload and the bound event, and returns an awaitable resolving to ``Resp``.

    Example::

        class AsyncInMemoryBus(AsyncBasicPublisher[None]):
            async def emit_publish(self, payload, event):
                ...  # dispatch to registered handlers

        emit_order = bus.publish(OrderPlaced)
        await emit_order(order_id=42)
    """

    def publish[**P](self, schema: Callable[P, EventSchema]) -> AsyncBoundEvent[P, None, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.

        Returns:
            An ``AsyncBoundEvent`` that, when called and awaited with ``P``
            arguments, constructs an instance of ``schema`` and forwards it
            to ``emit_publish``.

        """
        return AsyncBoundEvent(schema, self.emit_publish, config=None)

    @abstractmethod
    async def emit_publish[**P](
        self, payload: EventSchema, event: BoundEvent[P, None, Awaitable[Resp]]
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
    Mixin that binds ``EventSchema`` subclasses to an asynchronous emitter with routing config.

    Subclasses must implement ``emit_publish`` as a coroutine, which receives the constructed
    event payload and the bound event (carrying the config), and returns an awaitable resolving
    to ``Resp``.  Routing config is passed by callers via the ``config`` keyword argument on
    ``publish``.
    """

    def publish[**P](
        self,
        schema: Callable[P, EventSchema],
        *,
        config: EventConfig,
    ) -> AsyncBoundEvent[P, EventConfig, Resp]:
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Args:
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
            config: The adapter-specific routing config for this binding.

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
