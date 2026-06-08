"""Pub/sub mixins for synchronous and asynchronous event publishing."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, overload

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

        @publisher.publish(config=KafkaMeta("orders"))
        class order_placed(EventSchema):
            def __init__(self, order_id: int) -> None: ...
    """

    @overload
    def publish[**P](
        self, schema: Callable[P, EventSchema], *, config: EventConfig
    ) -> BoundEvent[P, EventConfig, Resp]: ...

    @overload
    def publish[**P](
        self, *, config: EventConfig
    ) -> Callable[[Callable[P, EventSchema]], BoundEvent[P, EventConfig, Resp]]: ...

    def publish[**P](
        self, schema: Callable[P, EventSchema] | None = None, *, config: EventConfig
    ) -> (
        BoundEvent[P, EventConfig, Resp]
        | Callable[[Callable[P, EventSchema]], BoundEvent[P, EventConfig, Resp]]
    ):
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Can be called directly or used as a decorator factory::

            emit_order = publisher.publish(OrderPlaced, config=KafkaMeta("orders"))

            @publisher.publish(config=KafkaMeta("orders"))
            class order_placed(EventSchema): ...

        Args:
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
                    Omit to use as a decorator factory.
            config: The adapter-specific routing config for this binding.

        Returns:
            A ``BoundEvent`` when ``schema`` is provided, otherwise a decorator
            that accepts a schema and returns a ``BoundEvent``.

        """
        if schema is None:

            def decorator(s: Callable[P, EventSchema]) -> BoundEvent[P, EventConfig, Resp]:
                return BoundEvent(s, self.emit_publish, config=config)

            return decorator
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
        self, payload: EventSchema, event: AsyncBoundEvent[P, None, Resp]
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

    Example::

        @publisher.publish(config=RabbitMQConfig("", "order.placed"))
        class order_placed(EventSchema):
            def __init__(self, order_id: int) -> None: ...
    """

    @overload
    def publish[**P](
        self,
        schema: Callable[P, EventSchema],
        *,
        config: EventConfig,
    ) -> AsyncBoundEvent[P, EventConfig, Resp]: ...

    @overload
    def publish[**P](
        self,
        *,
        config: EventConfig,
    ) -> Callable[[Callable[P, EventSchema]], AsyncBoundEvent[P, EventConfig, Resp]]: ...

    def publish[**P](
        self,
        schema: Callable[P, EventSchema] | None = None,
        *,
        config: EventConfig,
    ) -> (
        AsyncBoundEvent[P, EventConfig, Resp]
        | Callable[[Callable[P, EventSchema]], AsyncBoundEvent[P, EventConfig, Resp]]
    ):
        """
        Bind an ``EventSchema`` subclass to this publisher's ``emit_publish``.

        Can be called directly or used as a decorator factory::

            emit_order = publisher.publish(OrderPlaced, config=RabbitMQConfig("", "orders"))

            @publisher.publish(config=RabbitMQConfig("", "orders"))
            class order_placed(EventSchema): ...

        Args:
            schema: An ``EventSchema`` subclass whose constructor accepts ``P``.
                    Omit to use as a decorator factory.
            config: The adapter-specific routing config for this binding.

        Returns:
            An ``AsyncBoundEvent`` when ``schema`` is provided, otherwise a decorator
            that accepts a schema and returns an ``AsyncBoundEvent``.

        """
        if schema is None:

            def decorator(s: Callable[P, EventSchema]) -> AsyncBoundEvent[P, EventConfig, Resp]:
                return AsyncBoundEvent(s, self.emit_publish, config=config)

            return decorator
        return AsyncBoundEvent(schema, self.emit_publish, config=config)

    @abstractmethod
    async def emit_publish[**P](
        self, payload: EventSchema, event: AsyncBoundEvent[P, EventConfig, Resp]
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
