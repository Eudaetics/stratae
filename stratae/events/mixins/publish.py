"""Pub/sub mixins for synchronous and asynchronous event publishing."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, overload

from stratae.events.bound import AsyncBoundEvent, BoundEvent
from stratae.events.event import EventConfig, EventType, Payload


class BasicPublisher[Resp](ABC):
    """
    Mixin that binds an ``EventConfig`` to a synchronous emitter with no routing config.

    Subclasses must implement ``emit_publish``, which receives the constructed event payload
    and the bound event, and returns ``Resp``.

    Example::

        class InMemoryBus(BasicPublisher[None]):
            def emit_publish(self, payload, event):
                ...  # dispatch to registered handlers

        emit_order = bus.publish(order_placed)
        emit_order(order_id=42)
    """

    def publish[**P, S: Payload, T: EventType](
        self, event: EventConfig[P, S, T]
    ) -> BoundEvent[P, S, T, None, Resp]:
        """
        Bind an ``EventConfig`` to this publisher's ``emit_publish``.

        Args:
            event: The ``EventConfig`` to bind.

        Returns:
            A ``BoundEvent`` that, when called with ``P`` arguments, constructs
            a payload and forwards it to ``emit_publish``.

        """
        return BoundEvent(self.emit_publish, event, config=None)

    @abstractmethod
    def emit_publish[**P, S: Payload, T: EventType](
        self, payload: Payload, event: BoundEvent[P, S, T, None, Resp]
    ) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            payload: The constructed ``Payload`` instance to dispatch.
            event:   The bound event being emitted.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class Publisher[RoutingConfig: Any, Resp](ABC):
    """
    Mixin that binds an ``EventConfig`` to a synchronous emitter with routing config.

    Subclasses must implement ``emit_publish``, which receives the constructed event payload
    and the bound event (carrying the config), and returns ``Resp``.  Routing config is
    passed by callers via the ``config`` keyword argument on ``publish``.

    Example::

        class KafkaPublisher(Publisher[KafkaMeta, None]):
            def emit_publish(self, payload, event):
                ...  # forward to Kafka using event.config.topic, etc.

        emit_order = publisher.publish(order_placed, config=KafkaMeta("orders"))
        emit_order(order_id=42)

        @publisher.publish(config=KafkaMeta("orders"))
        def order_placed(order_id: int) -> OrderPlaced: ...
    """

    @overload
    def publish[**P, S: Payload, T: EventType](
        self, event: EventConfig[P, S, T], *, config: RoutingConfig
    ) -> BoundEvent[P, S, T, RoutingConfig, Resp]: ...

    @overload
    def publish[**P, S: Payload, T: EventType](
        self, *, config: RoutingConfig
    ) -> Callable[[EventConfig[P, S, T]], BoundEvent[P, S, T, RoutingConfig, Resp]]: ...

    def publish[**P, S: Payload, T: EventType](
        self, event: EventConfig[P, S, T] | None = None, *, config: RoutingConfig
    ) -> (
        BoundEvent[P, S, T, RoutingConfig, Resp]
        | Callable[[EventConfig[P, S, T]], BoundEvent[P, S, T, RoutingConfig, Resp]]
    ):
        """
        Bind an ``EventConfig`` to this publisher's ``emit_publish``.

        Can be called directly or used as a decorator factory::

            emit_order = publisher.publish(order_placed, config=KafkaMeta("orders"))

            @publisher.publish(config=KafkaMeta("orders"))
            def order_placed(order_id: int) -> OrderPlaced: ...

        Args:
            event:  The ``EventConfig`` to bind. Omit to use as a decorator factory.
            config: The adapter-specific routing config for this binding.

        Returns:
            A ``BoundEvent`` when ``event`` is provided, otherwise a decorator
            that accepts an ``EventConfig`` and returns a ``BoundEvent``.

        """
        if event is None:

            def decorator(
                evt: EventConfig[P, S, T],
            ) -> BoundEvent[P, S, T, RoutingConfig, Resp]:
                return BoundEvent(self.emit_publish, evt, config=config)

            return decorator
        return BoundEvent(self.emit_publish, event, config=config)

    @abstractmethod
    def emit_publish[**P, S: Payload, T: EventType](
        self, payload: Payload, event: BoundEvent[P, S, T, RoutingConfig, Resp]
    ) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            payload: The constructed ``Payload`` instance to dispatch.
            event:   The bound event being emitted.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class AsyncBasicPublisher[Resp](ABC):
    """
    Mixin that binds ``Payload`` subclasses to an asynchronous emitter with no routing config.

    Subclasses must implement ``emit_publish`` as a coroutine, which receives the constructed
    event payload and the bound event, and returns an awaitable resolving to ``Resp``.

    Example::

        class AsyncInMemoryBus(AsyncBasicPublisher[None]):
            async def emit_publish(self, payload, event):
                ...  # dispatch to registered handlers

        emit_order = bus.publish(OrderPlaced)
        await emit_order(order_id=42)
    """

    def publish[**P](self, schema: Callable[P, Payload]) -> AsyncBoundEvent[P, None, Resp]:
        """
        Bind a ``Payload`` subclass to this publisher's ``emit_publish``.

        Args:
            schema: A ``Payload`` subclass whose constructor accepts ``P``.

        Returns:
            An ``AsyncBoundEvent`` that, when called and awaited with ``P``
            arguments, constructs an instance of ``schema`` and forwards it
            to ``emit_publish``.

        """
        return AsyncBoundEvent(self.emit_publish, schema, config=None)

    @abstractmethod
    async def emit_publish[**P](
        self, payload: Payload, event: AsyncBoundEvent[P, None, Resp]
    ) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            payload: The constructed ``Payload`` instance to dispatch.
            event:   The bound event being emitted.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class AsyncPublisher[RoutingConfig: Any, Resp](ABC):
    """
    Mixin that binds ``Payload`` subclasses to an asynchronous emitter with routing config.

    Subclasses must implement ``emit_publish`` as a coroutine, which receives the constructed
    event payload and the bound event (carrying the config), and returns an awaitable resolving
    to ``Resp``.  Routing config is passed by callers via the ``config`` keyword argument on
    ``publish``.

    Example::

        @publisher.publish(config=RabbitMQConfig("", "order.placed"))
        class order_placed(Payload):
            def __init__(self, order_id: int) -> None: ...
    """

    @overload
    def publish[**P](
        self,
        schema: Callable[P, Payload],
        *,
        config: RoutingConfig,
    ) -> AsyncBoundEvent[P, RoutingConfig, Resp]: ...

    @overload
    def publish[**P](
        self,
        *,
        config: RoutingConfig,
    ) -> Callable[[Callable[P, Payload]], AsyncBoundEvent[P, RoutingConfig, Resp]]: ...

    def publish[**P](
        self,
        schema: Callable[P, Payload] | None = None,
        *,
        config: RoutingConfig,
    ) -> (
        AsyncBoundEvent[P, RoutingConfig, Resp]
        | Callable[[Callable[P, Payload]], AsyncBoundEvent[P, RoutingConfig, Resp]]
    ):
        """
        Bind a ``Payload`` subclass to this publisher's ``emit_publish``.

        Can be called directly or used as a decorator factory::

            emit_order = publisher.publish(OrderPlaced, config=RabbitMQConfig("", "orders"))

            @publisher.publish(config=RabbitMQConfig("", "orders"))
            class order_placed(Payload): ...

        Args:
            schema: A ``Payload`` subclass whose constructor accepts ``P``.
                    Omit to use as a decorator factory.
            config: The adapter-specific routing config for this binding.

        Returns:
            An ``AsyncBoundEvent`` when ``schema`` is provided, otherwise a decorator
            that accepts a schema and returns an ``AsyncBoundEvent``.

        """
        if schema is None:

            def decorator(s: Callable[P, Payload]) -> AsyncBoundEvent[P, RoutingConfig, Resp]:
                return AsyncBoundEvent(self.emit_publish, s, config=config)

            return decorator
        return AsyncBoundEvent(self.emit_publish, schema, config=config)

    @abstractmethod
    async def emit_publish[**P](
        self, payload: Payload, event: AsyncBoundEvent[P, RoutingConfig, Resp]
    ) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            payload: The constructed ``Payload`` instance to dispatch.
            event:   The bound event being emitted.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...
