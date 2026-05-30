"""Pub/sub mixins for synchronous and asynchronous event subscription."""

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Callable, overload

from stratae.events.channel import Channel
from stratae.events.event import EventMeta, EventSchema
from stratae.events.handler import Handler


class SubscriberBase[Meta: (EventMeta | None)]:
    """
    Base mixin providing shared handler storage for subscriber mixins.

    Maintains a mapping from ``Channel`` to sets of registered ``Handler``
    instances.  Both ``Subscriber`` and ``AsyncSubscriber`` inherit from this
    class to share the same storage contract.

    ``subscribe`` may be used as a decorator factory or as a direct call::

        @bus.subscribe(orders)
        def on_order(payload: OrderPlaced) -> None: ...

        handle = bus.subscribe(orders, on_order)
        bus.unsubscribe(orders, handle)

    Adapter-specific metadata is passed as a keyword argument::

        @bus.subscribe(orders, meta=kafka_meta)
        def on_order(payload: OrderPlaced) -> None: ...
        # on_order is a Handler; pass it to unsubscribe to deregister
    """

    def __init__(self) -> None:
        """Initialise the handler storage mapping."""
        self._handlers: dict[Channel, set[Handler[Any, Meta, Any]]] = defaultdict(set)
        super().__init__()

    @overload
    def subscribe[**P, R](
        self,
        channel: Channel,
        fn: Callable[P, R],
        *,
        meta: Meta = ...,
    ) -> Handler[P, Meta, R]: ...

    @overload
    def subscribe[**P, R](
        self,
        channel: Channel,
        *,
        meta: Meta = ...,
    ) -> Callable[[Callable[P, R]], Handler[P, Meta, R]]: ...

    def subscribe[**P, R](
        self,
        channel: Channel,
        fn: Callable[P, R] | None = None,
        *,
        meta: Meta = None,
    ) -> Handler[P, Meta, R] | Callable[[Callable[P, R]], Handler[P, Meta, R]]:
        """
        Register a handler callable for a channel, as a decorator or direct call.

        Returns the ``Handler`` instance in both forms so callers can pass it
        to ``unsubscribe`` later.

        Args:
            channel: The ``Channel`` to subscribe to.
            fn:      When supplied, registers ``fn`` directly and returns its
                     ``Handler``.  When omitted, returns a decorator that
                     registers and returns the ``Handler``.
            meta:    Optional adapter-specific metadata used for filtering at
                     dispatch time.

        """

        def decorator(f: Callable[P, R]) -> Handler[P, Meta, R]:
            handler: Handler[P, Meta, R] = Handler(f, meta)
            self._handlers[channel].add(handler)
            return handler

        if fn is not None:
            return decorator(fn)
        return decorator

    def get_handlers(self, channel: Channel) -> set[Handler[Any, Meta, Any]]:
        """
        Return the set of handlers registered for a channel.

        Args:
            channel: The ``Channel`` to look up.

        Returns:
            The set of handlers registered for ``channel``, or an empty set if
            none have been registered.

        """
        return self._handlers.get(channel, set())

    def unsubscribe(self, channel: Channel, handler: Handler[Any, Meta, Any]) -> None:
        """
        Deregister a handler for a channel.

        A no-op if ``handler`` is not currently registered for ``channel``.

        Args:
            channel: The ``Channel`` to unsubscribe from.
            handler: The ``Handler`` returned by ``subscribe``.

        """
        if channel in self._handlers:
            self._handlers[channel].discard(handler)


class Subscriber[Meta: (EventMeta | None)](SubscriberBase[Meta], ABC):
    """
    Mixin that provides synchronous event subscription.

    Subclasses must implement ``handle_subscribe``, which receives the channel,
    adapter metadata, and event payload and is responsible for invoking the
    registered handlers.
    """

    @abstractmethod
    def handle_subscribe(
        self, channel: Channel, payload: EventSchema, *, meta: Meta | None
    ) -> None:
        """
        Dispatch a payload to all handlers registered for a channel.

        Args:
            channel: The ``Channel`` the event arrived on.
            payload: The constructed ``EventSchema`` instance to dispatch.
            meta:    The adapter-specific routing metadata.

        """
        ...


class AsyncSubscriber[Meta: (EventMeta | None)](SubscriberBase[Meta], ABC):
    """
    Mixin that provides asynchronous event subscription.

    Subclasses must implement ``handle_subscribe`` as a coroutine, which
    receives the channel, adapter metadata, and event payload and is
    responsible for invoking the registered handlers.
    """

    @abstractmethod
    async def handle_subscribe(
        self, channel: Channel, payload: EventSchema, *, meta: Meta | None
    ) -> None:
        """
        Dispatch a payload to all handlers registered for a channel.

        Args:
            channel: The ``Channel`` the event arrived on.
            payload: The constructed ``EventSchema`` instance to dispatch.
            meta:    The adapter-specific routing metadata.

        """
        ...
