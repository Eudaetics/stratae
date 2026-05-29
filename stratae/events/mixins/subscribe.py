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

        @bus.subscribe(orders, meta)
        def on_order(payload: OrderPlaced) -> None: ...

        bus.subscribe(orders, meta, on_order)
    """

    def __init__(self) -> None:
        """Initialise the handler storage mapping."""
        self._handlers: dict[Channel, set[Handler[Meta, Any]]] = defaultdict(set)
        super().__init__()

    @overload
    def subscribe[R](
        self,
        channel: Channel,
        meta: Meta = ...,
    ) -> Callable[[Callable[[EventSchema], R]], Callable[[EventSchema], R]]: ...

    @overload
    def subscribe[R](
        self,
        channel: Channel,
        meta: Meta,
        fn: Callable[[EventSchema], R],
    ) -> Callable[[EventSchema], R]: ...

    def subscribe[R](
        self,
        channel: Channel,
        meta: Meta = None,
        fn: Callable[[EventSchema], R] | None = None,
    ) -> (
        Callable[[EventSchema], R]
        | Callable[[Callable[[EventSchema], R]], Callable[[EventSchema], R]]
    ):
        """
        Register a handler callable for a channel, as a decorator or direct call.

        Args:
            channel: The ``Channel`` to subscribe to.
            meta:    Optional adapter-specific metadata used for filtering at
                     dispatch time.
            fn:      When supplied, registers ``fn`` directly and returns it.
                     When omitted, returns a decorator that registers and
                     returns the decorated callable.

        """

        def decorator(f: Callable[[EventSchema], R]) -> Callable[[EventSchema], R]:
            self._handlers[channel].add(Handler(f, meta))
            return f

        if fn is not None:
            return decorator(fn)
        return decorator

    def get_handlers(self, channel: Channel) -> set[Handler[Meta, Any]]:
        """
        Return the set of handlers registered for a channel.

        Args:
            channel: The ``Channel`` to look up.

        Returns:
            The set of handlers registered for ``channel``, or an empty set if
            none have been registered.

        """
        return self._handlers.get(channel, set())

    def unsubscribe(self, channel: Channel, fn: Callable[..., Any]) -> None:
        """
        Deregister a handler for a channel by its original callable.

        A no-op if ``fn`` is not currently registered for ``channel``.

        Args:
            channel: The ``Channel`` to unsubscribe from.
            fn:      The original callable passed to ``subscribe``.

        """
        if channel in self._handlers:
            self._handlers[channel].discard(fn)  # pyright: ignore[reportArgumentType]


class Subscriber[Meta: (EventMeta | None)](SubscriberBase[Meta], ABC):
    """
    Mixin that provides synchronous event subscription.

    Subclasses must implement ``handle_subscribe``, which receives the channel,
    adapter metadata, and event payload and is responsible for invoking the
    registered handlers.
    """

    @abstractmethod
    def handle_subscribe(self, channel: Channel, meta: Meta | None, payload: EventSchema) -> None:
        """
        Dispatch a payload to all handlers registered for a channel.

        Args:
            channel: The ``Channel`` the event arrived on.
            meta:    The adapter-specific routing metadata.
            payload: The constructed ``EventSchema`` instance to dispatch.

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
        self, channel: Channel, meta: Meta | None, payload: EventSchema
    ) -> None:
        """
        Dispatch a payload to all handlers registered for a channel.

        Args:
            channel: The ``Channel`` the event arrived on.
            meta:    The adapter-specific routing metadata.
            payload: The constructed ``EventSchema`` instance to dispatch.

        """
        ...
