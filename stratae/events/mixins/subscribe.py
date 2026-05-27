"""Pub/sub mixins for synchronous and asynchronous event subscription."""

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Callable

from stratae.events.event import Event

type Handler = Callable[[Event], Any]


class SubscriberBase:
    """
    Base mixin providing shared handler storage for subscriber mixins.

    Maintains a mapping from ``Event`` subclasses to sets of registered
    handler callables.  Both ``Subscriber`` and ``AsyncSubscriber`` inherit
    from this class to share the same storage contract.
    """

    def __init__(self) -> None:
        """Initialise the handler storage mapping."""
        self._handlers: dict[type[Event], set[Handler]] = defaultdict(set)
        super().__init__()

    def subscribe(self, event: type[Event], handler: Handler) -> None:
        """
        Register a handler for an event type.

        Args:
            event:   The ``Event`` subclass to subscribe to.
            handler: The callable to invoke when an instance of ``event``
                     is dispatched.

        """
        self._handlers[event].add(handler)

    def get_handlers(self, event: type[Event]) -> set[Handler]:
        """
        Return the set of handlers registered for an event type.

        Args:
            event: The ``Event`` subclass to look up.

        Returns:
            The set of handlers registered for ``event``, or an empty set if
            none have been registered.

        """
        return self._handlers.get(event, set())

    def unsubscribe(self, event: type[Event], handler: Handler) -> None:
        """
        Deregister a handler for an event type.

        A no-op if ``handler`` is not currently registered for ``event``.

        Args:
            event:   The ``Event`` subclass to unsubscribe from.
            handler: The callable to remove.

        """
        if event in self._handlers:
            self._handlers[event].discard(handler)


class Subscriber(SubscriberBase, ABC):
    """
    Mixin that provides synchronous event subscription.

    Subclasses must implement ``handle_subscribe``, which receives a dispatched
    ``Event`` instance and is responsible for invoking the registered handlers.
    """

    @abstractmethod
    def handle_subscribe(self, event: Event) -> None:
        """
        Dispatch a constructed event to all registered handlers.

        Args:
            event: The ``Event`` instance to dispatch.

        """
        ...


class AsyncSubscriber(SubscriberBase, ABC):
    """
    Mixin that provides asynchronous event subscription.

    Subclasses must implement ``handle_subscribe`` as a coroutine, which receives
    a dispatched ``Event`` instance and is responsible for invoking the
    registered handlers.
    """

    @abstractmethod
    async def handle_subscribe(self, event: Event) -> None:
        """
        Dispatch a constructed event to all registered handlers.

        Args:
            event: The ``Event`` instance to dispatch.

        """
        ...
