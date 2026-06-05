"""Pub/sub mixins for synchronous and asynchronous event subscription."""

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Callable, overload

from stratae.events.event import EventSchema
from stratae.events.handler import Handler


class SubscriberBase[HandlerConfig: Any]:
    """
    Base mixin providing shared handler storage for subscriber mixins.

    Maintains a mapping from config to sets of registered ``Handler`` instances.
    Both ``Subscriber`` and ``AsyncSubscriber`` inherit from this class to share
    the same storage contract.

    ``subscribe`` may be used as a decorator or as a direct call::

        @bus.subscribe(emit_order)
        def on_order(payload: OrderPlaced) -> None: ...

        handle = bus.subscribe(emit_order, on_order)
        bus.unsubscribe(handle)
    """

    def __init__(self) -> None:
        """Initialise the handler storage mapping."""
        self._handlers: dict[Any, set[Handler[Any, HandlerConfig, Any]]] = defaultdict(set)
        super().__init__()

    @overload
    def subscribe[**P, R](
        self,
        config: HandlerConfig,
        fn: Callable[P, R],
    ) -> Handler[P, HandlerConfig, R]: ...

    @overload
    def subscribe[**P, R](
        self,
        config: HandlerConfig,
        fn: None = None,
    ) -> Callable[[Callable[P, R]], Handler[P, HandlerConfig, R]]: ...

    def subscribe[**P, R](
        self,
        config: HandlerConfig,
        fn: Callable[P, R] | None = None,
    ) -> Handler[P, HandlerConfig, R] | Callable[[Callable[P, R]], Handler[P, HandlerConfig, R]]:
        """
        Register a handler callable for a config, as a decorator or direct call.

        Returns the ``Handler`` instance in both forms so callers can pass it
        to ``unsubscribe`` later.

        Args:
            config: The adapter-specific config used as the handler routing key.
            fn:     When supplied, registers ``fn`` directly and returns its
                    ``Handler``.  When omitted, returns a decorator that
                    registers and returns the ``Handler``.

        """

        def decorator(f: Callable[P, R]) -> Handler[P, HandlerConfig, R]:
            handler: Handler[P, HandlerConfig, R] = Handler(f, config)
            self._handlers[self._make_handler_key(config)].add(handler)
            return handler

        if fn is not None:
            return decorator(fn)
        return decorator

    def get_handlers(self, config: HandlerConfig) -> set[Handler[Any, HandlerConfig, Any]]:
        """
        Return the set of handlers registered for a config.

        Args:
            config: Configuration option used to define a mapping to handlers.

        Returns:
            The set of handlers registered for ``config``, or an empty set if
            none have been registered.

        """
        return self._handlers.get(self._make_handler_key(config), set())

    def unsubscribe(self, handler: Handler[Any, HandlerConfig, Any]) -> None:
        """
        Deregister a handler using its config.

        A no-op if ``handler`` is not currently registered for ``config``.

        Args:
            handler: The ``Handler`` returned by ``subscribe``.

        """
        self._handlers[self._make_handler_key(handler.config)].discard(handler)

    def _make_handler_key(self, config: HandlerConfig) -> Any:
        return config


class Subscriber[HandlerConfig: Any](SubscriberBase[HandlerConfig], ABC):
    """
    Mixin that provides synchronous event subscription.

    Subclasses must implement ``handle_subscribe``, which receives the event
    payload and config and is responsible for invoking the registered handlers.
    """

    @abstractmethod
    def handle_subscribe(self, payload: EventSchema, *, config: HandlerConfig) -> None:
        """
        Dispatch a payload to all handlers registered for a config.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            config:    The adapter-specific handler configuration.

        """
        ...


class AsyncSubscriber[HandlerConfig: Any](SubscriberBase[HandlerConfig], ABC):
    """
    Mixin that provides asynchronous event subscription.

    Subclasses must implement ``handle_subscribe`` as a coroutine, which
    receives the event payload and config and is responsible for invoking
    the registered handlers.
    """

    @abstractmethod
    async def handle_subscribe(self, payload: EventSchema, *, config: HandlerConfig) -> None:
        """
        Dispatch a payload to all handlers registered for a config.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            config:    The adapter-specific handler configuration.

        """
        ...
