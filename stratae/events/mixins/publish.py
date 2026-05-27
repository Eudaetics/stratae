"""Pub/sub mixins for synchronous and asynchronous event publishing."""

from abc import ABC, abstractmethod
from typing import Callable

from stratae.events.event import AsyncBoundEvent, BoundEvent, Event


class Publisher[Resp](ABC):
    """
    Mixin that binds Event subclasses to a synchronous publish emitter.

    Subclasses must implement ``emit_publish``, which receives a constructed
    ``Event`` instance and returns ``Resp``.  Call ``publish`` with an ``Event``
    subclass to receive a ``BoundEvent`` that constructs and emits instances of
    that event through ``emit_publish``.
    """

    def publish[**P](self, event: Callable[P, Event]) -> BoundEvent[P, Resp]:
        """
        Bind an Event subclass to this bus's emit_publish.

        Args:
            event: An ``Event`` subclass whose constructor accepts ``P``.

        Returns:
            A ``BoundEvent`` that, when called with ``P`` arguments, constructs
            an instance of ``event`` and forwards it to ``emit_publish``.

        """
        return BoundEvent(event, self.emit_publish)

    @abstractmethod
    def emit_publish(self, event: Event) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            event: The ``Event`` instance to dispatch.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class AsyncPublisher[Resp](ABC):
    """
    Mixin that binds Event subclasses to an asynchronous publish emitter.

    Subclasses must implement ``emit_publish`` as a coroutine.  Call
    ``publish`` with an ``Event`` subclass to receive an ``AsyncBoundEvent``
    whose ``__call__`` is itself a coroutine that must be awaited to dispatch
    the event and obtain ``Resp``.
    """

    def publish[**P](self, event: Callable[P, Event]) -> AsyncBoundEvent[P, Resp]:
        """
        Bind an Event subclass to this bus's emit_publish.

        Returns an ``AsyncBoundEvent`` so that callers can directly await
        the result of calling the bound event.

        Args:
            event: An ``Event`` subclass whose constructor accepts ``P``.

        Returns:
            An ``AsyncBoundEvent`` that, when called and awaited with ``P``
            arguments, constructs an instance of ``event`` and forwards it
            to ``emit_publish``.

        """
        return AsyncBoundEvent(event, self.emit_publish)

    @abstractmethod
    async def emit_publish(self, event: Event) -> Resp:
        """
        Dispatch a constructed event to all registered subscribers.

        Args:
            event: The ``Event`` instance to dispatch.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...
