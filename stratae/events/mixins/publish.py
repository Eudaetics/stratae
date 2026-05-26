"""Pub/sub mixins for synchronous and asynchronous event publishing."""

from abc import ABC, abstractmethod
from typing import Awaitable, Callable

from stratae.events.event import BoundEvent, Event


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


class AsyncPublisher[Resp](Publisher[Awaitable[Resp]]):
    """
    Async variant of ``Publisher`` for coroutine-based emitters.

    Specialises ``Publisher`` so that ``emit_publish`` is a coroutine.
    Calling ``publish`` returns a ``BoundEvent`` whose result must be awaited
    to dispatch the event and obtain ``Resp``.
    """

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
