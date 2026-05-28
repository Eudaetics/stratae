"""Base event and bound-event abstractions for the stratae event system."""

from typing import Awaitable, Callable


class EventSchema:
    """
    Base class for event payload schemas in the stratae event system.

    Subclass ``EventSchema`` to define the data shape carried by an event.
    Schemas are reusable across channels — the same class may be published
    under different event type identifiers via ``BoundEvent``.

    ``EventSchema`` carries no routing metadata.  Channel, event type, and
    version all live on ``BoundEvent``.

    Example::

        class OrderPlaced(EventSchema):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id
    """


class BoundEvent[**P, Resp]:
    """
    Abstract base that binds an ``Event`` type to a synchronous emitter.

    A ``BoundEvent`` acts as a callable façade: invoking it constructs an
    instance of ``event`` from the supplied arguments and forwards it to
    ``emitter``, returning whatever the emitter produces.

    Type parameters:
        P:    The parameter specification of the bound event's ``__call__`` signature.
        R:    The return type produced by each registered handler.
        Resp: The return type produced by the emitter (e.g. an aggregated
              collection of handler results).
    """

    def __init__(
        self,
        event: Callable[P, EventSchema],
        emitter: Callable[[EventSchema], Resp],
    ) -> None:
        """
        Bind an event type to its emitter.

        Args:
            event:   The ``Event`` subclass whose instances will be emitted.
            emitter: A callable that receives a constructed ``Event`` instance
                     and returns ``Resp``.

        """
        self.event = event
        self.emitter = emitter

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the event and emit it.

        Passes all positional and keyword arguments to the event constructor,
        then forwards the resulting instance to ``self.emitter``.

        Returns:
            Whatever ``self.emitter`` returns.

        """
        return self.emitter(self.event(*args, **kwargs))


class AsyncBoundEvent[**P, Resp](BoundEvent[P, Awaitable[Resp]]):
    """
    Async variant of ``BoundEvent`` for use with coroutine-based emitters.

    Specialises ``BoundEvent`` so that both the handlers and the emitter are
    awaitable.  Invoking an ``AsyncBoundEvent`` returns a coroutine that must
    be awaited by the caller.

    Type parameters:
        P:    The parameter specification of the bound event's ``__call__`` signature.
        R:    The type that each registered handler's coroutine resolves to.
        Resp: The type that the emitter's coroutine resolves to.
    """

    def __init__(
        self,
        event: Callable[P, EventSchema],
        emitter: Callable[[EventSchema], Awaitable[Resp]],
    ) -> None:
        """
        Bind an event type to its async emitter.

        Args:
            event:   The ``Event`` subclass whose instances will be emitted.
            emitter: An async callable that receives a constructed ``Event``
                     instance and returns an awaitable resolving to ``Resp``.

        """
        super().__init__(event, emitter)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the event, emit it, and await the result.

        Passes all positional and keyword arguments to the event constructor,
        forwards the resulting instance to ``self.emitter``, and awaits the
        returned coroutine.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        return await self.emitter(self.event(*args, **kwargs))
