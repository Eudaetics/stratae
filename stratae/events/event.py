"""Base event schema and bound-event abstractions for the stratae event system."""

from typing import Awaitable, Callable


class EventSchema:
    """
    Marker base class for event payload schemas.

    Subclass ``EventSchema`` to define the data shape carried by an event.
    The contract is that subclasses must be serializable and deserializable —
    the library does not enforce how, so any approach works: plain classes,
    ``dataclasses``, ``msgspec.Struct``, ``pydantic.BaseModel``, etc.

    Schemas carry no routing metadata and are reusable across channels.
    Routing information (channel, event type, version) lives on ``BoundEvent``.

    Example::

        class OrderPlaced(EventSchema):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id
    """


class BoundEvent[**P, Resp]:
    """
    Binds an ``EventSchema`` subclass to a synchronous emitter.

    A ``BoundEvent`` acts as a callable façade: invoking it constructs an
    instance of ``schema`` from the supplied arguments and forwards it to
    ``emitter``, returning whatever the emitter produces.

    Type parameters:
        P:    The parameter specification of the bound event's ``__call__`` signature.
        Resp: The return type produced by the emitter.
    """

    def __init__(
        self,
        schema: Callable[P, EventSchema],
        emitter: Callable[[EventSchema], Resp],
    ) -> None:
        """
        Bind an event schema to its emitter.

        Args:
            schema:  The ``EventSchema`` subclass used to construct the event payload.
            emitter: A callable that receives a constructed ``EventSchema`` instance
                     and returns ``Resp``.

        """
        self.event = schema
        self.emitter = emitter

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance and emit it.

        Passes all positional and keyword arguments to the schema constructor,
        then forwards the resulting instance to ``self.emitter``.

        Returns:
            Whatever ``self.emitter`` returns.

        """
        return self.emitter(self.event(*args, **kwargs))


class AsyncBoundEvent[**P, Resp](BoundEvent[P, Awaitable[Resp]]):
    """
    Async variant of ``BoundEvent`` for use with coroutine-based emitters.

    Invoking an ``AsyncBoundEvent`` returns a coroutine that must be awaited
    by the caller.

    Type parameters:
        P:    The parameter specification of the bound event's ``__call__`` signature.
        Resp: The type that the emitter's coroutine resolves to.
    """

    def __init__(
        self,
        schema: Callable[P, EventSchema],
        emitter: Callable[[EventSchema], Awaitable[Resp]],
    ) -> None:
        """
        Bind an event schema to its async emitter.

        Args:
            schema:  The ``EventSchema`` subclass used to construct the event payload.
            emitter: An async callable that receives a constructed ``EventSchema``
                     instance and returns an awaitable resolving to ``Resp``.

        """
        super().__init__(schema, emitter)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance, emit it, and await the result.

        Passes all positional and keyword arguments to the schema constructor,
        forwards the resulting instance to ``self.emitter``, and awaits the
        returned coroutine.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        return await self.emitter(self.event(*args, **kwargs))
