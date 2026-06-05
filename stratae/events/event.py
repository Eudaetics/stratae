"""Base event schema and bound-event abstractions for the stratae event system."""

from __future__ import annotations

from typing import Any, Awaitable, Callable


class EventSchema:
    """
    Marker base class for event payload schemas.

    Subclass ``EventSchema`` to define the data shape carried by an event.
    The contract is that subclasses must be serializable and deserializable —
    the library does not enforce how, so any approach works: plain classes,
    ``dataclasses``, ``msgspec.Struct``, ``pydantic.BaseModel``, etc.

    Schemas carry no routing metadata and are reusable across channels.
    Routing information lives on the adapter-specific ``EventMeta`` subclass.

    Example::

        class OrderPlaced(EventSchema):
            def __init__(self, order_id: int) -> None:
                self.order_id = order_id
    """


class BoundEvent[**P, EventConfig: Any, Resp]:
    """
    Binds an ``EventSchema`` subclass to a synchronous emitter with routing metadata.

    A ``BoundEvent`` acts as a callable façade: invoking it constructs an
    instance of ``schema`` from the supplied arguments and forwards the
    routing metadata and payload to ``emitter``, returning whatever the
    emitter produces.

    Type parameters:
        P:    The parameter specification of the bound event's ``__call__`` signature.
        Meta: The ``EventMeta`` subclass carrying adapter-specific routing metadata.
        Resp: The return type produced by the emitter.
    """

    def __init__(
        self,
        schema: Callable[P, EventSchema],
        emitter: Callable[[EventSchema, BoundEvent[P, EventConfig, Resp]], Resp],
        *,
        config: EventConfig = None,
    ) -> None:
        """
        Bind an event schema to its emitter with routing metadata.

        Args:
            schema:  The ``EventSchema`` subclass used to construct the event payload.
            emitter: A callable that receives a ``Channel``, ``Meta`` and a constructed
                     ``EventSchema`` instance, and returns ``Resp``.
            config:    The adapter-specific configuration for this binding.

        """
        self.schema = schema
        self.emitter = emitter
        self.config = config

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance and emit it with routing metadata.

        Returns:
            Whatever ``self.emitter`` returns.

        """
        return self.emitter(self.schema(*args, **kwargs), self)


class AsyncBoundEvent[**P, EventConfig: Any, Resp](BoundEvent[P, EventConfig, Awaitable[Resp]]):
    """
    Async variant of ``BoundEvent`` for use with coroutine-based emitters.

    Invoking an ``AsyncBoundEvent`` returns a coroutine that must be awaited
    by the caller.

    Type parameters:
        P:    The parameter specification of the bound event's ``__call__`` signature.
        Meta: The ``EventMeta`` subclass carrying adapter-specific routing metadata.
        Resp: The type that the emitter's coroutine resolves to.
    """

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance, emit it with routing metadata, and await the result.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        return await self.emitter(self.schema(*args, **kwargs), self)
