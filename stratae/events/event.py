"""Base event schema and bound-event abstractions for the stratae event system."""

from typing import Awaitable, Callable

from stratae.events.channel import Channel


class EventMeta:
    """
    Base marker class for event routing metadata.

    Subclass ``EventMeta`` to define the metadata shape required by a specific
    broker adapter.  The base class carries no fields; each adapter declares
    exactly what it needs as typed attributes on its own subclass.

    ``EventMeta`` instances are constructed by the adapter's ``publish``
    implementation and forwarded to ``emit_publish`` alongside the event payload.

    Example::

        class KafkaMeta(EventMeta):
            def __init__(self, topic: str, partition_key: str | None = None) -> None:
                self.topic = topic
                self.partition_key = partition_key
    """


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


class BoundEvent[**P, Metadata: (EventMeta | None), Resp]:
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
        channel: Channel,
        schema: Callable[P, EventSchema],
        emitter: Callable[[Channel, Metadata | None, EventSchema], Resp],
        meta: Metadata | None = None,
    ) -> None:
        """
        Bind an event schema to its emitter with routing metadata.

        Args:
            channel: The Channel over which the event will be emitted.
            schema:  The ``EventSchema`` subclass used to construct the event payload.
            emitter: A callable that receives a ``Channel``, ``Meta`` and a constructed
                     ``EventSchema`` instance, and returns ``Resp``.
            meta:    The adapter-specific routing metadata for this binding.

        """
        self.channel = channel
        self.schema = schema
        self.emitter = emitter
        self.meta = meta

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance and emit it with routing metadata.

        Returns:
            Whatever ``self.emitter`` returns.

        """
        return self.emitter(self.channel, self.meta, self.schema(*args, **kwargs))


class AsyncBoundEvent[**P, Metadata: (EventMeta | None), Resp](
    BoundEvent[P, Metadata, Awaitable[Resp]]
):
    """
    Async variant of ``BoundEvent`` for use with coroutine-based emitters.

    Invoking an ``AsyncBoundEvent`` returns a coroutine that must be awaited
    by the caller.

    Type parameters:
        P:    The parameter specification of the bound event's ``__call__`` signature.
        Meta: The ``EventMeta`` subclass carrying adapter-specific routing metadata.
        Resp: The type that the emitter's coroutine resolves to.
    """

    def __init__(
        self,
        channel: Channel,
        schema: Callable[P, EventSchema],
        emitter: Callable[[Channel, Metadata | None, EventSchema], Awaitable[Resp]],
        meta: Metadata | None = None,
    ) -> None:
        """
        Bind an event schema to its async emitter with routing metadata.

        Args:
            channel: The Channel over which the event will be emitted.
            schema:  The ``EventSchema`` subclass used to construct the event payload.
            emitter: An async callable that receives a ``Channel``, ``Meta`` and a constructed
                     ``EventSchema`` instance, and returns an awaitable resolving to ``Resp``.
            meta:    The adapter-specific routing metadata for this binding.

        """
        super().__init__(channel, schema, emitter, meta)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance, emit it with routing metadata, and await the result.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        return await self.emitter(self.channel, self.meta, self.schema(*args, **kwargs))
