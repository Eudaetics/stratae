"""Req/rep mixins for synchronous and asynchronous event requesting."""

from abc import ABC, abstractmethod
from typing import Callable

from stratae.events.channel import Channel
from stratae.events.event import AsyncBoundEvent, BoundEvent, EventMeta, EventSchema


class Requester[Metadata: (EventMeta | None), Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to a synchronous request emitter.

    Subclasses must implement ``emit_request``, which receives the channel,
    adapter-specific metadata, and a constructed ``EventSchema`` instance and
    returns ``Resp``.  Adapter-specific concerns such as the expected response
    type are carried by the ``Metadata`` subclass and passed via ``meta``.

    Example::

        class RpcRequester(Requester[RpcMeta, PriceResponse]):
            def emit_request(self, channel, payload, *, meta):
                ...  # send request, deserialize response using meta.response_type

        get_price = requester.request(channel, GetPrice, meta=RpcMeta(response_type=PriceResponse))
        price = get_price(item_id=42)
    """

    def request[**P](
        self,
        channel: Channel,
        schema: Callable[P, EventSchema],
        *,
        meta: Metadata = None,
    ) -> BoundEvent[P, Metadata, Resp]:
        """
        Bind an ``EventSchema`` subclass to this requester's ``emit_request``.

        Args:
            channel: A Channel over which to send the request.
            schema:  An ``EventSchema`` subclass whose constructor accepts ``P``.
            meta:    The adapter-specific routing metadata for this binding,
                     including any response type or deserialization hints.

        Returns:
            A ``BoundEvent`` that, when called with ``P`` arguments, constructs
            an instance of ``schema`` and forwards it to ``emit_request``.

        """
        return BoundEvent(channel, schema, self.emit_request, meta=meta)

    @abstractmethod
    def emit_request(self, channel: Channel, payload: EventSchema, *, meta: Metadata) -> Resp:
        """
        Send a constructed request event and return the response.

        Args:
            channel: A Channel over which to send the request.
            payload: The constructed ``EventSchema`` instance to send.
            meta:    The adapter-specific routing metadata, including any
                     response type or deserialization hints.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class AsyncRequester[Metadata: (EventMeta | None), Resp](ABC):
    """
    Mixin that binds ``EventSchema`` subclasses to an asynchronous request emitter.

    Subclasses must implement ``emit_request`` as a coroutine, which receives the
    channel, adapter-specific metadata, and a constructed ``EventSchema`` instance
    and returns an awaitable resolving to ``Resp``.  Adapter-specific concerns such
    as the expected response type are carried by the ``Metadata`` subclass and
    passed via ``meta``.
    """

    def request[**P](
        self,
        channel: Channel,
        schema: Callable[P, EventSchema],
        *,
        meta: Metadata = None,
    ) -> AsyncBoundEvent[P, Metadata, Resp]:
        """
        Bind an ``EventSchema`` subclass to this requester's ``emit_request``.

        Args:
            channel: A Channel over which to send the request.
            schema:  An ``EventSchema`` subclass whose constructor accepts ``P``.
            meta:    The adapter-specific routing metadata for this binding,
                     including any response type or deserialization hints.

        Returns:
            An ``AsyncBoundEvent`` that, when called and awaited with ``P``
            arguments, constructs an instance of ``schema`` and forwards it
            to ``emit_request``.

        """
        return AsyncBoundEvent(channel, schema, self.emit_request, meta=meta)

    @abstractmethod
    async def emit_request(self, channel: Channel, payload: EventSchema, *, meta: Metadata) -> Resp:
        """
        Send a constructed request event and return the awaitable response.

        Args:
            channel: A Channel over which to send the request.
            payload: The constructed ``EventSchema`` instance to send.
            meta:    The adapter-specific routing metadata, including any
                     response type or deserialization hints.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...
