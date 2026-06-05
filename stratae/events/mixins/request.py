"""Req/rep mixins for synchronous and asynchronous event requesting."""

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

from stratae.events.event import AsyncBoundEvent, BoundEvent, EventSchema


class Requester[EventConfig: Any, Resp](ABC):
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
        schema: Callable[P, EventSchema],
        *,
        config: EventConfig = None,
    ) -> BoundEvent[P, EventConfig, Resp]:
        """
        Bind an ``EventSchema`` subclass to this requester's ``emit_request``.

        Args:
            schema:  An ``EventSchema`` subclass whose constructor accepts ``P``.
            config:  The adapter-specific configuration for a request.

        Returns:
            A ``BoundEvent`` that, when called with ``P`` arguments, constructs
            an instance of ``schema`` and forwards it to ``emit_request``.

        """
        return BoundEvent(schema, self.emit_request, config=config)

    @abstractmethod
    def emit_request[**P](
        self, payload: EventSchema, event: BoundEvent[P, EventConfig, Resp]
    ) -> Resp:
        """
        Send a constructed request event and return the response.

        Args:
            payload: The constructed ``EventSchema`` instance to send.
            event:  The bound event being emitted.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...


class AsyncRequester[EventConfig: Any, Resp](ABC):
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
        schema: Callable[P, EventSchema],
        *,
        config: EventConfig = None,
    ) -> AsyncBoundEvent[P, EventConfig, Resp]:
        """
        Bind an ``EventSchema`` subclass to this requester's ``emit_request``.

        Args:
            schema:  An ``EventSchema`` subclass whose constructor accepts ``P``.
            config:  The adapter-specific configuration for a request.

        Returns:
            An ``AsyncBoundEvent`` that, when called and awaited with ``P``
            arguments, constructs an instance of ``schema`` and forwards it
            to ``emit_request``.

        """
        return AsyncBoundEvent(schema, self.emit_request, config=config)

    @abstractmethod
    async def emit_request[**P](
        self, payload: EventSchema, event: BoundEvent[P, EventConfig, Awaitable[Resp]]
    ) -> Resp:
        """
        Send a constructed request event and return the awaitable response.

        Args:
            payload: The constructed ``EventSchema`` instance to send.
            event:  The adapter-specific configuration for a request.

        Returns:
            ``Resp`` as defined by the concrete subclass.

        """
        ...
