"""Bound event facades for synchronous and asynchronous bus bindings."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Protocol, overload

from stratae.events.event import Event, EventSchema, EventType, PubSub, event


class BoundEvent[**P, EventConfig: Any, Resp]:
    """
    Binds an ``EventSchema`` subclass to a synchronous emitter with routing config.

    A ``BoundEvent`` acts as a callable façade: invoking it constructs an
    instance of ``schema`` from the supplied arguments and forwards the
    payload and itself to ``emitter``, returning whatever the emitter produces.

    Type parameters:
        P:           The parameter specification of the bound event's ``__call__`` signature.
        EventConfig: The adapter-specific routing config type.
        Resp:        The return type produced by the emitter.
    """

    def __init__(
        self,
        emitter: Callable[[EventSchema, BoundEvent[P, EventConfig, Resp]], Resp],
        factory: Callable[P, EventSchema],
        *,
        config: EventConfig,
    ) -> None:
        """
        Bind an event schema to its emitter with routing config.

        Args:
            schema:  The ``EventSchema`` subclass used to construct the event payload.
            emitter: A callable that receives the constructed payload and this
                     ``BoundEvent``, and returns ``Resp``.
            config:  The adapter-specific routing config for this binding.

        """
        self.emitter = emitter
        self.factory = factory
        self.config = config

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance and forward it to the emitter.

        Returns:
            Whatever ``self.emitter`` returns.

        """
        return self.emitter(self.factory(*args, **kwargs), self)


class AsyncBoundEvent[**P, EventConfig: Any, Resp]:
    """
    Binds an ``EventSchema`` subclass to an asynchronous emitter with routing config.

    An ``AsyncBoundEvent`` acts as a callable façade: invoking it constructs an
    instance of ``schema`` from the supplied arguments, forwards the payload and
    itself to ``emitter``, and awaits the resulting coroutine.

    Type parameters:
        P:           The parameter specification of the bound event's ``__call__`` signature.
        EventConfig: The adapter-specific routing config type.
        Resp:        The type that the emitter's coroutine resolves to.
    """

    def __init__(
        self,
        schema: Callable[P, EventSchema],
        emitter: Callable[[EventSchema, AsyncBoundEvent[P, EventConfig, Resp]], Awaitable[Resp]],
        *,
        config: EventConfig,
    ) -> None:
        """
        Bind an event schema to its async emitter with routing config.

        Args:
            schema:  The ``EventSchema`` subclass used to construct the event payload.
            emitter: A coroutine callable that receives the constructed payload and this
                     ``AsyncBoundEvent``, and returns an awaitable resolving to ``Resp``.
            config:  The adapter-specific routing config for this binding.

        """
        self.schema = schema
        self.emitter = emitter
        self.config = config

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance, forward it to the emitter, and await the result.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        return await self.emitter(self.schema(*args, **kwargs), self)


class _BindDecorator[C, R](Protocol):
    def __call__[**P, S: EventSchema, T: EventType](
        self, event: Event[P, S, T]
    ) -> BoundEvent[P, C, R]: ...


@overload
def bind[**P, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, BoundEvent[P, C, R]], R],
    event: Event[P, S, T],
    *,
    config: C,
) -> BoundEvent[P, C, R]: ...


@overload
def bind[C, R](
    emitter: Callable[[EventSchema, BoundEvent[..., C, R]], R],
    *,
    config: C,
) -> _BindDecorator[C, R]: ...


def bind[**P, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, BoundEvent[P, C, R]], R],
    event: Event[P, S, T] | None = None,
    *,
    config: C,
) -> BoundEvent[P, C, R] | Callable[[Event[P, S, T]], BoundEvent[P, C, R]]:
    if event is None:

        def decorator(evt: Event[P, S, T]) -> BoundEvent[P, C, R]:
            return BoundEvent(emitter, evt.schema, config=config)

        return decorator
    return BoundEvent(emitter, event.schema, config=config)


class Example(EventSchema):
    def __init__(self, name: str):
        self.name = name


def x[R](payload: EventSchema, y: BoundEvent[Any, Any, R]) -> None:
    print("testing")


@bind(x, config=None)
@event(PubSub)
class new_example(EventSchema):
    """Some docstring"""

    def __init__(self, name: str):
        self.name = name


e = Event(Example, PubSub)

thing = bind(x, e, config=None)
thing("x")
new_example("asdf")
