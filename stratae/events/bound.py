"""Bound event facades for synchronous and asynchronous bus bindings."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Protocol, overload

from stratae.events.event import Event, EventSchema, EventType


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
        Bind a factory and emitter with routing config.

        Args:
            emitter: A callable that receives the constructed payload and this
                     ``BoundEvent``, and returns ``Resp``.
            factory: A callable that constructs the ``EventSchema`` payload from
                     the arguments passed to ``__call__``.
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

    An ``AsyncBoundEvent`` acts as a callable façade: invoking it calls ``factory``
    with the supplied arguments to construct the payload, forwards it and itself
    to ``emitter``, and awaits the resulting coroutine.

    Type parameters:
        P:           The parameter specification of the bound event's ``__call__`` signature.
        EventConfig: The adapter-specific routing config type.
        Resp:        The type that the emitter's coroutine resolves to.
    """

    def __init__(
        self,
        emitter: Callable[[EventSchema, AsyncBoundEvent[P, EventConfig, Resp]], Awaitable[Resp]],
        factory: Callable[P, EventSchema],
        *,
        config: EventConfig,
    ) -> None:
        """
        Bind a factory and async emitter with routing config.

        Args:
            emitter: A coroutine callable that receives the constructed payload and this
                     ``AsyncBoundEvent``, and returns an awaitable resolving to ``Resp``.
            factory: A callable that constructs the ``EventSchema`` payload from
                     the arguments passed to ``__call__``.
            config:  The adapter-specific routing config for this binding.

        """
        self.factory = factory
        self.emitter = emitter
        self.config = config

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance, forward it to the emitter, and await the result.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        return await self.emitter(self.factory(*args, **kwargs), self)


class _BindDecorator[C, R](Protocol):
    """Return type of the decorator form of ``bind``: an emitter+config awaiting an ``Event``."""

    def __call__[**P, S: EventSchema, T: EventType](
        self, event: Event[P, S, T]
    ) -> BoundEvent[P, C, R]:
        """Bind the emitter and config to ``event``, returning a ``BoundEvent``."""
        ...


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
    """Bind an emitter to an event schema, returning a ``BoundEvent`` or a decorator."""
    if event is None:

        def decorator(evt: Event[P, S, T]) -> BoundEvent[P, C, R]:
            return BoundEvent(emitter, evt.schema, config=config)

        return decorator
    return BoundEvent(emitter, event.schema, config=config)


class _ABindDecorator[C, R](Protocol):
    """Return type of the decorator form of ``abind``: async emitter+config awaiting an Event."""

    def __call__[**P, S: EventSchema, T: EventType](
        self, event: Event[P, S, T]
    ) -> AsyncBoundEvent[P, C, R]:
        """Bind the async emitter and config to ``event``, returning an ``AsyncBoundEvent``."""
        ...


@overload
def abind[**P, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, AsyncBoundEvent[P, C, R]], Awaitable[R]],
    event: Event[P, S, T],
    *,
    config: C,
) -> AsyncBoundEvent[P, C, R]: ...


@overload
def abind[C, R](
    emitter: Callable[[EventSchema, AsyncBoundEvent[..., C, R]], Awaitable[R]],
    *,
    config: C,
) -> _ABindDecorator[C, R]: ...


def abind[**P, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, AsyncBoundEvent[P, C, R]], Awaitable[R]],
    event: Event[P, S, T] | None = None,
    *,
    config: C,
) -> AsyncBoundEvent[P, C, R] | Callable[[Event[P, S, T]], AsyncBoundEvent[P, C, R]]:
    """Bind an async emitter to an event, returning an ``AsyncBoundEvent`` or a decorator."""
    if event is None:

        def decorator(evt: Event[P, S, T]) -> AsyncBoundEvent[P, C, R]:
            return AsyncBoundEvent(emitter, evt.schema, config=config)

        return decorator
    return AsyncBoundEvent(emitter, event.schema, config=config)


class _BindFactoryDecorator[**Q, S: EventSchema, C, R](Protocol):
    """Returned by ``bind_factory`` when no factory is given; decorates a factory callable."""

    def __call__(self, factory: Callable[Q, S]) -> BoundEvent[Q, C, R]:
        """Bind the emitter and config to ``factory``, returning a ``BoundEvent``."""
        ...


@overload
def bind_factory[**P, **Q, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, BoundEvent[Q, C, R]], R],
    event: Event[P, S, T],
    factory: Callable[Q, S],
    *,
    config: C,
) -> BoundEvent[Q, C, R]: ...


@overload
def bind_factory[**P, **Q, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, BoundEvent[Q, C, R]], R],
    event: Event[P, S, T],
    *,
    config: C,
) -> _BindFactoryDecorator[Q, S, C, R]: ...


def bind_factory[**P, **Q, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, BoundEvent[Q, C, R]], R],
    event: Event[P, S, T],
    factory: Callable[Q, S] | None = None,
    *,
    config: C,
) -> BoundEvent[Q, C, R] | Callable[[Callable[Q, S]], BoundEvent[Q, C, R]]:
    """Bind a custom factory to an event emitter, returning a ``BoundEvent`` or a decorator."""
    if factory is None:

        def decorator(f: Callable[Q, S]) -> BoundEvent[Q, C, R]:
            return BoundEvent(emitter, f, config=config)

        return decorator
    return BoundEvent(emitter, factory, config=config)


class _ABindFactoryDecorator[**Q, S: EventSchema, C, R](Protocol):
    """Returned by ``abind_factory`` when no factory is given; decorates a factory callable."""

    def __call__(self, factory: Callable[Q, S]) -> AsyncBoundEvent[Q, C, R]:
        """Bind the async emitter and config to ``factory``, returning an ``AsyncBoundEvent``."""
        ...


@overload
def abind_factory[**P, **Q, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, AsyncBoundEvent[Q, C, R]], Awaitable[R]],
    event: Event[P, S, T],
    factory: Callable[Q, S],
    *,
    config: C,
) -> AsyncBoundEvent[Q, C, R]: ...


@overload
def abind_factory[**P, **Q, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, AsyncBoundEvent[Q, C, R]], Awaitable[R]],
    event: Event[P, S, T],
    *,
    config: C,
) -> _ABindFactoryDecorator[Q, S, C, R]: ...


def abind_factory[**P, **Q, S: EventSchema, T: EventType, C, R](
    emitter: Callable[[EventSchema, AsyncBoundEvent[Q, C, R]], Awaitable[R]],
    event: Event[P, S, T],
    factory: Callable[Q, S] | None = None,
    *,
    config: C,
) -> AsyncBoundEvent[Q, C, R] | Callable[[Callable[Q, S]], AsyncBoundEvent[Q, C, R]]:
    """Bind a custom factory to an async emitter, returning an ``AsyncBoundEvent`` or decorator."""
    if factory is None:

        def decorator(f: Callable[Q, S]) -> AsyncBoundEvent[Q, C, R]:
            return AsyncBoundEvent(emitter, f, config=config)

        return decorator
    return AsyncBoundEvent(emitter, factory, config=config)
