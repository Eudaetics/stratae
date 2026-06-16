"""Bound event facades for synchronous and asynchronous bus bindings."""

from __future__ import annotations

import inspect
from typing import Any, Awaitable, Callable, Protocol, TypeGuard, overload

from stratae.events.event import EventConfig, EventType, Payload

_SYNC_FACTORY_REQUIRED = "bind requires a sync factory; resolve async work outside the factory"


class BoundEvent[**P, S: Payload, T: EventType, RoutingConfig: Any, Resp]:
    """
    Binds an ``EventConfig`` to a synchronous emitter with routing config.

    A ``BoundEvent`` acts as a callable façade: invoking it constructs a
    payload via the event's factory and forwards the payload and itself to
    ``emitter``, returning whatever the emitter produces.

    Type parameters:
        P:             The parameter specification of the bound event's ``__call__`` signature.
        S:             The ``Payload`` subclass produced by the event's factory.
        T:             The ``EventType`` discriminant of the bound event.
        RoutingConfig: The adapter-specific routing config type.
        Resp:          The return type produced by the emitter.
    """

    def __init__(
        self,
        emitter: Callable[[Payload, EventConfig[P, S, T], RoutingConfig], Resp],
        event: EventConfig[P, S, T],
        *,
        config: RoutingConfig,
    ) -> None:
        """
        Bind an event and emitter with routing config.

        Args:
            emitter: A callable that receives the constructed payload and this
                     ``BoundEvent``, and returns ``Resp``.
            event:   The ``EventConfig`` whose factory constructs the payload.
            config:  The adapter-specific routing config for this binding.

        """
        self.emitter = emitter
        self.event = event
        self.config = config

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the payload and forward it to the emitter.

        Returns:
            Whatever ``self.emitter`` returns.

        """
        if not _is_sync_factory(self.event.factory):
            raise TypeError(_SYNC_FACTORY_REQUIRED)
        return self.emitter(self.event.factory(*args, **kwargs), self.event, self.config)


class AsyncBoundEvent[**P, S: Payload, T: EventType, RoutingConfig: Any, Resp]:
    """
    Binds a ``Payload`` subclass to an asynchronous emitter with routing config.

    An ``AsyncBoundEvent`` acts as a callable façade: invoking it calls ``factory``
    with the supplied arguments to construct the payload, forwards it and itself
    to ``emitter``, and awaits the resulting coroutine.

    Type parameters:
        P:             The parameter specification of the bound event's ``__call__`` signature.
        RoutingConfig: The adapter-specific routing config type.
        Resp:          The type that the emitter's coroutine resolves to.
    """

    def __init__(
        self,
        emitter: Callable[[Payload, EventConfig[P, S, T], RoutingConfig], Awaitable[Resp]],
        event: EventConfig[P, S, T],
        *,
        config: RoutingConfig,
    ) -> None:
        """
        Bind a factory and async emitter with routing config.

        Args:
            emitter: A coroutine callable that receives the constructed payload and this
                     ``AsyncBoundEvent``, and returns an awaitable resolving to ``Resp``.
            event:   The ``EventConfig`` whose factory constructs the payload.
            config:  The adapter-specific routing config for this binding.

        """
        self.event = event
        self.emitter = emitter
        self.config = config

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance, forward it to the emitter, and await the result.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        payload = self.event.factory(*args, **kwargs)
        if isinstance(payload, Awaitable):
            payload = await payload
        return await self.emitter(payload, self.event, self.config)


def _is_sync_factory[**P, E: Payload](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[Callable[P, E]]:
    return not inspect.iscoroutinefunction(factory)


class _BindDecorator[C, R](Protocol):
    """Decorator form of ``bind``: takes an ``EventConfig``, returns a ``BoundEvent``."""

    def __call__[**P, S: Payload, T: EventType](
        self, event: EventConfig[P, S, T]
    ) -> BoundEvent[P, S, T, C, R]:
        """Bind the emitter and config to ``event``, returning a ``BoundEvent``."""
        ...


@overload
def bind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, EventConfig[P, S, T], C], R],
    event: EventConfig[P, S, T],
    *,
    config: C,
) -> BoundEvent[P, S, T, C, R]: ...


@overload
def bind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, EventConfig[P, S, T], C], R],
    *,
    config: C,
) -> _BindDecorator[C, R]: ...


def bind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, EventConfig[P, S, T], C], R],
    event: EventConfig[P, S, T] | None = None,
    *,
    config: C,
) -> BoundEvent[P, S, T, C, R] | Callable[[EventConfig[P, S, T]], BoundEvent[P, S, T, C, R]]:
    """Bind an emitter to an ``EventConfig``, returning a ``BoundEvent`` or a decorator."""
    if event is None:

        def decorator(evt: EventConfig[P, S, T]) -> BoundEvent[P, S, T, C, R]:
            if not _is_sync_factory(evt.factory):
                raise TypeError(_SYNC_FACTORY_REQUIRED)
            return BoundEvent(emitter, evt, config=config)

        return decorator
    if not _is_sync_factory(event.factory):
        raise TypeError(_SYNC_FACTORY_REQUIRED)
    return BoundEvent(emitter, event, config=config)


class _ABindDecorator[C, R](Protocol):
    """Return type of the decorator form of ``abind``: async emitter+config awaiting an Event."""

    def __call__[**P, S: Payload, T: EventType](
        self, event: EventConfig[P, S, T]
    ) -> AsyncBoundEvent[P, S, T, C, R]:
        """Bind the async emitter and config to ``event``, returning an ``AsyncBoundEvent``."""
        ...


@overload
def abind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, EventConfig[P, S, T], C], Awaitable[R]],
    event: EventConfig[P, S, T],
    *,
    config: C,
) -> AsyncBoundEvent[P, S, T, C, R]: ...


@overload
def abind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, EventConfig[P, S, T], C], Awaitable[R]],
    *,
    config: C,
) -> _ABindDecorator[C, R]: ...


def abind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, EventConfig[P, S, T], C], Awaitable[R]],
    event: EventConfig[P, S, T] | None = None,
    *,
    config: C,
) -> (
    AsyncBoundEvent[P, S, T, C, R]
    | Callable[[EventConfig[P, S, T]], AsyncBoundEvent[P, S, T, C, R]]
):
    """Bind an async emitter to an event, returning an ``AsyncBoundEvent`` or a decorator."""
    if event is None:

        def decorator(evt: EventConfig[P, S, T]) -> AsyncBoundEvent[P, S, T, C, R]:
            return AsyncBoundEvent(emitter, evt, config=config)

        return decorator
    return AsyncBoundEvent(emitter, event, config=config)
