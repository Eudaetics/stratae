"""Bound event facades for synchronous and asynchronous bus bindings."""

from __future__ import annotations

import inspect
from typing import Any, Awaitable, Callable, Protocol, TypeGuard, overload

from stratae.events.event import EventConfig, EventType, Payload


class BoundEvent[**P, RoutingConfig: Any, Resp]:
    """
    Binds a ``Payload`` subclass to a synchronous emitter with routing config.

    A ``BoundEvent`` acts as a callable façade: invoking it constructs an
    instance of ``schema`` from the supplied arguments and forwards the
    payload and itself to ``emitter``, returning whatever the emitter produces.

    Type parameters:
        P:             The parameter specification of the bound event's ``__call__`` signature.
        RoutingConfig: The adapter-specific routing config type.
        Resp:          The return type produced by the emitter.
    """

    def __init__(
        self,
        emitter: Callable[[Payload, BoundEvent[P, RoutingConfig, Resp]], Resp],
        factory: Callable[P, Payload],
        *,
        config: RoutingConfig,
    ) -> None:
        """
        Bind a factory and emitter with routing config.

        Args:
            emitter: A callable that receives the constructed payload and this
                     ``BoundEvent``, and returns ``Resp``.
            factory: A callable that constructs the ``Payload`` from the arguments
                     passed to ``__call__``.
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


class AsyncBoundEvent[**P, RoutingConfig: Any, Resp]:
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
        emitter: Callable[[Payload, AsyncBoundEvent[P, RoutingConfig, Resp]], Awaitable[Resp]],
        factory: Callable[P, Payload] | Callable[P, Awaitable[Payload]],
        *,
        config: RoutingConfig,
    ) -> None:
        """
        Bind a factory and async emitter with routing config.

        Args:
            emitter: A coroutine callable that receives the constructed payload and this
                     ``AsyncBoundEvent``, and returns an awaitable resolving to ``Resp``.
            factory: A callable that constructs the ``Payload`` payload from
                     the arguments passed to ``__call__``. May be a coroutine function,
                     in which case it is awaited before the payload is forwarded.
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
        payload = self.factory(*args, **kwargs)
        if isinstance(payload, Awaitable):
            payload = await payload
        return await self.emitter(payload, self)


def _is_sync_factory[**P, E: Payload](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
) -> TypeGuard[Callable[P, E]]:
    return not inspect.iscoroutinefunction(factory)


class _BindDecorator[C, R](Protocol):
    """Decorator form of ``bind``: takes an ``EventConfig``, returns a ``BoundEvent``."""

    def __call__[**P, S: Payload, T: EventType](
        self, event: EventConfig[P, S, T]
    ) -> BoundEvent[P, C, R]:
        """Bind the emitter and config to ``event``, returning a ``BoundEvent``."""
        ...


@overload
def bind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, BoundEvent[P, C, R]], R],
    event: EventConfig[P, S, T],
    *,
    config: C,
) -> BoundEvent[P, C, R]: ...


@overload
def bind[**P, C, R](
    emitter: Callable[[Payload, BoundEvent[P, C, R]], R],
    *,
    config: C,
) -> _BindDecorator[C, R]: ...


def bind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, BoundEvent[P, C, R]], R],
    event: EventConfig[P, S, T] | None = None,
    *,
    config: C,
) -> BoundEvent[P, C, R] | Callable[[EventConfig[P, S, T]], BoundEvent[P, C, R]]:
    """Bind an emitter to an ``EventConfig``, returning a ``BoundEvent`` or a decorator."""
    if event is None:

        def decorator(evt: EventConfig[P, S, T]) -> BoundEvent[P, C, R]:
            if not _is_sync_factory(evt.factory):
                raise TypeError(
                    "bind requires a sync factory; resolve async work outside the factory"
                )
            return BoundEvent(emitter, evt.factory, config=config)

        return decorator
    if not _is_sync_factory(event.factory):
        raise TypeError("bind requires a sync factory; resolve async work outside the factory")
    return BoundEvent(emitter, event.factory, config=config)


class _ABindDecorator[C, R](Protocol):
    """Return type of the decorator form of ``abind``: async emitter+config awaiting an Event."""

    def __call__[**P, S: Payload, T: EventType](
        self, event: EventConfig[P, S, T]
    ) -> AsyncBoundEvent[P, C, R]:
        """Bind the async emitter and config to ``event``, returning an ``AsyncBoundEvent``."""
        ...


@overload
def abind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, AsyncBoundEvent[P, C, R]], Awaitable[R]],
    event: EventConfig[P, S, T],
    *,
    config: C,
) -> AsyncBoundEvent[P, C, R]: ...


@overload
def abind[**P, C, R](
    emitter: Callable[[Payload, AsyncBoundEvent[P, C, R]], Awaitable[R]],
    *,
    config: C,
) -> _ABindDecorator[C, R]: ...


def abind[**P, S: Payload, T: EventType, C, R](
    emitter: Callable[[Payload, AsyncBoundEvent[P, C, R]], Awaitable[R]],
    event: EventConfig[P, S, T] | None = None,
    *,
    config: C,
) -> AsyncBoundEvent[P, C, R] | Callable[[EventConfig[P, S, T]], AsyncBoundEvent[P, C, R]]:
    """Bind an async emitter to an event, returning an ``AsyncBoundEvent`` or a decorator."""
    if event is None:

        def decorator(evt: EventConfig[P, S, T]) -> AsyncBoundEvent[P, C, R]:
            return AsyncBoundEvent(emitter, evt.factory, config=config)

        return decorator
    return AsyncBoundEvent(emitter, event.factory, config=config)
