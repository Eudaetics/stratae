"""Bound event facades for synchronous and asynchronous bus bindings."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Protocol, cast, overload

from stratae.events._typeguards import is_async_factory, is_sync_factory
from stratae.events.event import EventConfig, EventType
from stratae.events.protocols import EmitCallable

_SYNC_FACTORY_REQUIRED = "bind requires a sync factory; resolve async work outside the factory"


class BoundEvent[**P, S: Any, T: EventType, RoutingConfig: Any, Resp]:
    """
    Binds an ``EventConfig`` to a synchronous emitter with routing config.

    A ``BoundEvent`` acts as a callable façade: invoking it constructs a
    payload via the event's factory and forwards the payload and itself to
    ``emitter``, returning whatever the emitter produces.

    Type parameters:
        P:             The parameter specification of the bound event's ``__call__`` signature.
        S:             The payload type produced by the event's factory.
        T:             The ``EventType`` discriminant of the bound event.
        RoutingConfig: The adapter-specific routing config type.
        Resp:          The return type produced by the emitter.
    """

    __slots__ = ("emitter", "event", "config", "serializer", "_factory")

    def __init__(
        self,
        emitter: EmitCallable[P, S, T, RoutingConfig, Resp],
        event: EventConfig[P, S, T],
        *,
        config: RoutingConfig,
        serializer: Callable[[S], Any] | None = None,
    ) -> None:
        """
        Bind an event and emitter with routing config.

        Args:
            emitter:    A callable that receives the constructed payload and this
                        ``BoundEvent``, and returns ``Resp``.
            event:      The ``EventConfig`` whose factory constructs the payload.
            config:     The adapter-specific routing config for this binding.
            serializer: Encodes payload before dispatch. Format is
                        adapter-defined (bytes, a JSON string, etc.). When
                        omitted, the emitter falls back to its own default
                        serializer, if any.

        """
        if not is_sync_factory(event.factory):
            raise TypeError(_SYNC_FACTORY_REQUIRED)
        self.emitter = emitter
        self.event = event
        self.config = config
        self.serializer = serializer
        self._factory = event.factory

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the payload and forward it to the emitter.

        Returns:
            Whatever ``self.emitter`` returns.

        """
        return self.emitter(
            self._factory(*args, **kwargs), self.event, self.config, serializer=self.serializer
        )


class AsyncBoundEvent[**P, S: Any, T: EventType, RoutingConfig: Any, Resp]:
    """
    Binds an ``EventConfig`` to an asynchronous emitter with routing config.

    An ``AsyncBoundEvent`` acts as a callable façade: invoking it calls ``factory``
    with the supplied arguments to construct the payload, forwards it and itself
    to ``emitter``, and awaits the resulting coroutine.

    Type parameters:
        P:             The parameter specification of the bound event's ``__call__`` signature.
        RoutingConfig: The adapter-specific routing config type.
        Resp:          The type that the emitter's coroutine resolves to.
    """

    __slots__ = ("event", "emitter", "config", "serializer", "_sync_factory", "_async_factory")

    def __init__(
        self,
        emitter: EmitCallable[P, S, T, RoutingConfig, Awaitable[Resp]],
        event: EventConfig[P, S, T],
        *,
        config: RoutingConfig,
        serializer: Callable[[S], Any] | None = None,
    ) -> None:
        """
        Bind a factory and async emitter with routing config.

        Args:
            emitter:    A coroutine callable that receives the constructed payload
                        and this ``AsyncBoundEvent``, and returns an awaitable
                        resolving to ``Resp``.
            event:      The ``EventConfig`` whose factory constructs the payload.
            config:     The adapter-specific routing config for this binding.
            serializer: Encodes payload before dispatch. Format is
                        adapter-defined (bytes, a JSON string, etc.). When
                        omitted, the emitter falls back to its own default
                        serializer, if any.

        """
        self.event = event
        self.emitter = emitter
        self.config = config
        self.serializer = serializer
        factory = event.factory
        self._sync_factory: Callable[P, S] = cast(Callable[P, S], factory)
        self._async_factory: Callable[P, Awaitable[S]] | None = (
            factory if is_async_factory(factory) else None
        )

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the schema instance, forward it to the emitter, and await the result.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        if self._async_factory is not None:
            payload = await self._async_factory(*args, **kwargs)
        else:
            payload = self._sync_factory(*args, **kwargs)
        return await self.emitter(payload, self.event, self.config, serializer=self.serializer)


class _BindDecorator[C, R](Protocol):
    """Decorator form of ``bind``: takes an ``EventConfig``, returns a ``BoundEvent``."""

    def __call__[**P, S: Any, T: EventType](
        self, event: EventConfig[P, S, T]
    ) -> BoundEvent[P, S, T, C, R]:
        """Bind the emitter and config to ``event``, returning a ``BoundEvent``."""
        ...


@overload
def bind[**P, S: Any, T: EventType, C, R](
    emitter: EmitCallable[P, S, T, C, R],
    event: EventConfig[P, S, T],
    *,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> BoundEvent[P, S, T, C, R]: ...


@overload
def bind[**P, S: Any, T: EventType, C, R](
    emitter: EmitCallable[P, S, T, C, R], *, config: C, serializer: Callable[[S], Any] | None = None
) -> _BindDecorator[C, R]: ...


def bind[**P, S: Any, T: EventType, C, R](
    emitter: EmitCallable[P, S, T, C, R],
    event: EventConfig[P, S, T] | None = None,
    *,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> BoundEvent[P, S, T, C, R] | Callable[[EventConfig[P, S, T]], BoundEvent[P, S, T, C, R]]:
    """Bind an emitter to an ``EventConfig``, returning a ``BoundEvent`` or a decorator."""
    if event is None:

        def decorator(evt: EventConfig[P, S, T]) -> BoundEvent[P, S, T, C, R]:
            if not is_sync_factory(evt.factory):
                raise TypeError(_SYNC_FACTORY_REQUIRED)
            return BoundEvent(emitter, evt, config=config, serializer=serializer)

        return decorator
    if not is_sync_factory(event.factory):
        raise TypeError(_SYNC_FACTORY_REQUIRED)
    return BoundEvent(emitter, event, config=config, serializer=serializer)


class _ABindDecorator[C, R](Protocol):
    """Return type of the decorator form of ``abind``: async emitter+config awaiting an Event."""

    def __call__[**P, S: Any, T: EventType](
        self, event: EventConfig[P, S, T]
    ) -> AsyncBoundEvent[P, S, T, C, R]:
        """Bind the async emitter and config to ``event``, returning an ``AsyncBoundEvent``."""
        ...


@overload
def abind[**P, S: Any, T: EventType, C, R](
    emitter: EmitCallable[P, S, T, C, Awaitable[R]],
    event: EventConfig[P, S, T],
    *,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> AsyncBoundEvent[P, S, T, C, R]: ...


@overload
def abind[**P, S: Any, T: EventType, C, R](
    emitter: EmitCallable[P, S, T, C, Awaitable[R]],
    *,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> _ABindDecorator[C, R]: ...


def abind[**P, S: Any, T: EventType, C, R](
    emitter: EmitCallable[P, S, T, C, Awaitable[R]],
    event: EventConfig[P, S, T] | None = None,
    *,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> (
    AsyncBoundEvent[P, S, T, C, R]
    | Callable[[EventConfig[P, S, T]], AsyncBoundEvent[P, S, T, C, R]]
):
    """Bind an async emitter to an event, returning an ``AsyncBoundEvent`` or a decorator."""
    if event is None:

        def decorator(evt: EventConfig[P, S, T]) -> AsyncBoundEvent[P, S, T, C, R]:
            return AsyncBoundEvent(emitter, evt, config=config, serializer=serializer)

        return decorator
    return AsyncBoundEvent(emitter, event, config=config, serializer=serializer)
