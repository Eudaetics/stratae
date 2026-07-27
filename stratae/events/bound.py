"""Bound event facades for synchronous and asynchronous bus bindings."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, cast, overload

from stratae.events._typeguards import is_async_factory, is_sync_factory
from stratae.events.event import DispatchPattern, Event
from stratae.events.protocols import EmitCallable

_SYNC_FACTORY_REQUIRED = "bind requires a sync factory; resolve async work outside the factory"


class BoundEvent[S: Any, T: DispatchPattern, RoutingConfig: Any, Resp]:
    """
    Binds an ``Event`` directly to a synchronous emitter, with no factory.

    A ``BoundEvent`` acts as a callable facade: invoking it takes an
    already-built payload and forwards it and configuration settings
    to the emitter. Use this for passing instances of the payload directly.
    Use ``FactoryBoundEvent`` when a factory should build the payload
    from the call's arguments.

    Type parameters:
        S:             The payload type.
        T:             The ``DispatchPattern`` discriminant of the bound event.
        RoutingConfig: The adapter-specific routing config type.
        Resp:          The return type produced by the emitter.
    """

    __slots__ = ("emitter", "event", "config", "serializer")

    def __init__(
        self,
        emitter: EmitCallable[S, T, RoutingConfig, Resp],
        event: Event[S, T],
        *,
        config: RoutingConfig,
        serializer: Callable[[S], Any] | None = None,
    ) -> None:
        """
        Bind an event and emitter with routing config, with no factory.

        Args:
            emitter:    A callable that receives the payload and this
                        ``BoundEvent``, and returns ``Resp``.
            event:      The ``Event`` this binding delivers.
            config:     The adapter-specific routing config for this binding.
            serializer: Encodes payload before dispatch. Format is
                        adapter-defined (bytes, a JSON string, etc.) — when
                        omitted, the emitter falls back to its own default
                        serializer, if any.

        """
        self.emitter = emitter
        self.event = event
        self.config = config
        self.serializer = serializer

    def __call__(self, payload: S) -> Resp:
        """
        Forward an already-built payload to the emitter.

        Returns:
            Whatever ``self.emitter`` returns.

        """
        return self.emitter(payload, self.event, self.config, serializer=self.serializer)


class FactoryBoundEvent[**P, S: Any, T: DispatchPattern, RoutingConfig: Any, Resp]:
    """
    Binds an ``Event`` and a factory to a synchronous emitter with routing config.

    A ``FactoryBoundEvent`` acts as a callable facade: invoking it constructs
    a payload via ``factory`` and forwards the payload and configuration
    information to the emitter, returning whatever the emitter produces.

    Type parameters:
        P:             The parameter specification of the bound event's ``__call__`` signature.
        S:             The payload type produced by ``factory``.
        T:             The ``DispatchPattern`` discriminant of the bound event.
        RoutingConfig: The adapter-specific routing config type.
        Resp:          The return type produced by the emitter.
    """

    __slots__ = ("emitter", "event", "config", "serializer", "_factory")

    def __init__(
        self,
        emitter: EmitCallable[S, T, RoutingConfig, Resp],
        event: Event[S, T],
        *,
        factory: Callable[P, S],
        config: RoutingConfig,
        serializer: Callable[[S], Any] | None = None,
    ) -> None:
        """
        Bind an event, factory, and emitter with routing config.

        Args:
            emitter:    A callable that receives the constructed payload and this
                        ``FactoryBoundEvent``, and returns ``Resp``.
            event:      The ``Event`` this binding delivers.
            factory:    Builds the payload from the bound call's arguments.
            config:     The adapter-specific routing config for this binding.
            serializer: Encodes payload before dispatch. Format is
                        adapter-defined (bytes, a JSON string, etc.) — when
                        omitted, the emitter falls back to its own default
                        serializer, if any.

        """
        if not is_sync_factory(factory):
            raise TypeError(_SYNC_FACTORY_REQUIRED)
        self.emitter = emitter
        self.event = event
        self.config = config
        self.serializer = serializer
        self._factory = factory

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the payload and forward it to the emitter.

        Returns:
            Whatever ``self.emitter`` returns.

        """
        return self.emitter(
            self._factory(*args, **kwargs), self.event, self.config, serializer=self.serializer
        )


class AsyncBoundEvent[S: Any, T: DispatchPattern, RoutingConfig: Any, Resp]:
    """
    Binds an ``Event`` directly to an asynchronous emitter, with no factory.

    An ``AsyncBoundEvent`` acts as a callable facade: invoking it takes an
    already-built payload and forwards it and settings to ``emitter``,
    awaiting the resulting coroutine. Use this when the caller already has
    a constructed payload rather than the raw arguments to build one; use
    ``AsyncFactoryBoundEvent`` when a factory should build the payload from
    the call's arguments.

    Type parameters:
        S:             The payload type.
        T:             The ``DispatchPattern`` discriminant of the bound event.
        RoutingConfig: The adapter-specific routing config type.
        Resp:          The type that the emitter's coroutine resolves to.
    """

    __slots__ = ("event", "emitter", "config", "serializer")

    def __init__(
        self,
        emitter: EmitCallable[S, T, RoutingConfig, Awaitable[Resp]],
        event: Event[S, T],
        *,
        config: RoutingConfig,
        serializer: Callable[[S], Any] | None = None,
    ) -> None:
        """
        Bind an event and async emitter with routing config, with no factory.

        Args:
            emitter:    A coroutine callable that receives the payload and
                        this ``AsyncBoundEvent``, and returns an awaitable
                        resolving to ``Resp``.
            event:      The ``Event`` this binding delivers.
            config:     The adapter-specific routing config for this binding.
            serializer: Encodes payload before dispatch. Format is
                        adapter-defined (bytes, a JSON string, etc.) — when
                        omitted, the emitter falls back to its own default
                        serializer, if any.

        """
        self.event = event
        self.emitter = emitter
        self.config = config
        self.serializer = serializer

    async def __call__(self, payload: S) -> Resp:
        """
        Forward an already-built payload to the emitter and await the result.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        return await self.emitter(payload, self.event, self.config, serializer=self.serializer)


class AsyncFactoryBoundEvent[**P, S: Any, T: DispatchPattern, RoutingConfig: Any, Resp]:
    """
    Binds an ``Event`` and a factory to an asynchronous emitter with routing config.

    An ``AsyncFactoryBoundEvent`` acts as a callable facade: invoking it
    calls ``factory``, sync or async, with the supplied arguments to
    construct the payload, forwards it and settings to ``emitter``, and
    awaits the resulting coroutine.

    Type parameters:
        P:             The parameter specification of the bound event's ``__call__`` signature.
        S:             The payload type produced by ``factory``.
        T:             The ``DispatchPattern`` discriminant of the bound event.
        RoutingConfig: The adapter-specific routing config type.
        Resp:          The type that the emitter's coroutine resolves to.
    """

    __slots__ = ("event", "emitter", "config", "serializer", "_sync_factory", "_async_factory")

    def __init__(
        self,
        emitter: EmitCallable[S, T, RoutingConfig, Awaitable[Resp]],
        event: Event[S, T],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
        config: RoutingConfig,
        serializer: Callable[[S], Any] | None = None,
    ) -> None:
        """
        Bind an event, factory, and async emitter with routing config.

        Args:
            emitter:    A coroutine callable that receives the constructed payload
                        and this ``AsyncFactoryBoundEvent``, and returns an awaitable
                        resolving to ``Resp``.
            event:      The ``Event`` this binding delivers.
            factory:    Builds the payload from the bound call's arguments,
                        sync or async.
            config:     The adapter-specific routing config for this binding.
            serializer: Encodes payload before dispatch. Format is
                        adapter-defined (bytes, a JSON string, etc.) — when
                        omitted, the emitter falls back to its own default
                        serializer, if any.

        """
        self.event = event
        self.emitter = emitter
        self.config = config
        self.serializer = serializer
        self._sync_factory: Callable[P, S] = cast(Callable[P, S], factory)
        self._async_factory: Callable[P, Awaitable[S]] | None = (
            factory if is_async_factory(factory) else None
        )

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the payload, forward it to the emitter, and await the result.

        Returns:
            The resolved value of ``self.emitter``'s coroutine.

        """
        if self._async_factory is not None:
            payload = await self._async_factory(*args, **kwargs)
        else:
            payload = self._sync_factory(*args, **kwargs)
        return await self.emitter(payload, self.event, self.config, serializer=self.serializer)


@overload
def bind[**P, S: Any, T: DispatchPattern, C, R](
    emitter: EmitCallable[S, T, C, R],
    event: Event[S, T],
    *,
    factory: Callable[P, S],
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> FactoryBoundEvent[P, S, T, C, R]: ...


@overload
def bind[S: Any, T: DispatchPattern, C, R](
    emitter: EmitCallable[S, T, C, R],
    event: Event[S, T],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> BoundEvent[S, T, C, R]: ...


def bind(
    emitter: EmitCallable[Any, Any, Any, Any],
    event: Event[Any, Any],
    *,
    factory: Callable[..., Any] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
) -> FactoryBoundEvent[Any, Any, Any, Any, Any] | BoundEvent[Any, Any, Any, Any]:
    """Bind an emitter to an ``Event``, with a factory, or without one for passthrough."""
    if factory is None:
        return BoundEvent(emitter, event, config=config, serializer=serializer)
    return FactoryBoundEvent(emitter, event, factory=factory, config=config, serializer=serializer)


@overload
def abind[**P, S: Any, T: DispatchPattern, C, R](
    emitter: EmitCallable[S, T, C, Awaitable[R]],
    event: Event[S, T],
    *,
    factory: Callable[P, S] | Callable[P, Awaitable[S]],
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> AsyncFactoryBoundEvent[P, S, T, C, R]: ...


@overload
def abind[S: Any, T: DispatchPattern, C, R](
    emitter: EmitCallable[S, T, C, Awaitable[R]],
    event: Event[S, T],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> AsyncBoundEvent[S, T, C, R]: ...


def abind(
    emitter: EmitCallable[Any, Any, Any, Awaitable[Any]],
    event: Event[Any, Any],
    *,
    factory: Callable[..., Any] | Callable[..., Awaitable[Any]] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
) -> AsyncFactoryBoundEvent[Any, Any, Any, Any, Any] | AsyncBoundEvent[Any, Any, Any, Any]:
    """Bind an async emitter to an event, with a factory, or without one for passthrough."""
    if factory is None:
        return AsyncBoundEvent(emitter, event, config=config, serializer=serializer)
    return AsyncFactoryBoundEvent(
        emitter, event, factory=factory, config=config, serializer=serializer
    )
