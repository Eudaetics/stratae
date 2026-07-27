"""
Callable facades that bind an event definition to a concrete emitter and routing config.

{py:func}`bind` and {py:func}`abind` attach a sync or async emitter,
respectively, along with adapter-specific routing config, to an
{py:class}`Event <stratae.events.event.Event>`. Passed a `factory`, each
returns a {py:class}`FactoryBoundEvent` or {py:class}`AsyncFactoryBoundEvent`,
which build the payload from the bound call's arguments. Omitting `factory`
returns a {py:class}`BoundEvent` or {py:class}`AsyncBoundEvent` instead,
which forward an already-built payload straight through.

A bound event is a callable facade. The factory variants construct the
event's payload via `factory` and forward the payload, along with
themselves, to the emitter, returning whatever the emitter produces. The
no-factory variants take an already-built payload directly and forward it
the same way. `BoundEvent` and `FactoryBoundEvent` require a sync factory,
since there's no way to await one from inside their synchronous `__call__`.
`AsyncBoundEvent` and `AsyncFactoryBoundEvent` accept either a sync or async
factory, and await their `__call__` either way.

````{example} Binding an event to a generic emitter
```{code-block} python
from typing import Any, Callable
from stratae.events import Event, Request, bind

class CreateOrder:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

class Order:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_created = Event(CreateOrder, Request[Order])

# A generic emitter only needs to match EmitCallable's shape: payload,
# event, and config in, some result out. This one does the work
# directly, using config as the destination label to route to.
def emit(
    payload: CreateOrder,
    event: Event[CreateOrder, Request[Order]],
    config: str,
    *,
    serializer: Callable[[CreateOrder], Any] | None = None,
) -> Order:
    print(f"[{config}] creating order {payload.order_id}")
    return Order(order_id=payload.order_id)

create_order = bind(emit, order_created, factory=CreateOrder, config="orders")

created = create_order(order_id=42)
print(f"created order: {created.order_id}")
```
```{output}
[orders] creating order 42
created order: 42
```
````

See {py:func}`bind`, {py:func}`abind`, {py:class}`BoundEvent`,
{py:class}`FactoryBoundEvent`, {py:class}`AsyncBoundEvent`, and
{py:class}`AsyncFactoryBoundEvent` for the rest of the module's API.

"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, cast, overload

from stratae.events._typeguards import is_async_factory, is_sync_factory
from stratae.events.event import DispatchPattern, Event
from stratae.events.protocols import EmitCallable

_SYNC_FACTORY_REQUIRED = "bind requires a sync factory; resolve async work outside the factory"


class BoundEvent[S: Any, T: DispatchPattern, RoutingConfig: Any, Resp]:
    """
    Bind an Event directly to a synchronous emitter, with no factory.

    A `BoundEvent` acts as a callable facade: invoking it takes an
    already-built payload and forwards it, along with itself, to `emitter`,
    returning whatever `emitter` produces. Use this when the caller already
    has a constructed payload; use {py:class}`FactoryBoundEvent` when a
    factory should build the payload from the call's arguments instead.
    `RoutingConfig` is the adapter-specific config attached at construction
    time.
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

        :param emitter: A callable that receives the payload and this
            `BoundEvent`, and returns `Resp`.
        :param event: The {py:class}`Event <stratae.events.event.Event>`
            this binding delivers.
        :param config: The adapter-specific routing config for this binding.
        :param serializer: Encodes the payload before dispatch. Format is
            adapter-defined (bytes, a JSON string, etc.). When omitted,
            the emitter falls back to its own default serializer, if
            any.

        """
        self.emitter = emitter
        self.event = event
        self.config = config
        self.serializer = serializer

    def __call__(self, payload: S) -> Resp:
        """
        Forward an already-built payload to the emitter.

        :returns: Whatever `self.emitter` returns.

        """
        return self.emitter(payload, self.event, self.config, serializer=self.serializer)


class FactoryBoundEvent[**P, S: Any, T: DispatchPattern, RoutingConfig: Any, Resp]:
    """
    Bind an Event and a factory to a synchronous emitter with routing config.

    A `FactoryBoundEvent` acts as a callable facade: invoking it constructs
    a payload via `factory` and forwards the payload, along with itself, to
    `emitter`, returning whatever `emitter` produces. Requires a sync
    factory, since there's no way to await an async one from inside a sync
    `__call__`; use {py:class}`AsyncFactoryBoundEvent` for an async factory.
    `RoutingConfig` is the adapter-specific config attached at construction
    time.
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

        :param emitter: A callable that receives the constructed payload and
            this `FactoryBoundEvent`, and returns `Resp`.
        :param event: The {py:class}`Event <stratae.events.event.Event>`
            this binding delivers.
        :param factory: Builds the payload from the bound call's arguments.
        :param config: The adapter-specific routing config for this binding.
        :param serializer: Encodes the payload before dispatch. Format is
            adapter-defined (bytes, a JSON string, etc.). When omitted,
            the emitter falls back to its own default serializer, if
            any.
        :raises TypeError: If `factory` is async. A `FactoryBoundEvent`
            requires a sync factory since it cannot await one from inside
            its sync `__call__`.

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

        :returns: Whatever `self.emitter` returns.

        """
        return self.emitter(
            self._factory(*args, **kwargs), self.event, self.config, serializer=self.serializer
        )


class AsyncBoundEvent[S: Any, T: DispatchPattern, RoutingConfig: Any, Resp]:
    """
    Bind an Event directly to an asynchronous emitter, with no factory.

    An `AsyncBoundEvent` acts as a callable facade: invoking it takes an
    already-built payload and forwards it, along with itself, to `emitter`,
    awaiting the resulting coroutine. Use this when the caller already has
    a constructed payload rather than the raw arguments to build one; use
    {py:class}`AsyncFactoryBoundEvent` when a factory should build the
    payload from the call's arguments instead.
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

        :param emitter: A coroutine callable that receives the payload and
            this `AsyncBoundEvent`, and returns an awaitable resolving to
            `Resp`.
        :param event: The {py:class}`Event <stratae.events.event.Event>`
            this binding delivers.
        :param config: The adapter-specific routing config for this binding.
        :param serializer: Encodes the payload before dispatch. Format is
            adapter-defined (bytes, a JSON string, etc.). When omitted,
            the emitter falls back to its own default serializer, if
            any.

        """
        self.event = event
        self.emitter = emitter
        self.config = config
        self.serializer = serializer

    async def __call__(self, payload: S) -> Resp:
        """
        Forward an already-built payload to the emitter and await the result.

        :returns: The resolved value of `self.emitter`'s coroutine.

        """
        return await self.emitter(payload, self.event, self.config, serializer=self.serializer)


class AsyncFactoryBoundEvent[**P, S: Any, T: DispatchPattern, RoutingConfig: Any, Resp]:
    """
    Bind an Event and a factory to an asynchronous emitter with routing config.

    An `AsyncFactoryBoundEvent` acts as a callable facade: invoking it calls
    `factory`, sync or async, with the supplied arguments to construct the
    payload, forwards it and itself to `emitter`, and awaits the resulting
    coroutine.
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

        :param emitter: A coroutine callable that receives the constructed
            payload and this `AsyncFactoryBoundEvent`, and returns an
            awaitable resolving to `Resp`.
        :param event: The {py:class}`Event <stratae.events.event.Event>`
            this binding delivers.
        :param factory: Builds the payload from the bound call's arguments,
            sync or async.
        :param config: The adapter-specific routing config for this binding.
        :param serializer: Encodes the payload before dispatch. Format is
            adapter-defined (bytes, a JSON string, etc.). When omitted,
            the emitter falls back to its own default serializer, if
            any.

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

        Awaits the factory first when it's async, then awaits
        `self.emitter`'s coroutine.

        :returns: The resolved value of `self.emitter`'s coroutine.

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
    """
    Bind a sync emitter to an event, with a factory, or without one for passthrough.

    :param emitter: A callable that receives the payload and the resulting
        bound event, and returns `R`.
    :param event: The {py:class}`Event <stratae.events.event.Event>` to bind.
    :param factory: Builds the payload from the bound call's arguments. When
        omitted, the returned facade instead forwards an already-built
        payload straight through.
    :param config: The adapter-specific routing config for this binding.
    :param serializer: Encodes the payload before dispatch. Format is
        adapter-defined (bytes, a JSON string, etc.). When omitted, the
        emitter falls back to its own default serializer, if any.
    :returns: A {py:class}`FactoryBoundEvent` when `factory` is given,
        otherwise a {py:class}`BoundEvent`.
    :raises TypeError: If `factory` is given and is async.

    """
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
    """
    Bind an async emitter to an event, with a factory, or without one for passthrough.

    :param emitter: A coroutine callable that receives the payload and the
        resulting bound event, and returns an awaitable resolving to `R`.
    :param event: The {py:class}`Event <stratae.events.event.Event>` to
        bind.
    :param factory: Builds the payload from the bound call's arguments, sync
        or async. When omitted, the returned facade instead forwards an
        already-built payload straight through.
    :param config: The adapter-specific routing config for this binding.
    :param serializer: Encodes the payload before dispatch. Format is
        adapter-defined (bytes, a JSON string, etc.). When omitted, the
        emitter falls back to its own default serializer, if any.
    :returns: An {py:class}`AsyncFactoryBoundEvent` when `factory` is given,
        otherwise an {py:class}`AsyncBoundEvent`.

    """
    if factory is None:
        return AsyncBoundEvent(emitter, event, config=config, serializer=serializer)
    return AsyncFactoryBoundEvent(
        emitter, event, factory=factory, config=config, serializer=serializer
    )
