"""
Callable facades that bind an event definition to a concrete emitter and routing config.

{py:func}`bind` and {py:func}`abind` attach a sync or async emitter,
respectively, along with adapter-specific routing config, to an
{py:class}`EventConfig <stratae.events.event.EventConfig>`. Each returns a
{py:class}`BoundEvent` or {py:class}`AsyncBoundEvent`.

A bound event is a callable facade. Calling it constructs the event's
payload via its factory and forwards the payload, along with itself, to
the emitter, returning whatever the emitter produces.
{py:class}`BoundEvent` requires a sync factory, since there's no way to
await one from inside its synchronous `__call__`. {py:class}`AsyncBoundEvent`
accepts either a sync or async factory, and awaits its `__call__` either
way.

````{example} Binding an event to a generic emitter
```{code-block} python
from typing import Any
from stratae.events import EventConfig, Request, bind, event

class CreateOrder:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

class Order:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_created = event(CreateOrder, Request[Order])

# A generic emitter only needs to match EmitCallable's shape: payload,
# event, and config in, some result out. This one does the work directly,
# with no bus involved.
def emit(payload: CreateOrder, event: EventConfig[Any, Any, Any], config: None, *, serializer=None) -> Order:
    print(f"creating order {payload.order_id}")
    return Order(order_id=payload.order_id)

create_order = bind(emit, order_created, config=None)

created = create_order(order_id=42)
print(f"created order: {created.order_id}")
```
```{output}
creating order 42
created order: 42
```
````

See {py:func}`bind`, {py:func}`abind`, {py:class}`BoundEvent`, and
{py:class}`AsyncBoundEvent` for the rest of the module's API.

"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, cast

from stratae.events._typeguards import is_async_factory, is_sync_factory
from stratae.events.event import EventConfig, EventType
from stratae.events.protocols import EmitCallable

_SYNC_FACTORY_REQUIRED = "bind requires a sync factory; resolve async work outside the factory"


class BoundEvent[**P, S: Any, T: EventType, RoutingConfig: Any, Resp]:
    """
    Bind an EventConfig to a synchronous emitter with routing config.

    A `BoundEvent` acts as a callable facade: invoking it constructs a
    payload via the event's factory and forwards the payload and itself
    to `emitter`, returning whatever `emitter` produces. Requires a sync
    factory, since there's no way to await an async one from inside a
    sync `__call__`; use {py:class}`AsyncBoundEvent` for an async
    factory. `RoutingConfig` is the adapter-specific config attached at
    construction time.
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

        :param emitter: A callable that receives the constructed payload and
            this `BoundEvent`, and returns `Resp`.
        :param event: The {py:class}`EventConfig <stratae.events.event.EventConfig>`
            whose factory constructs the payload.
        :param config: The adapter-specific routing config for this binding.
        :param serializer: Encodes the payload before dispatch. Format is
            adapter-defined (bytes, a JSON string, etc.). When omitted,
            the emitter falls back to its own default serializer, if
            any.
        :raises TypeError: If `event`'s factory is async. A `BoundEvent`
            requires a sync factory since it cannot await one from
            inside its sync `__call__`.

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

        :returns: Whatever `self.emitter` returns.

        """
        return self.emitter(
            self._factory(*args, **kwargs), self.event, self.config, serializer=self.serializer
        )


class AsyncBoundEvent[**P, S: Any, T: EventType, RoutingConfig: Any, Resp]:
    """
    Bind an EventConfig to an asynchronous emitter with routing config.

    An `AsyncBoundEvent` acts as a callable facade: invoking it calls the
    event's factory to construct the payload, forwards the payload and
    itself to `emitter`, and awaits the result. Accepts either a sync or
    async factory; an async factory is awaited before its payload is
    forwarded to `emitter`. Use {py:class}`BoundEvent` for a synchronous
    emitter.
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

        :param emitter: A coroutine callable that receives the constructed
            payload and this `AsyncBoundEvent`, and returns an awaitable
            resolving to `Resp`.
        :param event: The {py:class}`EventConfig <stratae.events.event.EventConfig>`
            whose factory constructs the payload. May have a sync or
            async factory.
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
        factory = event.factory
        self._sync_factory: Callable[P, S] = cast(Callable[P, S], factory)
        self._async_factory: Callable[P, Awaitable[S]] | None = (
            factory if is_async_factory(factory) else None
        )

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Resp:
        """
        Construct the payload, forward it to the emitter, and await the result.

        Awaits the factory first when `event`'s factory is async, then
        awaits `self.emitter`'s coroutine.

        :returns: The resolved value of `self.emitter`'s coroutine.

        """
        if self._async_factory is not None:
            payload = await self._async_factory(*args, **kwargs)
        else:
            payload = self._sync_factory(*args, **kwargs)
        return await self.emitter(payload, self.event, self.config, serializer=self.serializer)


def bind[**P, S: Any, T: EventType, C, R](
    emitter: EmitCallable[P, S, T, C, R],
    event: EventConfig[P, S, T],
    *,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> BoundEvent[P, S, T, C, R]:
    """
    Bind a sync emitter to an event, returning a BoundEvent.

    :param emitter: A callable that receives the constructed payload and the
        resulting {py:class}`BoundEvent`, and returns `R`.
    :param event: The {py:class}`EventConfig <stratae.events.event.EventConfig>`
        to bind.
    :param config: The adapter-specific routing config for this binding.
    :param serializer: Encodes the payload before dispatch. Format is
        adapter-defined (bytes, a JSON string, etc.). When omitted, the
        emitter falls back to its own default serializer, if any.
    :returns: A {py:class}`BoundEvent` wrapping `emitter` and `event`.
    :raises TypeError: If `event`'s factory is async.
    """
    if not is_sync_factory(event.factory):
        raise TypeError(_SYNC_FACTORY_REQUIRED)
    return BoundEvent(emitter, event, config=config, serializer=serializer)


def abind[**P, S: Any, T: EventType, C, R](
    emitter: EmitCallable[P, S, T, C, Awaitable[R]],
    event: EventConfig[P, S, T],
    *,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> AsyncBoundEvent[P, S, T, C, R]:
    """
    Bind an async emitter to an event, returning an AsyncBoundEvent.

    :param emitter: A coroutine callable that receives the constructed
        payload and the resulting {py:class}`AsyncBoundEvent`, and
        returns an awaitable resolving to `R`.
    :param event: The {py:class}`EventConfig <stratae.events.event.EventConfig>`
        to bind. May have a sync or async factory; either way
        `AsyncBoundEvent.__call__` awaits it.
    :param config: The adapter-specific routing config for this binding.
    :param serializer: Encodes the payload before dispatch. Format is
        adapter-defined (bytes, a JSON string, etc.). When omitted, the
        emitter falls back to its own default serializer, if any.
    :returns: An {py:class}`AsyncBoundEvent` wrapping `emitter` and `event`.
    """
    return AsyncBoundEvent(emitter, event, config=config, serializer=serializer)
