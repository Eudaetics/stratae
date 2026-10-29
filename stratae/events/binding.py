"""
Callable facades that bind an event definition to a concrete emitter and routing config.

{py:func}`bind` and {py:func}`abind` bind an event to a sync or async emitter and its
routing config. With a `factory`, the result builds the payload from its arguments; without
one, it takes a built payload, or nothing for a payload-less event. {py:class}`BindMixin`
and {py:class}`AsyncBindMixin` give an adapter its own typed `bind` method.

````{example} Binding an event to a generic emitter
```{code-block} python
from typing import Any, Callable
from stratae.events import EventType, Hooks, Request, bind

class CreateOrder:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

class Order:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_created = Request(CreateOrder, reply=Order)

def emit(
    event: EventType[CreateOrder, Any, Any, Any],
    config: str,
    payload: CreateOrder,
    *,
    serializer: Callable[[CreateOrder], Any] | None = None,
    hooks: Hooks[CreateOrder, Order] | None = None,
) -> Order:
    print(f"[{config}] creating order {payload.order_id}")
    return Order(order_id=payload.order_id)

create_order = bind(order_created, emit, factory=CreateOrder, config="orders")

created = create_order(order_id=42)
print(f"created order: {created.order_id}")
```
```{output}
[orders] creating order 42
created order: 42
```
````
"""

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Literal, overload

from stratae.events._typeguards import is_async_factory, is_sync_factory
from stratae.events.hooks import Hooks
from stratae.events.patterns import NoPayload
from stratae.events.protocols import (
    AnyEvent,
    EmitCallable,
    EventType,
    PayloadEvent,
    SignalEvent,
)

_SYNC_FACTORY_REQUIRED = "bind requires a sync factory; resolve async work outside the factory"

_NO_PAYLOAD = NoPayload()
"""What a payload-less event's bound callable passes as its payload."""


def _emit_options(
    serializer: Callable[[Any], Any] | None, hooks: Hooks[Any, Any] | None
) -> dict[str, Any]:
    """
    Return the keyword arguments a bound callable passes to its emitter.

    `hooks` is passed only when given, so an emitter that doesn't support hooks still works
    until someone binds with them, then refuses loudly.
    """
    if hooks is None:
        return {"serializer": serializer}
    return {"serializer": serializer, "hooks": hooks}


@overload
def bind[**P, S, C, R, E](
    event: PayloadEvent[S, E, Any],
    emitter: EmitCallable[S, C, R, Literal[False]],
    *,
    factory: Callable[P, S],
    config: C,
    serializer: Callable[[S], Any] | None = None,
    hooks: Hooks[S, E] | None = None,
) -> Callable[P, R]: ...


@overload
def bind[C, R, E](
    event: SignalEvent[E, Any],
    emitter: EmitCallable[NoPayload, C, R, Literal[True]],
    *,
    factory: None = None,
    config: C,
    serializer: None = None,
    hooks: Hooks[NoPayload, E] | None = None,
) -> Callable[[], R]: ...


@overload
def bind[S, C, R, E](
    event: PayloadEvent[S, E, Any],
    emitter: EmitCallable[S, C, R, Literal[False]],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
    hooks: Hooks[S, E] | None = None,
) -> Callable[[S], R]: ...


def bind(
    event: AnyEvent,
    emitter: EmitCallable[Any, Any, Any, Any],
    *,
    factory: Callable[..., Any] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
    hooks: Hooks[Any, Any] | None = None,
) -> Callable[..., Any]:
    """
    Bind an event to a sync emitter.

    :param event: The event to bind.
    :param emitter: Called with the event, config, and payload.
    :param factory: A sync callable building the payload from the call's arguments. If
        omitted, the payload is passed directly.
    :param config: The adapter's routing config.
    :param serializer: Encodes the payload; defaults to the emitter's own.
    :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit,
        passed to `emitter` only when given.
    :returns: A callable that dispatches through `emitter`.
    :raises TypeError: If `factory` is async.
    """
    options = _emit_options(serializer, hooks)
    if factory is None:

        def passthrough(payload: Any = _NO_PAYLOAD) -> Any:
            return emitter(event, config, payload, **options)

        return passthrough

    if not is_sync_factory(factory):
        raise TypeError(_SYNC_FACTORY_REQUIRED)

    def construct(*args: Any, **kwargs: Any) -> Any:
        return emitter(event, config, factory(*args, **kwargs), **options)

    return construct


@overload
def abind[**P, S, C, R, E](
    event: PayloadEvent[S, E, Any],
    emitter: EmitCallable[S, C, Awaitable[R], Literal[False]],
    *,
    factory: Callable[P, S] | Callable[P, Awaitable[S]],
    config: C,
    serializer: Callable[[S], Any] | None = None,
    hooks: Hooks[S, E] | None = None,
) -> Callable[P, Awaitable[R]]: ...


@overload
def abind[C, R, E](
    event: SignalEvent[E, Any],
    emitter: EmitCallable[NoPayload, C, Awaitable[R], Literal[True]],
    *,
    factory: None = None,
    config: C,
    serializer: None = None,
    hooks: Hooks[NoPayload, E] | None = None,
) -> Callable[[], Awaitable[R]]: ...


@overload
def abind[S, C, R, E](
    event: PayloadEvent[S, E, Any],
    emitter: EmitCallable[S, C, Awaitable[R], Literal[False]],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
    hooks: Hooks[S, E] | None = None,
) -> Callable[[S], Awaitable[R]]: ...


def abind(
    event: AnyEvent,
    emitter: EmitCallable[Any, Any, Awaitable[Any], Any],
    *,
    factory: Callable[..., Any] | Callable[..., Awaitable[Any]] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
    hooks: Hooks[Any, Any] | None = None,
) -> Callable[..., Awaitable[Any]]:
    """
    Bind an event to an async emitter.

    :param event: The event to bind.
    :param emitter: An async callable taking the event, config, and payload.
    :param factory: A sync or async callable building the payload from the call's
        arguments. If omitted, the payload is passed directly.
    :param config: The adapter's routing config.
    :param serializer: Encodes the payload; defaults to the emitter's own.
    :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit.
    :returns: An async callable that dispatches through `emitter`.
    """
    options = _emit_options(serializer, hooks)
    if factory is None:

        async def passthrough(payload: Any = _NO_PAYLOAD) -> Any:
            return await emitter(event, config, payload, **options)

        return passthrough

    async_factory = factory if is_async_factory(factory) else None

    async def construct(*args: Any, **kwargs: Any) -> Any:
        if async_factory is not None:
            payload = await async_factory(*args, **kwargs)
        else:
            payload = factory(*args, **kwargs)
        return await emitter(event, config, payload, **options)

    return construct


class BindMixin[C](ABC):
    """
    Give a sync adapter a typed `bind` method wrapping its `emit`.

    `C` is the adapter's config type; with `BindMixin[None]`, `config` can be omitted.
    """

    __slots__ = ()

    @abstractmethod
    def emit[S, R](
        self,
        event: EventType[S, R, Any, Any],
        config: C,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> R:
        """
        Dispatch a payload.

        See {py:meth}`Producer.emit <stratae.events.protocols.Producer.emit>`.
        """
        ...

    @abstractmethod
    def _check_event(self, event: AnyEvent, hooks: Hooks[Any, Any] | None = None) -> None:
        """
        Raise if this adapter can't emit the event's pattern, or run the given hooks.

        Called by `bind`, so an unsupported event is refused before anything emits it.
        """
        ...

    @overload
    def bind[**P, S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: Callable[P, S],
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[P, R]: ...
    @overload
    def bind[R](
        self,
        event: SignalEvent[R, Any],
        *,
        factory: None = None,
        config: C = ...,
        serializer: None = None,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> Callable[[], R]: ...
    @overload
    def bind[S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: None = None,
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[[S], R]: ...

    def bind(
        self,
        event: AnyEvent,
        *,
        factory: Callable[..., Any] | None = None,
        config: Any = None,
        serializer: Callable[[Any], Any] | None = None,
        hooks: Hooks[Any, Any] | None = None,
    ) -> Callable[..., Any]:
        """
        Bind an event to this adapter's `emit`. See {py:func}`bind`.

        :param event: The event to bind.
        :param factory: Builds the payload from the call's arguments.
        :param config: The adapter's routing config.
        :param serializer: Encodes the payload.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit.
        :returns: A callable that dispatches through this adapter.
        """
        self._check_event(event, hooks)
        return bind(
            event, self.emit, factory=factory, config=config, serializer=serializer, hooks=hooks
        )


class AsyncBindMixin[C](ABC):
    """
    Give an async adapter a typed `bind` method wrapping its `emit`.

    The async counterpart to {py:class}`BindMixin`.
    """

    __slots__ = ()

    @abstractmethod
    def emit[S, R](
        self,
        event: EventType[S, R, Any, Any],
        config: C,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> Awaitable[R]:
        """
        Dispatch a payload.

        See {py:meth}`Producer.emit <stratae.events.protocols.Producer.emit>`.
        """
        ...

    @abstractmethod
    def _check_event(self, event: AnyEvent, hooks: Hooks[Any, Any] | None = None) -> None:
        """
        Raise if this adapter can't emit the event's pattern, or run the given hooks.

        Called by `bind`, so an unsupported event is refused before anything emits it.
        """
        ...

    @overload
    def bind[**P, S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[P, Awaitable[R]]: ...
    @overload
    def bind[R](
        self,
        event: SignalEvent[R, Any],
        *,
        factory: None = None,
        config: C = ...,
        serializer: None = None,
        hooks: Hooks[NoPayload, R] | None = None,
    ) -> Callable[[], Awaitable[R]]: ...
    @overload
    def bind[S, R](
        self,
        event: PayloadEvent[S, R, Any],
        *,
        factory: None = None,
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
        hooks: Hooks[S, R] | None = None,
    ) -> Callable[[S], Awaitable[R]]: ...

    def bind(
        self,
        event: AnyEvent,
        *,
        factory: Callable[..., Any] | None = None,
        config: Any = None,
        serializer: Callable[[Any], Any] | None = None,
        hooks: Hooks[Any, Any] | None = None,
    ) -> Callable[..., Awaitable[Any]]:
        """
        Bind an event to this adapter's `emit`. See {py:func}`abind`.

        :param event: The event to bind.
        :param factory: Builds the payload from the call's arguments, sync or async.
        :param config: The adapter's routing config.
        :param serializer: Encodes the payload.
        :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit.
        :returns: An async callable that dispatches through this adapter.
        """
        self._check_event(event, hooks)
        return abind(
            event, self.emit, factory=factory, config=config, serializer=serializer, hooks=hooks
        )
