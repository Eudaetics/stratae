"""
Callable facades that bind an event definition to a concrete emitter and routing config.

{py:func}`bind` and {py:func}`abind` bind an {py:class}`Event <stratae.events.event.Event>`
to a sync or async emitter and its routing config. With a `factory`, the result builds the
payload from its arguments; without one, it takes a built payload, or nothing for a
`NoPayload` event. {py:class}`BindMixin` and {py:class}`AsyncBindMixin` give an adapter its
own typed `bind` method.

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

order_created = Event(Request[Order], CreateOrder)

def emit(
    event: Event[Request[Order], CreateOrder, Any],
    config: str,
    payload: CreateOrder,
    *,
    serializer: Callable[[CreateOrder], Any] | None = None,
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

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Literal, overload

from stratae.events._typeguards import is_async_factory, is_sync_factory
from stratae.events.event import DispatchPattern, Event, NoPayload
from stratae.events.protocols import EmitCallable

_SYNC_FACTORY_REQUIRED = "bind requires a sync factory; resolve async work outside the factory"


@overload
def bind[**P, T: DispatchPattern[Any, Any], S, C, R](
    event: Event[T, S, Literal[False]],
    emitter: EmitCallable[T, S, C, R, Literal[False]],
    *,
    factory: Callable[P, S],
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[P, R]: ...


@overload
def bind[T: DispatchPattern[Any, Any], C, R](
    event: Event[T, NoPayload, Literal[True]],
    emitter: EmitCallable[T, NoPayload, C, R, Literal[True]],
    *,
    factory: None = None,
    config: C,
    serializer: None = None,
) -> Callable[[], R]: ...


@overload
def bind[T: DispatchPattern[Any, Any], S, C, R](
    event: Event[T, S, Literal[False]],
    emitter: EmitCallable[T, S, C, R, Literal[False]],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[[S], R]: ...


def bind(
    event: Event[Any, Any, Any],
    emitter: EmitCallable[Any, Any, Any, Any, Any],
    *,
    factory: Callable[..., Any] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
) -> Callable[..., Any]:
    """
    Bind an event to a sync emitter.

    :param event: The {py:class}`Event <stratae.events.event.Event>` to bind.
    :param emitter: Called with the event, config, and payload.
    :param factory: A sync callable building the payload from the call's arguments. If
        omitted, the payload is passed directly.
    :param config: The adapter's routing config.
    :param serializer: Encodes the payload; defaults to the emitter's own.
    :returns: A callable that dispatches through `emitter`.
    :raises TypeError: If `factory` is async.
    """
    if factory is None:

        def passthrough(payload: Any = None) -> Any:
            return emitter(event, config, payload, serializer=serializer)

        return passthrough

    if not is_sync_factory(factory):
        raise TypeError(_SYNC_FACTORY_REQUIRED)

    def construct(*args: Any, **kwargs: Any) -> Any:
        return emitter(event, config, factory(*args, **kwargs), serializer=serializer)

    return construct


@overload
def abind[**P, T: DispatchPattern[Any, Any], S, C, R](
    event: Event[T, S, Literal[False]],
    emitter: EmitCallable[T, S, C, Awaitable[R], Literal[False]],
    *,
    factory: Callable[P, S] | Callable[P, Awaitable[S]],
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[P, Awaitable[R]]: ...


@overload
def abind[T: DispatchPattern[Any, Any], C, R](
    event: Event[T, NoPayload, Literal[True]],
    emitter: EmitCallable[T, NoPayload, C, Awaitable[R], Literal[True]],
    *,
    factory: None = None,
    config: C,
    serializer: None = None,
) -> Callable[[], Awaitable[R]]: ...


@overload
def abind[T: DispatchPattern[Any, Any], S, C, R](
    event: Event[T, S, Literal[False]],
    emitter: EmitCallable[T, S, C, Awaitable[R], Literal[False]],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[[S], Awaitable[R]]: ...


def abind(
    event: Event[Any, Any, Any],
    emitter: EmitCallable[Any, Any, Any, Awaitable[Any], Any],
    *,
    factory: Callable[..., Any] | Callable[..., Awaitable[Any]] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
) -> Callable[..., Awaitable[Any]]:
    """
    Bind an event to an async emitter.

    :param event: The {py:class}`Event <stratae.events.event.Event>` to bind.
    :param emitter: An async callable taking the event, config, and payload.
    :param factory: A sync or async callable building the payload from the call's
        arguments. If omitted, the payload is passed directly.
    :param config: The adapter's routing config.
    :param serializer: Encodes the payload; defaults to the emitter's own.
    :returns: An async callable that dispatches through `emitter`.
    """
    if factory is None:

        async def passthrough(payload: Any = None) -> Any:
            return await emitter(event, config, payload, serializer=serializer)

        return passthrough

    async_factory = factory if is_async_factory(factory) else None

    async def construct(*args: Any, **kwargs: Any) -> Any:
        if async_factory is not None:
            payload = await async_factory(*args, **kwargs)
        else:
            payload = factory(*args, **kwargs)
        return await emitter(event, config, payload, serializer=serializer)

    return construct


class BindMixin[C](ABC):
    """
    Give a sync adapter a typed `bind` method wrapping its `emit`.

    `C` is the adapter's config type; with `BindMixin[None]`, `config` can be omitted.
    """

    __slots__ = ()

    @abstractmethod
    def emit[S, R, Signal: bool](
        self,
        event: Event[DispatchPattern[R, Any], S, Signal],
        config: C,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R:
        """
        Dispatch a payload.

        See {py:meth}`Producer.emit <stratae.events.protocols.Producer.emit>`.
        """
        ...

    @overload
    def bind[**P, S, R](
        self,
        event: Event[DispatchPattern[R, Any], S, Literal[False]],
        *,
        factory: Callable[P, S],
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
    ) -> Callable[P, R]: ...
    @overload
    def bind[R](
        self,
        event: Event[DispatchPattern[R, Any], NoPayload, Literal[True]],
        *,
        factory: None = None,
        config: C = ...,
        serializer: None = None,
    ) -> Callable[[], R]: ...
    @overload
    def bind[S, R](
        self,
        event: Event[DispatchPattern[R, Any], S, Literal[False]],
        *,
        factory: None = None,
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
    ) -> Callable[[S], R]: ...

    def bind(
        self,
        event: Event[Any, Any, Any],
        *,
        factory: Callable[..., Any] | None = None,
        config: Any = None,
        serializer: Callable[[Any], Any] | None = None,
    ) -> Callable[..., Any]:
        """
        Bind an event to this adapter's `emit`. See {py:func}`bind`.

        :param event: The {py:class}`Event <stratae.events.event.Event>` to bind.
        :param factory: Builds the payload from the call's arguments.
        :param config: The adapter's routing config.
        :param serializer: Encodes the payload.
        :returns: A callable that dispatches through this adapter.
        """
        return bind(event, self.emit, factory=factory, config=config, serializer=serializer)


class AsyncBindMixin[C](ABC):
    """
    Give an async adapter a typed `bind` method wrapping its `emit`.

    The async counterpart to {py:class}`BindMixin`.
    """

    __slots__ = ()

    @abstractmethod
    def emit[S, R, Signal: bool](
        self,
        event: Event[DispatchPattern[R, Any], S, Signal],
        config: C,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> Awaitable[R]:
        """
        Dispatch a payload.

        See {py:meth}`Producer.emit <stratae.events.protocols.Producer.emit>`.
        """
        ...

    @overload
    def bind[**P, S, R](
        self,
        event: Event[DispatchPattern[R, Any], S, Literal[False]],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
    ) -> Callable[P, Awaitable[R]]: ...
    @overload
    def bind[R](
        self,
        event: Event[DispatchPattern[R, Any], NoPayload, Literal[True]],
        *,
        factory: None = None,
        config: C = ...,
        serializer: None = None,
    ) -> Callable[[], Awaitable[R]]: ...
    @overload
    def bind[S, R](
        self,
        event: Event[DispatchPattern[R, Any], S, Literal[False]],
        *,
        factory: None = None,
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
    ) -> Callable[[S], Awaitable[R]]: ...

    def bind(
        self,
        event: Event[Any, Any, Any],
        *,
        factory: Callable[..., Any] | None = None,
        config: Any = None,
        serializer: Callable[[Any], Any] | None = None,
    ) -> Callable[..., Awaitable[Any]]:
        """
        Bind an event to this adapter's `emit`. See {py:func}`abind`.

        :param event: The {py:class}`Event <stratae.events.event.Event>` to bind.
        :param factory: Builds the payload from the call's arguments, sync or async.
        :param config: The adapter's routing config.
        :param serializer: Encodes the payload.
        :returns: An async callable that dispatches through this adapter.
        """
        return abind(event, self.emit, factory=factory, config=config, serializer=serializer)
