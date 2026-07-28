"""
Callable facades that bind an event definition to a concrete emitter and routing config.

{py:func}`bind` and {py:func}`abind` bind an
{py:class}`Event <stratae.events.event.Event>` to a sync or async emitter,
respectively, along with adapter-specific routing config. Passed a
`factory`, each returns a callable that builds the payload from the call's
arguments and forwards it to the emitter. Omitting `factory` returns a
callable that takes an already-built payload directly and forwards it the
same way.

Omitting `factory` for a payload-less event, one whose schema is `None`,
returns a {py:class}`BoundSignal` instead, callable with no arguments at
all. There is nothing to build and nothing to pass.

Both returned callables forward themselves, along with the payload, to the
emitter, returning whatever the emitter produces. `bind` requires a sync
factory, since there's no way to await one from inside its synchronous
result. `abind` accepts either a sync or async factory, awaiting it either
way.

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

# A generic emitter only needs to match EmitCallable's shape: event,
# config, and payload in, some result out. This one does the work
# directly, using config as the destination label to route to.
def emit(
    event: Event[Request[Order], CreateOrder],
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

````{example} Binding a payload-less event
```{code-block} python
from typing import Any, Callable
from stratae.events import Event, PubSub, bind

# No schema, so the event's occurrence is the entire message.
heartbeat = Event(PubSub, name="heartbeat")

def emit(
    event: Event[PubSub, None],
    config: str,
    payload: None,
    *,
    serializer: Callable[[None], Any] | None = None,
) -> None:
    print(f"[{config}] {event.name}")

beat = bind(heartbeat, emit, config="health")

# Nothing to build, nothing to pass.
beat()
```
```{output}
[health] heartbeat
```
````

{py:class}`BindMixin` and {py:class}`AsyncBindMixin` give an adapter a
typed `bind` method of its own, wrapping its `emit`. A method's declared
signature is what resolves at the call site, so an adapter that wrote its
own `bind` couldn't defer to the overloads of what it delegates to.
Inheriting one of these mixins carries the overloads and the implementation
together, stating them once instead of per adapter.

See {py:func}`bind` and {py:func}`abind` for the rest of the module's API.

"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Protocol, overload

from stratae.events._typeguards import is_async_factory, is_sync_factory
from stratae.events.event import DispatchPattern, Event
from stratae.events.protocols import EmitCallable

_SYNC_FACTORY_REQUIRED = "bind requires a sync factory; resolve async work outside the factory"


class BoundSignal[R](Protocol):
    """
    Call shape of an event bound with no payload to pass.

    {py:func}`bind` returns one of these when `factory` is omitted for an
    {py:class}`Event <stratae.events.event.Event>` whose schema is `None`,
    so the binding is invoked with no arguments.

    `payload` is declared, defaulted to `None`, rather than omitted
    outright. A bare zero-argument callable would not be assignable to the
    `Callable[[S], R]` that {py:func}`bind` returns for a schema'd event,
    and the two overloads would be reported as overlapping with
    incompatible return types. Keeping the parameter optional makes the
    returns compatible while still allowing the no-argument call.
    """

    def __call__(self, payload: None = None, /) -> R:
        """
        Dispatch the event.

        :param payload: Always `None`. Present only so this stays
            assignable to `Callable[[None], R]`; there is no reason to
            pass it.
        :returns: Whatever the bound emitter produces.
        """
        ...


@overload
def bind[**P, T: DispatchPattern[Any, Any], S, C, R](
    event: Event[T, S],
    emitter: EmitCallable[T, S, C, R],
    *,
    factory: Callable[P, S],
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[P, R]: ...


@overload
def bind[T: DispatchPattern[Any, Any], C, R](
    event: Event[T, None],
    emitter: EmitCallable[T, None, C, R],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[None], Any] | None = None,
) -> BoundSignal[R]: ...


@overload
def bind[T: DispatchPattern[Any, Any], S, C, R](
    event: Event[T, S],
    emitter: EmitCallable[T, S, C, R],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[[S], R]: ...


def bind(
    event: Event[Any, Any],
    emitter: EmitCallable[Any, Any, Any, Any],
    *,
    factory: Callable[..., Any] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
) -> Callable[..., Any]:
    """
    Bind an event to a sync emitter, with a factory, or without one for passthrough.

    :param event: The {py:class}`Event <stratae.events.event.Event>` to
        bind. One whose schema is `None` binds to a
        {py:class}`BoundSignal`, taking no arguments.
    :param emitter: A callable that receives the `Event` being bound and
        the payload, and returns `R`.
    :param factory: Builds the payload from the bound call's arguments. When
        omitted, the returned callable instead forwards an already-built
        payload straight through.
    :param config: The adapter-specific routing config for this binding.
    :param serializer: Encodes the payload before dispatch. Format is
        adapter-defined (bytes, a JSON string, etc.). When omitted, the
        emitter falls back to its own default serializer, if any.
    :returns: A callable that builds the payload via `factory` and forwards
        it to `emitter` when `factory` is given, otherwise a callable that
        forwards an already-built payload straight through, or a
        `BoundSignal` when the event carries no payload.
    :raises TypeError: If `factory` is given and is async.

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
    event: Event[T, S],
    emitter: EmitCallable[T, S, C, Awaitable[R]],
    *,
    factory: Callable[P, S] | Callable[P, Awaitable[S]],
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[P, Awaitable[R]]: ...


@overload
def abind[T: DispatchPattern[Any, Any], C, R](
    event: Event[T, None],
    emitter: EmitCallable[T, None, C, Awaitable[R]],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[None], Any] | None = None,
) -> BoundSignal[Awaitable[R]]: ...


@overload
def abind[T: DispatchPattern[Any, Any], S, C, R](
    event: Event[T, S],
    emitter: EmitCallable[T, S, C, Awaitable[R]],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[[S], Awaitable[R]]: ...


def abind(
    event: Event[Any, Any],
    emitter: EmitCallable[Any, Any, Any, Awaitable[Any]],
    *,
    factory: Callable[..., Any] | Callable[..., Awaitable[Any]] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
) -> Callable[..., Awaitable[Any]]:
    """
    Bind an event to an async emitter, with a factory, or without one for passthrough.

    :param event: The {py:class}`Event <stratae.events.event.Event>` to
        bind. One whose schema is `None` binds to a
        {py:class}`BoundSignal`, taking no arguments.
    :param emitter: A coroutine callable that receives the `Event` being
        bound and the payload, and returns an awaitable resolving to `R`.
    :param factory: Builds the payload from the bound call's arguments, sync
        or async. When omitted, the returned callable instead forwards an
        already-built payload straight through.
    :param config: The adapter-specific routing config for this binding.
    :param serializer: Encodes the payload before dispatch. Format is
        adapter-defined (bytes, a JSON string, etc.). When omitted, the
        emitter falls back to its own default serializer, if any.
    :returns: A callable that builds the payload via `factory`, sync or
        async, and forwards it to `emitter` when `factory` is given,
        otherwise a callable that forwards an already-built payload straight
        through, or a `BoundSignal` when the event carries no payload.
        Either way, awaiting the callable resolves to `R`.

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
    Give a sync adapter a typed `bind` method wrapping its own `emit`.

    `C` is the adapter's routing config type. Inherit `BindMixin[None]` for
    an adapter needing no config, where `config` can then be omitted at the
    call site, or `BindMixin[SomeConfig]` for one that routes.

    The adapter inherits `bind` rather than defining it. Overload
    resolution doesn't pass through a wrapper, so an adapter that wrote its
    own `bind` method would have to restate all three overloads, and would
    otherwise be stuck declaring `Callable[..., Any]`, which type-checks
    nothing at any call site. Carrying the overloads and the implementation
    together here states them once for every adapter.

    Subclasses supply `emit`. It's abstract so the mixin can call it, and
    so an adapter that inherits this without providing one fails at
    definition rather than at dispatch.
    """

    __slots__ = ()

    @abstractmethod
    def emit[S, R](
        self,
        event: Event[DispatchPattern[R, Any], S],
        config: C,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R:
        """
        Dispatch a payload. See {py:meth}`Producer.emit <stratae.events.protocols.Producer.emit>`.

        :param event: The {py:class}`Event <stratae.events.event.Event>` being emitted.
        :param config: Adapter-specific routing configuration.
        :param payload: The constructed payload instance to dispatch.
        :param serializer: Encodes `payload` before dispatch.
        :returns: Adapter-defined result of dispatching the payload.
        """
        ...

    @overload
    def bind[**P, S, R](
        self,
        event: Event[DispatchPattern[R, Any], S],
        *,
        factory: Callable[P, S],
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
    ) -> Callable[P, R]: ...
    @overload
    def bind[R](
        self,
        event: Event[DispatchPattern[R, Any], None],
        *,
        factory: None = None,
        config: C = ...,
        serializer: Callable[[None], Any] | None = None,
    ) -> BoundSignal[R]: ...
    @overload
    def bind[S, R](
        self,
        event: Event[DispatchPattern[R, Any], S],
        *,
        factory: None = None,
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
    ) -> Callable[[S], R]: ...

    def bind(
        self,
        event: Event[Any, Any],
        *,
        factory: Callable[..., Any] | None = None,
        config: Any = None,
        serializer: Callable[[Any], Any] | None = None,
    ) -> Callable[..., Any]:
        """
        Return a callable bound to this adapter's `emit` and an event.

        :param event: The {py:class}`Event <stratae.events.event.Event>` to
            bind. One whose schema is `None` binds to a
            {py:class}`BoundSignal`, taking no arguments.
        :param factory: Builds the payload from the bound call's arguments.
            Omit it to pass an already-built payload straight through
            instead.
        :param config: The adapter-specific routing config for this binding.
        :param serializer: Encodes the payload before dispatch.
        :returns: A callable that builds the payload via `factory` when
            given, otherwise one that forwards an already-built payload
            straight through, or a `BoundSignal` when the event carries no
            payload; either way wrapping this adapter's `emit` and `event`.
        """
        return bind(event, self.emit, factory=factory, config=config, serializer=serializer)


class AsyncBindMixin[C](ABC):
    """
    Give an async adapter a typed `bind` method wrapping its own `emit`.

    The awaitable counterpart to {py:class}`BindMixin`, delegating to
    {py:func}`abind`. Every binding it produces resolves to `R` once
    awaited, and `factory` may be sync or async.

    Sync and async need separate mixins rather than one parameterized over
    the wrapper, since `BindMixin[Awaitable]` would require higher-kinded
    types.
    """

    __slots__ = ()

    @abstractmethod
    def emit[S, R](
        self,
        event: Event[DispatchPattern[R, Any], S],
        config: C,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> Awaitable[R]:
        """
        Dispatch a payload. See {py:meth}`Producer.emit <stratae.events.protocols.Producer.emit>`.

        Declared as returning `Awaitable[R]` rather than as `async def`, so
        an adapter's `async def emit` satisfies it without the mixin
        constraining how the coroutine is produced.

        :param event: The {py:class}`Event <stratae.events.event.Event>` being emitted.
        :param config: Adapter-specific routing configuration.
        :param payload: The constructed payload instance to dispatch.
        :param serializer: Encodes `payload` before dispatch.
        :returns: An awaitable resolving to the adapter-defined result.
        """
        ...

    @overload
    def bind[**P, S, R](
        self,
        event: Event[DispatchPattern[R, Any], S],
        *,
        factory: Callable[P, S] | Callable[P, Awaitable[S]],
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
    ) -> Callable[P, Awaitable[R]]: ...
    @overload
    def bind[R](
        self,
        event: Event[DispatchPattern[R, Any], None],
        *,
        factory: None = None,
        config: C = ...,
        serializer: Callable[[None], Any] | None = None,
    ) -> BoundSignal[Awaitable[R]]: ...
    @overload
    def bind[S, R](
        self,
        event: Event[DispatchPattern[R, Any], S],
        *,
        factory: None = None,
        config: C = ...,
        serializer: Callable[[S], Any] | None = None,
    ) -> Callable[[S], Awaitable[R]]: ...

    def bind(
        self,
        event: Event[Any, Any],
        *,
        factory: Callable[..., Any] | None = None,
        config: Any = None,
        serializer: Callable[[Any], Any] | None = None,
    ) -> Callable[..., Awaitable[Any]]:
        """
        Return an awaitable callable bound to this adapter's `emit` and an event.

        :param event: The {py:class}`Event <stratae.events.event.Event>` to
            bind. One whose schema is `None` binds to a
            {py:class}`BoundSignal`, taking no arguments.
        :param factory: Builds the payload from the bound call's arguments,
            sync or async. Omit it to pass an already-built payload
            straight through instead.
        :param config: The adapter-specific routing config for this binding.
        :param serializer: Encodes the payload before dispatch.
        :returns: A callable that builds the payload via `factory` when
            given, otherwise one that forwards an already-built payload
            straight through, or a `BoundSignal` when the event carries no
            payload; either way wrapping this adapter's `emit` and `event`,
            and resolving to its result once awaited.
        """
        return abind(event, self.emit, factory=factory, config=config, serializer=serializer)
