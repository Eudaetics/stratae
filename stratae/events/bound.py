"""
Callable facades that bind an event definition to a concrete emitter and routing config.

{py:func}`bind` and {py:func}`abind` attach a sync or async emitter,
respectively, along with adapter-specific routing config, to an
{py:class}`Event <stratae.events.event.Event>`. Passed a `factory`, each
returns a callable that builds the payload from the call's arguments and
forwards it to the emitter. Omitting `factory` returns a callable that
takes an already-built payload directly and forwards it the same way.

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

create_order = bind(emit, order_created, factory=CreateOrder, config="orders")

created = create_order(order_id=42)
print(f"created order: {created.order_id}")
```
```{output}
[orders] creating order 42
created order: 42
```
````

See {py:func}`bind` and {py:func}`abind` for the rest of the module's API.

"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, overload

from stratae.events._typeguards import is_async_factory, is_sync_factory
from stratae.events.event import DispatchPattern, Event
from stratae.events.protocols import EmitCallable

_SYNC_FACTORY_REQUIRED = "bind requires a sync factory; resolve async work outside the factory"


@overload
def bind[**P, T: DispatchPattern, S, C, R](
    emitter: EmitCallable[T, S, C, R],
    event: Event[T, S],
    *,
    factory: Callable[P, S],
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[P, R]: ...


@overload
def bind[T: DispatchPattern, S, C, R](
    emitter: EmitCallable[T, S, C, R],
    event: Event[T, S],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[[S], R]: ...


def bind(
    emitter: EmitCallable[Any, Any, Any, Any],
    event: Event[Any, Any],
    *,
    factory: Callable[..., Any] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
) -> Callable[..., Any]:
    """
    Bind a sync emitter to an event, with a factory, or without one for passthrough.

    :param emitter: A callable that receives the `Event` being bound and
        the payload, and returns `R`.
    :param event: The {py:class}`Event <stratae.events.event.Event>` to bind.
    :param factory: Builds the payload from the bound call's arguments. When
        omitted, the returned callable instead forwards an already-built
        payload straight through.
    :param config: The adapter-specific routing config for this binding.
    :param serializer: Encodes the payload before dispatch. Format is
        adapter-defined (bytes, a JSON string, etc.). When omitted, the
        emitter falls back to its own default serializer, if any.
    :returns: A callable that builds the payload via `factory` and forwards
        it to `emitter` when `factory` is given, otherwise a callable that
        forwards an already-built payload straight through.
    :raises TypeError: If `factory` is given and is async.

    """
    if factory is None:

        def passthrough(payload: Any) -> Any:
            return emitter(event, config, payload, serializer=serializer)

        return passthrough

    if not is_sync_factory(factory):
        raise TypeError(_SYNC_FACTORY_REQUIRED)

    def construct(*args: Any, **kwargs: Any) -> Any:
        return emitter(event, config, factory(*args, **kwargs), serializer=serializer)

    return construct


@overload
def abind[**P, T: DispatchPattern, S, C, R](
    emitter: EmitCallable[T, S, C, Awaitable[R]],
    event: Event[T, S],
    *,
    factory: Callable[P, S] | Callable[P, Awaitable[S]],
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[P, Awaitable[R]]: ...


@overload
def abind[T: DispatchPattern, S, C, R](
    emitter: EmitCallable[T, S, C, Awaitable[R]],
    event: Event[T, S],
    *,
    factory: None = None,
    config: C,
    serializer: Callable[[S], Any] | None = None,
) -> Callable[[S], Awaitable[R]]: ...


def abind(
    emitter: EmitCallable[Any, Any, Any, Awaitable[Any]],
    event: Event[Any, Any],
    *,
    factory: Callable[..., Any] | Callable[..., Awaitable[Any]] | None = None,
    config: Any,
    serializer: Callable[[Any], Any] | None = None,
) -> Callable[..., Awaitable[Any]]:
    """
    Bind an async emitter to an event, with a factory, or without one for passthrough.

    :param emitter: A coroutine callable that receives the `Event` being
        bound and the payload, and returns an awaitable resolving to `R`.
    :param event: The {py:class}`Event <stratae.events.event.Event>` to
        bind.
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
        through. Either way, awaiting the callable resolves to `R`.

    """
    if factory is None:

        async def passthrough(payload: Any) -> Any:
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
