"""
Callable facades that bind an event definition to a concrete emitter.

{py:func}`bind` binds an event to an emitter. A sync emitter gives a sync callable. An async
one, whose call gives a coroutine, gives an async callable, whose factory may be async too.
{py:func}`abind` binds an async emitter only. With a `factory`, the result builds the payload
from its arguments. Without one, it takes a built payload, or nothing for a payload-less
event. An adapter's `emitter` method gives its emitter for an event, routed if the adapter
routes, and its own `bind` method binds that.

A sync emitter runs hooks synchronously, so binding one refuses async hooks and an async
factory. An async emitter that isn't an `async def` is recognised only if it's marked with
`inspect.markcoroutinefunction`; otherwise it's bound as sync.

The emitter is called with the event and the payload, as an {py:class}`Emitter
<stratae.events.protocols.Emitter>`. Only binding with `hooks` needs one that also takes
`hooks=`, an {py:class}`EmitCallable <stratae.events.protocols.EmitCallable>`.

The bound callable's signature is typed at runtime, not only for type checkers. With a
factory, it takes the factory's parameters. Without one, it takes a `payload` of the event's
schema, or nothing for a payload-less event. It returns the event's reply type. A framework
that inspects it, for example to build a route, sees those types.

````{example} Binding an event to a generic emitter
```{code-block} python
from typing import Any
from stratae.events import EventType, Request, bind

class CreateOrder:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

class Order:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id

order_created = Request(CreateOrder, reply=Order)

def emit(event: EventType[CreateOrder, Any, Any, Any], payload: CreateOrder, /) -> Order:
    print(f"creating order {payload.order_id}")
    return Order(order_id=payload.order_id)

create_order = bind(order_created, emit, factory=CreateOrder)

created = create_order(order_id=42)
print(f"created order: {created.order_id}")
```
```{output}
creating order 42
created order: 42
```
````

````{example} Reading a bound callable's types at runtime
```{code-block} python
import inspect

create_built_order = bind(order_created, emit)

print(inspect.signature(create_order))
print(inspect.signature(create_built_order))
```
```{output}
(order_id: int) -> __main__.Order
(payload: __main__.CreateOrder) -> __main__.Order
```
````
"""

from collections.abc import Awaitable, Callable
from functools import partial
from inspect import Parameter, Signature, signature
from types import NoneType
from typing import Any, cast, overload

from stratae.callables import is_async_callable
from stratae.events.hooks import Hooks
from stratae.events.patterns import NoPayload
from stratae.events.protocols import (
    AnyEvent,
    EmitCallable,
    Emitter,
    PayloadEvent,
    SignalEvent,
)

_SYNC_FACTORY_REQUIRED = (
    "a sync emitter needs a sync factory; resolve async work outside the factory, or bind an"
    " async emitter"
)

_SYNC_HOOKS_REQUIRED = (
    "{!r} is bound to a sync emitter, which runs hooks synchronously; give it sync hooks, or bind"
    " an async emitter"
)

_NO_PAYLOAD = NoPayload()
"""What a payload-less event's bound callable passes as its payload."""


def _reply(event: AnyEvent, /) -> Any:
    """Return the event's reply type as a return annotation: `None`, not `NoneType`."""
    reply = event.emit_returns
    return None if reply is NoneType else reply


def _factory_parameters(factory: Callable[..., Any], /) -> list[Parameter]:
    """Return the factory's parameters, with string annotations resolved where they can be."""
    try:
        return list(signature(factory, eval_str=True).parameters.values())
    except NameError:
        return list(signature(factory).parameters.values())


def _payload_parameters(event: AnyEvent, /) -> list[Parameter]:
    """Return the single `payload` parameter, of the event's schema."""
    return [Parameter("payload", Parameter.POSITIONAL_OR_KEYWORD, annotation=event.schema)]


def _typed[F: Callable[..., Any]](bound: F, parameters: list[Parameter], event: AnyEvent, /) -> F:
    """Give a bound callable the signature and annotations its event and factory imply."""
    returns = _reply(event)
    annotations = {p.name: p.annotation for p in parameters if p.annotation is not Parameter.empty}
    target = cast(Any, bound)
    target.__signature__ = Signature(parameters, return_annotation=returns)
    target.__annotations__ = annotations | {"return": returns}
    return bound


def _with_hooks[Emit: Callable[..., Any]](emitter: Emit, hooks: Hooks[Any, Any] | None, /) -> Emit:
    """
    Return the emitter a bound callable calls: `emitter`, given the hooks it was bound with.

    `hooks` is passed only when given, so an emitter that doesn't take it still works until
    someone binds with hooks, then refuses loudly. Fixed when binding, so an emit without
    hooks passes none.
    """
    return cast(Emit, partial(emitter, hooks=hooks)) if hooks is not None else emitter


@overload
def bind[**P, Payload, Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[Event, Payload, Result],
    /,
    *,
    factory: Callable[P, Payload],
    hooks: None = None,
) -> Callable[P, Result]: ...


@overload
def bind[Result, Event: SignalEvent[Any, Any]](
    event: Event,
    emitter: Emitter[Event, NoPayload, Result],
    /,
    *,
    factory: None = None,
    hooks: None = None,
) -> Callable[[], Result]: ...


@overload
def bind[Payload, Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[Event, Payload, Result],
    /,
    *,
    factory: None = None,
    hooks: None = None,
) -> Callable[[Payload], Result]: ...


@overload
def bind[**P, Payload, HookType: Hooks[Any, Any], Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: EmitCallable[Event, Payload, HookType, Result],
    /,
    *,
    factory: Callable[P, Payload],
    hooks: HookType | None = None,
) -> Callable[P, Result]: ...


@overload
def bind[HookType: Hooks[Any, Any], Result, Event: SignalEvent[Any, Any]](
    event: Event,
    emitter: EmitCallable[Event, NoPayload, HookType, Result],
    /,
    *,
    factory: None = None,
    hooks: HookType | None = None,
) -> Callable[[], Result]: ...


@overload
def bind[Payload, HookType: Hooks[Any, Any], Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: EmitCallable[Event, Payload, HookType, Result],
    /,
    *,
    factory: None = None,
    hooks: HookType | None = None,
) -> Callable[[Payload], Result]: ...


@overload
def bind[**P, Payload, Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[Event, Payload, Awaitable[Result]],
    /,
    *,
    factory: Callable[P, Awaitable[Payload]],
    hooks: None = None,
) -> Callable[P, Awaitable[Result]]: ...


@overload
def bind[**P, Payload, HookType: Hooks[Any, Any], Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: EmitCallable[Event, Payload, HookType, Awaitable[Result]],
    /,
    *,
    factory: Callable[P, Awaitable[Payload]],
    hooks: HookType | None = None,
) -> Callable[P, Awaitable[Result]]: ...


def bind(
    event: AnyEvent,
    emitter: EmitCallable[Any, Any, Any, Any] | Emitter[Any, Any, Any],
    /,
    *,
    factory: Callable[..., Any] | None = None,
    hooks: Hooks[Any, Any] | None = None,
) -> Callable[..., Any]:
    """
    Bind an event to an emitter, sync or async.

    An async emitter, one whose call gives a coroutine, such as an `async def` or one marked
    with `inspect.markcoroutinefunction`, gives an async callable, whose factory may be async
    too. Any other emitter gives a sync callable.

    :param event: The event to bind.
    :param emitter: Called with the event and payload. Without `hooks`, it needn't take any.
    :param factory: A callable building the payload from the call's arguments, async only for
        an async emitter. If omitted, the payload is passed directly.
    :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit,
        passed to `emitter` only when given.
    :returns: A callable that dispatches through `emitter`.
    :raises TypeError: If the emitter is sync, and `factory` or a hook is async.
    """
    if is_async_callable(emitter):
        return _bind_async(event, emitter, factory, hooks)
    if hooks is not None and hooks.has_async:
        raise TypeError(_SYNC_HOOKS_REQUIRED.format(event.name))
    emit = _with_hooks(emitter, hooks)
    if factory is None and event.is_signal:

        def signal() -> Any:
            return emit(event, _NO_PAYLOAD)

        return _typed(signal, [], event)

    if factory is None:

        def passthrough(payload: Any) -> Any:
            return emit(event, payload)

        return _typed(passthrough, _payload_parameters(event), event)

    if is_async_callable(factory):
        raise TypeError(_SYNC_FACTORY_REQUIRED)

    def construct(*args: Any, **kwargs: Any) -> Any:
        return emit(event, factory(*args, **kwargs))

    return _typed(construct, _factory_parameters(factory), event)


@overload
def abind[**P, Payload, Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[Event, Payload, Awaitable[Result]],
    /,
    *,
    factory: Callable[P, Payload] | Callable[P, Awaitable[Payload]],
    hooks: None = None,
) -> Callable[P, Awaitable[Result]]: ...


@overload
def abind[Result, Event: SignalEvent[Any, Any]](
    event: Event,
    emitter: Emitter[Event, NoPayload, Awaitable[Result]],
    /,
    *,
    factory: None = None,
    hooks: None = None,
) -> Callable[[], Awaitable[Result]]: ...


@overload
def abind[Payload, Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[Event, Payload, Awaitable[Result]],
    /,
    *,
    factory: None = None,
    hooks: None = None,
) -> Callable[[Payload], Awaitable[Result]]: ...


@overload
def abind[**P, Payload, HookType: Hooks[Any, Any], Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: EmitCallable[Event, Payload, HookType, Awaitable[Result]],
    /,
    *,
    factory: Callable[P, Payload] | Callable[P, Awaitable[Payload]],
    hooks: HookType | None = None,
) -> Callable[P, Awaitable[Result]]: ...


@overload
def abind[HookType: Hooks[Any, Any], Result, Event: SignalEvent[Any, Any]](
    event: Event,
    emitter: EmitCallable[Event, NoPayload, HookType, Awaitable[Result]],
    /,
    *,
    factory: None = None,
    hooks: HookType | None = None,
) -> Callable[[], Awaitable[Result]]: ...


@overload
def abind[Payload, HookType: Hooks[Any, Any], Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: EmitCallable[Event, Payload, HookType, Awaitable[Result]],
    /,
    *,
    factory: None = None,
    hooks: HookType | None = None,
) -> Callable[[Payload], Awaitable[Result]]: ...


def abind(
    event: AnyEvent,
    emitter: EmitCallable[Any, Any, Any, Awaitable[Any]] | Emitter[Any, Any, Awaitable[Any]],
    /,
    *,
    factory: Callable[..., Any] | Callable[..., Awaitable[Any]] | None = None,
    hooks: Hooks[Any, Any] | None = None,
) -> Callable[..., Awaitable[Any]]:
    """
    Bind an event to an async emitter.

    :param event: The event to bind.
    :param emitter: An async callable taking the event and payload. Without `hooks`, it
        needn't take any.
    :param factory: A sync or async callable building the payload from the call's
        arguments. If omitted, the payload is passed directly.
    :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit,
        passed to `emitter` only when given.
    :returns: An async callable that dispatches through `emitter`.
    """
    return _bind_async(event, emitter, factory, hooks)


def _bind_async(
    event: AnyEvent,
    emitter: Callable[..., Awaitable[Any]],
    factory: Callable[..., Any] | None,
    hooks: Hooks[Any, Any] | None,
    /,
) -> Callable[..., Awaitable[Any]]:
    """Bind an event to an async emitter, as an async callable; its factory may be async."""
    emit = _with_hooks(emitter, hooks)
    if factory is None and event.is_signal:

        async def signal() -> Any:
            return await emit(event, _NO_PAYLOAD)

        return _typed(signal, [], event)

    if factory is None:

        async def passthrough(payload: Any) -> Any:
            return await emit(event, payload)

        return _typed(passthrough, _payload_parameters(event), event)

    if is_async_callable(factory):

        async def construct_async(*args: Any, **kwargs: Any) -> Any:
            payload = await factory(*args, **kwargs)
            return await emit(event, payload)

        return _typed(construct_async, _factory_parameters(factory), event)

    async def construct(*args: Any, **kwargs: Any) -> Any:
        return await emit(event, factory(*args, **kwargs))

    return _typed(construct, _factory_parameters(factory), event)
