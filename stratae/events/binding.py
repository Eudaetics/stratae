"""
Callable facades that bind an event definition to a concrete emitter.

{py:func}`bind` binds an event to an {py:class}`Emitter`: a call paired with its
{py:class}`Kind <stratae.events.kind.Kind>`, which says whether the call is sync or async.
`bind` hands the event and the call to the kind's binding function, {py:func}`bind_sync` or
{py:func}`bind_async`, so a sync emitter gives a sync callable and an async one an async
callable, whose factory may be async too. An adapter's `emitter` method gives its
`Emitter` for an event, routed if the adapter routes, and its own `bind` method binds that.
A plain function is bound by pairing it with its kind, as `Emitter(SYNC, emit)`.

A sync emitter runs hooks synchronously, so binding one refuses async hooks and an async
factory.

The call takes the event and the payload, as an {py:class}`Emit
<stratae.events.protocols.Emit>`. Only binding with `hooks` needs one that also takes
`hooks=`, an {py:class}`EmitCallable <stratae.events.protocols.EmitCallable>`.

The bound callable's signature is typed at runtime, not only for type checkers. With a
factory, it takes the factory's parameters. Without one, it takes a `payload` of the event's
schema, or nothing for a payload-less event. It returns the event's reply type. A framework
that inspects it, for example to build a route, sees those types.

````{example} Binding an event to a generic emitter
```{code-block} python
from typing import Any
from stratae.events import SYNC, Emitter, EventType, Request, bind

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

create_order = bind(order_created, Emitter(SYNC, emit), factory=CreateOrder)

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

create_built_order = bind(order_created, Emitter(SYNC, emit))

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
from dataclasses import dataclass
from functools import partial
from inspect import Parameter, Signature, signature
from types import NoneType
from typing import TYPE_CHECKING, Any, cast, overload

from stratae.callables import is_async_callable
from stratae.events.hooks import Hooks
from stratae.events.patterns import NoPayload
from stratae.events.protocols import (
    AnyEvent,
    Emit,
    EmitCallable,
    PayloadEvent,
    SignalEvent,
)

if TYPE_CHECKING:
    from stratae.events.kind import Kind

_SYNC_FACTORY_REQUIRED = (
    "a sync emitter needs a sync factory; resolve async work outside the factory, or bind an"
    " async emitter"
)

_SYNC_HOOKS_REQUIRED = (
    "{!r} is bound to a sync emitter, which runs hooks synchronously; give it sync hooks, or bind"
    " an async emitter"
)

_EMITTER_REQUIRED = (
    "bind takes an Emitter, a call paired with its Kind, such as Emitter(SYNC, emit) or a"
    " producer's emitter(event)"
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


def _with_hooks[Call: Callable[..., Any]](emitter: Call, hooks: Hooks[Any, Any] | None, /) -> Call:
    """
    Return the emitter a bound callable calls: `emitter`, given the hooks it was bound with.

    `hooks` is passed only when given, so an emitter that doesn't take it still works until
    someone binds with hooks, then refuses loudly. Fixed when binding, so an emit without
    hooks passes none.
    """
    return cast(Call, partial(emitter, hooks=hooks)) if hooks is not None else emitter


@dataclass(frozen=True, slots=True)
class Emitter[Call]:
    """
    An emitter: a call paired with its kind.

    The kind says whether the call is sync or async, and holds the function that binds an
    event to it, so {py:func}`bind` never inspects the call. A producer's `emitter` method gives
    one for an event. Pair a plain function with its kind to bind it, as `Emitter(SYNC, emit)`.
    """

    kind: "Kind"
    """Whether `call` is sync or async, and how an event is bound to it."""
    call: Call
    """Emits an event: an {py:class}`Emit <stratae.events.protocols.Emit>`, or an
    {py:class}`EmitCallable <stratae.events.protocols.EmitCallable>` taking hooks too."""


@overload
def bind[**P, Payload, Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[Emit[Event, Payload, Result]],
    /,
    *,
    factory: Callable[P, Payload],
    hooks: None = None,
) -> Callable[P, Result]: ...


@overload
def bind[Result, Event: SignalEvent[Any, Any]](
    event: Event,
    emitter: Emitter[Emit[Event, NoPayload, Result]],
    /,
    *,
    factory: None = None,
    hooks: None = None,
) -> Callable[[], Result]: ...


@overload
def bind[Payload, Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[Emit[Event, Payload, Result]],
    /,
    *,
    factory: None = None,
    hooks: None = None,
) -> Callable[[Payload], Result]: ...


@overload
def bind[**P, Payload, HookType: Hooks[Any, Any], Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[EmitCallable[Event, Payload, HookType, Result]],
    /,
    *,
    factory: Callable[P, Payload],
    hooks: HookType | None = None,
) -> Callable[P, Result]: ...


@overload
def bind[HookType: Hooks[Any, Any], Result, Event: SignalEvent[Any, Any]](
    event: Event,
    emitter: Emitter[EmitCallable[Event, NoPayload, HookType, Result]],
    /,
    *,
    factory: None = None,
    hooks: HookType | None = None,
) -> Callable[[], Result]: ...


@overload
def bind[Payload, HookType: Hooks[Any, Any], Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[EmitCallable[Event, Payload, HookType, Result]],
    /,
    *,
    factory: None = None,
    hooks: HookType | None = None,
) -> Callable[[Payload], Result]: ...


@overload
def bind[**P, Payload, Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[Emit[Event, Payload, Awaitable[Result]]],
    /,
    *,
    factory: Callable[P, Awaitable[Payload]],
    hooks: None = None,
) -> Callable[P, Awaitable[Result]]: ...


@overload
def bind[**P, Payload, HookType: Hooks[Any, Any], Result, Event: PayloadEvent[Any, Any, Any]](
    event: Event,
    emitter: Emitter[EmitCallable[Event, Payload, HookType, Awaitable[Result]]],
    /,
    *,
    factory: Callable[P, Awaitable[Payload]],
    hooks: HookType | None = None,
) -> Callable[P, Awaitable[Result]]: ...


def bind(
    event: AnyEvent,
    emitter: object,
    /,
    *,
    factory: Callable[..., Any] | None = None,
    hooks: Hooks[Any, Any] | None = None,
) -> Callable[..., Any]:
    """
    Bind an event to an emitter, sync or async as its kind says.

    The emitter's kind binds the event to its call: a sync kind gives a sync callable, and an
    async kind an async callable, whose factory may be async too.

    :param event: The event to bind.
    :param emitter: The {py:class}`Emitter`: its call takes the event and payload, and, only if
        `hooks` is given, the hooks.
    :param factory: A callable building the payload from the call's arguments, async only for
        an async emitter. If omitted, the payload is passed directly.
    :param hooks: {py:class}`Hooks <stratae.events.hooks.Hooks>` run around each emit,
        passed to the call only when given.
    :returns: A callable that dispatches through the emitter's call.
    :raises TypeError: If `emitter` isn't an `Emitter`, or it's sync and `factory` or a hook is
        async.
    """
    if not isinstance(emitter, Emitter):
        raise TypeError(_EMITTER_REQUIRED)
    checked = cast("Emitter[Callable[..., Any]]", emitter)
    return checked.kind.bind(event, checked.call, factory, hooks)


def bind_sync(
    event: AnyEvent,
    call: Callable[..., Any],
    factory: Callable[..., Any] | None,
    hooks: Hooks[Any, Any] | None,
    /,
) -> Callable[..., Any]:
    """
    Bind an event to a sync call, as a sync kind does.

    It's the binding function of {py:data}`SYNC <stratae.events.kind.SYNC>`. Bind an event with
    {py:func}`bind`; this is for composing a {py:class}`Kind <stratae.events.kind.Kind>` of
    your own.

    :param event: The event to bind.
    :param call: The emitter's call.
    :param factory: A sync callable building the payload, or `None` to pass it directly.
    :param hooks: The hooks passed to `call` with each emit, or `None`.
    :returns: A sync callable that dispatches through `call`.
    :raises TypeError: If `factory` or a hook is async.
    """
    if hooks is not None and hooks.has_async:
        raise TypeError(_SYNC_HOOKS_REQUIRED.format(event.name))
    emit = _with_hooks(call, hooks)
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


def bind_async(
    event: AnyEvent,
    call: Callable[..., Awaitable[Any]],
    factory: Callable[..., Any] | None,
    hooks: Hooks[Any, Any] | None,
    /,
) -> Callable[..., Awaitable[Any]]:
    """
    Bind an event to an async call, as an async kind does.

    It's the binding function of {py:data}`ASYNC <stratae.events.kind.ASYNC>`. Bind an event
    with {py:func}`bind`; this is for composing a {py:class}`Kind <stratae.events.kind.Kind>` of
    your own.

    :param event: The event to bind.
    :param call: The emitter's call, returning an awaitable.
    :param factory: A sync or async callable building the payload, or `None` to pass it
        directly.
    :param hooks: The hooks passed to `call` with each emit, or `None`.
    :returns: An async callable that dispatches through `call`.
    """
    emit = _with_hooks(call, hooks)
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
