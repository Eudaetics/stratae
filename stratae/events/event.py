"""
Define events as payload types bound to a dispatch pattern.

An event definition captures what an event IS: a payload schema and a
dispatch pattern, independent of any bus or routing config.
{py:class}`EventType` subclasses describe the dispatch pattern.
{py:class}`PubSub` is fire and forget. {py:class}`Request` is
request/reply; subscript it with the reply type, e.g. `Request[BookFound]`.

{py:func}`event` binds a payload factory and an `EventType` discriminant
into an {py:class}`EventConfig`, or an {py:class}`AsyncEventConfig` when
the factory is async. {py:func}`is_request` and {py:func}`reply_type`
inspect an `EventConfig`'s discriminant: {py:func}`is_request` reports
whether it's a subscripted `Request`, and {py:func}`reply_type` recovers
the reply type from one.

```{rubric} Example:
```
```{code-block} python
:caption: A create-user request whose responder logs a pub/sub event as a side effect

from stratae.events.direct import DirectBus
from stratae.events.event import PubSub, Request, event

class LogMessage:
    def __init__(self, text: str) -> None:
        self.text = text

class CreateUserSchema:
    def __init__(self, username: str) -> None:
        self.username = username

class User:
    def __init__(self, username: str) -> None:
        self.username = username

log_message = event(LogMessage, PubSub)
create_user = event(CreateUserSchema, Request[User])

bus = DirectBus()
# DirectBus takes no routing config since it dispatches in-process. A real
# broker's bind() would take that config here instead (e.g. a queue or
# exchange name), once, so it isn't repeated at every call site.
log = bus.bind(log_message)
create = bus.bind(create_user)

logged: list[str] = []

@bus.handle(log_message)
def write_to_log(entry: LogMessage) -> None:
    logged.append(entry.text)

@bus.handle(create_user)
def handle_create_user(cmd: CreateUserSchema) -> User:
    log(text=f"creating user {cmd.username}")
    return User(username=cmd.username)

created = create(username="ada")
assert created.username == "ada"
assert logged == ["creating user ada"]
```

See {py:class}`EventConfig`, {py:class}`AsyncEventConfig`, {py:func}`event`,
{py:func}`is_request`, and {py:func}`reply_type` for additional examples.

"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, cast, get_args, get_origin, overload

from stratae.events._typeguards import is_async_factory, is_class_factory

_UNSUBSCRIPTED_REQUEST = "Request must be subscripted with its reply type (e.g. Request[BookFound])"
_NOT_A_REQUEST = "event does not carry a subscripted Request discriminant"


class EventType:
    """Marker base class for event type discriminants."""


class PubSub(EventType):
    """
    Fire-and-forget dispatch pattern discriminant.

    Emitting an event with this discriminant returns immediately; there is
    no reply to wait for. Pass it as the `event_type` argument to
    {py:func}`event`.
    """


class Request[Reply](EventType):
    """
    Request/reply dispatch pattern discriminant.

    Emitting an event with this discriminant blocks until a responder
    returns a `Reply` value. Always subscript with the reply type, e.g.
    `Request[BookFound]`.

    {py:class}`EventConfig` rejects a bare, unsubscripted `Request` since
    the reply type could not be recovered at runtime. Use
    {py:func}`reply_type` to recover it from a request event's
    discriminant.
    """


def _validate_event_type(event_type: type[EventType]):
    """Raise `TypeError` if `event_type` is an unsubscripted `Request`."""
    if get_origin(event_type) is None and issubclass(event_type, Request):
        raise TypeError(_UNSUBSCRIPTED_REQUEST)


class EventConfig[**P, E: Any, T: EventType]:
    """
    Bus-agnostic event definition binding a payload type to a dispatch pattern.

    An event captures what an event IS: the payload schema and the dispatch
    pattern, independent of any bus or routing config. It's the shareable
    definition that one or more bus bindings can reference. Typically
    constructed via {py:func}`event`, which also derives an
    {py:class}`AsyncEventConfig` for async factories.

    `payload_type` is just an explicit declaration. It doesn't
    retroactively type-check `factory`'s own parameters. If `factory` is a
    generic class (e.g. `class Wrapped[T]: ...`), subscript it at the call
    site instead (`event(Wrapped[OrderPlaced], PubSub)`) so its constructor
    is fully checked. Passing the bare class with
    `payload_type=Wrapped[OrderPlaced]` leaves any parameter typed with `T`
    unchecked.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Binding a plain payload class to a pub/sub dispatch pattern

    from stratae.events.event import EventConfig, PubSub

    class LogMessage:
        def __init__(self, text: str) -> None:
            self.text = text

    log_message = EventConfig(LogMessage, PubSub)
    payload = log_message.factory(text="hello")
    ```

    """

    __slots__ = ("_factory", "event_type", "name", "payload_type")

    def __init__(
        self,
        factory: Callable[P, E] | Callable[P, Awaitable[E]],
        event_type: type[T],
        *,
        name: str | None = None,
        payload_type: type[E] | None = None,
    ) -> None:
        """
        Define an event with a schema type and dispatch pattern.

        :param factory: A factory used to construct the payload.
        :param event_type: The dispatch pattern discriminant class.
        :param name: Human-readable identifier for this event. Defaults to
            `factory.__name__`.
        :param payload_type: The concrete payload type this event carries.
            Derived from `factory` when `factory` is itself a class; must
            be provided explicitly otherwise.
        :raises TypeError: If `event_type` is an unsubscripted `Request`, or
            if `payload_type` is omitted and `factory` is not a class.
        """
        _validate_event_type(event_type)
        self._factory = factory
        self.event_type = event_type

        self.name = name if name is not None else factory.__name__

        if payload_type is None:
            if not is_class_factory(factory):
                raise TypeError("payload_type must be provided when factory is not a class")
            payload_type = factory
        self.payload_type = payload_type

    @property
    def factory(self) -> Callable[P, E] | Callable[P, Awaitable[E]]:
        """Return the factory used to construct this event's payload."""
        return self._factory


class AsyncEventConfig[**P, E: Any, T: EventType](EventConfig[P, E, T]):
    """
    Specialization of EventConfig for async payload factories.

    Its `factory` is typed as returning `Awaitable[E]` rather than `E`
    directly.

    Unlike a class factory, whose class object doubles as its own payload
    type, an async factory is a plain callable with no self-describing
    runtime type. `payload_type` is required here because there's no way
    to recover it from `factory` alone.

    Declaring `payload_type` explicitly doesn't retroactively type-check
    `factory`'s own parameters, though. A generic async factory function
    (e.g. `async def make_wrapped[T](item: T) -> Wrapped[T]: ...`) can
    still end up with unchecked argument types even when `payload_type`
    looks precise. Where practical, prefer a concrete factory whose
    parameters and return type are already fully resolved.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Binding an async payload factory to a pub/sub dispatch pattern

    import asyncio
    from stratae.events.event import AsyncEventConfig, PubSub

    class LogMessage:
        def __init__(self, text: str) -> None:
            self.text = text

    async def make_log_message(text: str) -> LogMessage:
        await asyncio.sleep(0)  # e.g. an async lookup before constructing the payload
        return LogMessage(text=text)

    log_message = AsyncEventConfig(make_log_message, PubSub, payload_type=LogMessage)
    payload = asyncio.run(log_message.factory(text="hello"))
    ```

    """

    __slots__ = ("_async_factory",)

    def __init__(
        self,
        factory: Callable[P, Awaitable[E]],
        event_type: type[T],
        *,
        name: str | None = None,
        payload_type: type[E] | None = None,
    ) -> None:
        """
        Define an event with an async factory.

        :param factory: An async factory used to construct the payload.
        :param event_type: The dispatch pattern discriminant class.
        :param name: Human-readable identifier for this event. Defaults to
            `factory.__name__`.
        :param payload_type: The concrete payload type this event carries.
            Required for async factories, since they cannot self-derive it
            from `factory` alone.
        :raises TypeError: If `payload_type` is omitted, or if `event_type`
            is an unsubscripted `Request`.
        """
        if payload_type is None:
            raise TypeError("payload_type must be provided for async factories")
        super().__init__(factory, event_type, name=name, payload_type=payload_type)
        self._async_factory = factory

    @property
    def factory(self) -> Callable[P, Awaitable[E]]:
        """Return the async factory used to construct this event's payload."""
        return self._async_factory


@overload
def event[**P, E: Any, T: EventType](
    factory: Callable[P, E],
    event_type: type[T],
    *,
    name: str | None = None,
) -> EventConfig[P, E, T]: ...


@overload
def event[**P, E: Any, T: EventType](
    factory: Callable[P, Awaitable[E]],
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E],
) -> AsyncEventConfig[P, E, T]: ...


@overload
def event[**P, E: Any, T: EventType](
    factory: Callable[P, E],
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E],
) -> EventConfig[P, E, T]: ...


def event[**P, E: Any, T: EventType](
    factory: Callable[P, E] | Callable[P, Awaitable[E]],
    event_type: type[T],
    *,
    name: str | None = None,
    payload_type: type[E] | None = None,
) -> EventConfig[P, E, T] | AsyncEventConfig[P, E, T]:
    """
    Define an event binding a factory to a dispatch pattern.

    Dispatches to {py:class}`AsyncEventConfig` when `factory` is an async
    function, or to {py:class}`EventConfig` otherwise.

    `payload_type` is just an explicit declaration. It doesn't
    retroactively type-check `factory`'s own parameters. If `factory` is a
    generic class (e.g. `class Wrapped[T]: ...`), subscript it at the call
    site instead (`event(Wrapped[OrderPlaced], PubSub)`) so its constructor
    is fully checked. Passing the bare class with
    `payload_type=Wrapped[OrderPlaced]` leaves any parameter typed with `T`
    unchecked.

    :param factory: A factory used to construct the payload.
    :param event_type: The dispatch pattern discriminant class.
    :param name: Human-readable identifier for this event. Defaults to
        `factory.__name__`.
    :param payload_type: The concrete payload type this event carries.
        Derived from `factory` when `factory` is itself a class; must be
        provided explicitly otherwise.
    :returns: An {py:class}`EventConfig` (or {py:class}`AsyncEventConfig`,
        for an async factory) binding `factory` to `event_type`.
    :raises TypeError: If `event_type` is an unsubscripted `Request`, or if
        `payload_type` is omitted for a factory that isn't itself a class.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Defining a pub/sub event and emitting it through a bus

    from stratae.events.direct import DirectBus
    from stratae.events.event import PubSub, event

    class OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    order_placed = event(OrderPlaced, PubSub)

    bus = DirectBus()

    @bus.handle(order_placed)
    def on_order_placed(order: OrderPlaced) -> None:
        print(f"order {order.order_id} placed")

    bus.emit(OrderPlaced(order_id=42), order_placed)
    ```

    """
    if is_async_factory(factory):
        return AsyncEventConfig(factory, event_type, name=name, payload_type=payload_type)
    return EventConfig(factory, event_type, name=name, payload_type=payload_type)


def is_request[**P, S: Any, T: EventType](event: EventConfig[P, S, T]) -> bool:
    """
    Report whether the event carries a subscripted Request discriminant.

    Adapters branch on this at dispatch time to select request/reply
    semantics over fire-and-forget dispatch.

    :param event: Any {py:class}`EventConfig`.
    :returns: `True` when the event's discriminant is a subscripted
        {py:class}`Request` (or a subscripted subclass of it), `False`
        otherwise.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Distinguishing a request/reply event from a pub/sub event

    from stratae.events.event import PubSub, Request, event, is_request

    class OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    class BookFound:
        def __init__(self, title: str) -> None:
            self.title = title

    class FindBook:
        def __init__(self, query: str) -> None:
            self.query = query

    order_placed = event(OrderPlaced, PubSub)
    find_book = event(FindBook, Request[BookFound])

    assert not is_request(order_placed)
    assert is_request(find_book)
    ```

    """
    origin: object = get_origin(event.event_type)
    return isinstance(origin, type) and issubclass(origin, Request)


def reply_type[**P, S: Any, R](event: EventConfig[P, S, Request[R]]) -> type[R]:
    """
    Recover the reply type from a request event's discriminant.

    Adapters use this to know what a blocking emit resolves to at runtime.
    For example, to deserialize a reply arriving from the far side of a
    broker.

    :param event: An {py:class}`EventConfig` whose discriminant is a
        subscripted {py:class}`Request`.
    :returns: The type `Request` was subscripted with when the event was
        defined.
    :raises TypeError: If the event's discriminant is not a subscripted
        `Request`. Unreachable for type-checked callers; guards dynamic
        construction paths.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Recovering a request event's reply type at runtime

    from stratae.events.event import Request, event, reply_type

    class FindBook:
        def __init__(self, query: str) -> None:
            self.query = query

    class BookFound:
        def __init__(self, title: str) -> None:
            self.title = title

    find_book = event(FindBook, Request[BookFound])
    assert reply_type(find_book) is BookFound
    ```

    """
    if not is_request(event):
        raise TypeError(_NOT_A_REQUEST)
    return cast(type[R], get_args(event.event_type)[0])
