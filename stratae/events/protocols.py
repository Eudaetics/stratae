"""
Structural protocols for the stratae event system.

{py:class}`Producer` and {py:class}`Consumer` describe the emit and handle
sides of a bus adapter structurally. Any class with a compatible method
satisfies the protocol, sync or async, without inheriting from it.
{py:class}`EmitCallable` describes a single bound emit call. It's
parameterized over a concrete return type `R`, so a specific binding — e.g. a
{py:class}`BoundEvent <stratae.events.bound.BoundEvent>`'s `emitter` — can be
checked against its own return type instead of `Any`.

```{rubric} Example:
```
```{code-block} python
:caption: Checking that a bus satisfies Producer and Consumer structurally

from stratae.events import Consumer, Producer

class MinimalBus:
    def emit(self, payload, event, config, *, serializer=None):
        return None

    def handle(self, config, fn=None):
        return fn

bus = MinimalBus()
assert isinstance(bus, Producer)
assert isinstance(bus, Consumer)
```

See {py:class}`EmitCallable`, {py:class}`Producer`, and {py:class}`Consumer`
for additional examples.

"""

from typing import Any, Callable, Protocol, runtime_checkable

from stratae.events.event import EventConfig, EventType


@runtime_checkable
class EmitCallable[**P, S: Any, T: EventType, C: Any, R: Any](Protocol):
    """
    Structural protocol for a single bound emit call.

    Captures the call shape of {py:meth}`Producer.emit`: payload, event, and
    config in, some adapter-defined result out. It's parameterized over a
    concrete `R` instead of `Any`, though. That lets a specific binding —
    e.g. a {py:class}`BoundEvent <stratae.events.bound.BoundEvent>`'s
    `emitter` — be checked against its own return type.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Binding a plain function as an emitter typed via EmitCallable

    from typing import Any, Callable
    from stratae.events import EmitCallable, EventConfig, PubSub, event

    class OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    def emit_order(
        payload: OrderPlaced,
        event: EventConfig[..., OrderPlaced, PubSub],
        config: Any,
        *,
        serializer: Callable[[OrderPlaced], Any] | None = None,
    ) -> int:
        return payload.order_id

    order_placed = event(OrderPlaced, PubSub)
    emitter: EmitCallable = emit_order
    assert emitter(OrderPlaced(order_id=42), order_placed, None) == 42
    ```

    """

    def __call__(
        self,
        payload: S,
        event: EventConfig[P, S, T],
        config: C,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R:
        """
        Dispatch a constructed event payload.

        :param payload: The constructed payload instance to dispatch.
        :param event: The {py:class}`EventConfig <stratae.events.event.EventConfig>`
            definition being emitted.
        :param config: Adapter-specific routing configuration.
        :param serializer: Serializer the payload will be sent to prior to routing.
        :returns: Adapter-defined result of dispatching the payload.
        """
        ...


@runtime_checkable
class Producer(Protocol):
    """
    Structural protocol for the emit side of the event system.

    Any class with a compatible `emit` method satisfies this protocol,
    whether sync or async. {py:class}`BoundEvent <stratae.events.bound.BoundEvent>`
    calls `emit` when invoked. Adapters implement it to perform the actual
    dispatch.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Checking that an in-process bus satisfies the Producer protocol

    from stratae.events import DirectBus, Producer, PubSub, event

    class OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    order_placed = event(OrderPlaced, PubSub)
    bus = DirectBus()
    assert isinstance(bus, Producer)

    received: list[int] = []

    @bus.handle(order_placed)
    def on_order_placed(order: OrderPlaced) -> None:
        received.append(order.order_id)

    bus.emit(OrderPlaced(order_id=42), order_placed, None)
    assert received == [42]
    ```

    """

    def emit[**P, S: Any, T: EventType](
        self,
        payload: S,
        event: EventConfig[P, S, T],
        config: Any,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> Any:
        """
        Dispatch a constructed event payload.

        :param payload: The constructed payload instance to dispatch.
        :param event: The {py:class}`EventConfig <stratae.events.event.EventConfig>`
            definition being emitted.
        :param config: Adapter-specific routing configuration.
        :param serializer: Encodes `payload` before dispatch. Format is
            adapter-defined (bytes, a JSON string, etc.). When omitted,
            the adapter falls back to its own default serializer, if any.
        :returns: Adapter-defined result of dispatching the payload. Sync
            implementations return it directly; async implementations
            return a coroutine.
        """
        ...


@runtime_checkable
class Consumer(Protocol):
    """
    Structural protocol for the receive side of the event system.

    Any class with a compatible `handle` method satisfies this protocol.
    `handle` is the user-facing API for registering handlers against a
    config key, replacing the former `subscribe`. It covers every consumer
    pattern: pub/sub handlers, repliers, RPC responders, and so on.

    The internal dispatch mechanism is an implementation detail of each
    adapter and isn't part of this protocol. That includes how a queued
    message, or an in-process event, actually triggers the registered
    handlers.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Registering a handler for a pub/sub event via handle

    from stratae.events import Consumer, DirectBus, PubSub, event

    class LogMessage:
        def __init__(self, text: str) -> None:
            self.text = text

    log_message = event(LogMessage, PubSub)
    bus = DirectBus()
    assert isinstance(bus, Consumer)

    logged: list[str] = []

    @bus.handle(log_message)
    def write_to_log(entry: LogMessage) -> None:
        logged.append(entry.text)

    bus.emit(LogMessage(text="hello"), log_message, None)
    assert logged == ["hello"]
    ```

    """

    def handle(self, config: Any, fn: Callable[[Any], Any] | None = None) -> Any:
        """
        Register a handler for the given config.

        Can be called directly, passing the handler function as `fn`, or
        used as a decorator factory by omitting `fn` and applying the
        result to a function definition instead.

        :param config: The adapter-specific key used to route events to handlers.
        :param fn: When supplied, registers `fn` directly and returns it.
            When omitted, returns a decorator that registers and returns
            the handler.
        """
        ...
