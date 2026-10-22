"""
Structural protocols for the event system.

{py:class}`Producer` and {py:class}`Consumer` describe the emit and handle sides of a bus
adapter. {py:class}`EmitCallable` describes a single emitter with a concrete return type.
"""

from typing import Any, Callable, Protocol, runtime_checkable

from stratae.events.event import DispatchPattern, Event


@runtime_checkable
class EmitCallable[T: DispatchPattern[Any, Any], S, C, R, Signal: bool](Protocol):
    """Structural protocol for one emitter, shaped like {py:meth}`Producer.emit`."""

    def __call__(
        self,
        event: Event[T, S, Signal],
        config: C,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> R:
        """
        Dispatch a payload.

        :param event: The {py:class}`Event <stratae.events.event.Event>` being emitted.
        :param config: Adapter-specific routing config.
        :param payload: The payload to dispatch.
        :param serializer: Encodes the payload; defaults to the adapter's own.
        :returns: Adapter-defined result, or a coroutine resolving to it.
        """
        ...


@runtime_checkable
class Producer(Protocol):
    """Structural protocol for the emit side of a bus adapter, sync or async."""

    def emit[T: DispatchPattern[Any, Any], S, Signal: bool](
        self,
        event: Event[T, S, Signal],
        config: Any,
        payload: S,
        *,
        serializer: Callable[[S], Any] | None = None,
    ) -> Any:
        """
        Dispatch a payload.

        :param event: The {py:class}`Event <stratae.events.event.Event>` being emitted.
        :param config: Adapter-specific routing config.
        :param payload: The payload to dispatch.
        :param serializer: Encodes the payload; defaults to the adapter's own.
        :returns: Adapter-defined result, or a coroutine resolving to it.
        """
        ...


@runtime_checkable
class Consumer(Protocol):
    """Structural protocol for the handle side of a bus adapter."""

    def handle(self, config: Any, fn: Callable[[Any], Any] | None = None) -> Any:
        """
        Register a handler, as a decorator or direct call.

        :param config: The routing key for the handler.
        :param fn: The handler. If omitted, returns a decorator.
        """
        ...
