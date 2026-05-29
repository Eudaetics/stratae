"""Event handler wrapper carrying async detection semantics."""

from inspect import iscoroutinefunction
from typing import Callable

from stratae.events.event import EventMeta, EventSchema


class Handler[Metadata: (EventMeta | None), R]:
    """
    Wraps an event handler callable with async detection and value equality.

    ``R`` is the return type of the wrapped callable.  For async handlers
    ``R`` will be an ``Awaitable`` of the resolved type.

    Stores the callable and detects at construction time whether it is a
    coroutine function, so dispatchers can branch once on ``is_async`` rather
    than inspecting the callable on every dispatch.

    Calling a ``Handler`` delegates directly to the wrapped callable.  For
    async handlers, the result is a coroutine that the caller can ``await``;
    a sync wrapper around an async callable naturally returns the coroutine
    object, so ``await handler(payload)`` works without ``Handler.__call__``
    itself being declared ``async``.

    Identity is object identity.  Each call to ``subscribe`` produces a
    distinct ``Handler`` instance, so the same callable may be registered
    multiple times (with different metadata, or independently) without
    deduplication.  Callers unsubscribe by passing the ``Handler`` returned
    from ``subscribe``.
    """

    def __init__(self, call: Callable[[EventSchema], R], meta: Metadata = None) -> None:
        """
        Wrap a callable as an event handler.

        Args:
            call: The sync or async callable to wrap.  Must accept a
                  single ``EventSchema`` instance as its argument.
            meta: Optional adapter-specific metadata used for filtering at
                  dispatch time.  Never passed to ``call``.

        """
        self.call = call
        self.meta = meta
        self.is_async: bool = iscoroutinefunction(call)

    def __call__(self, payload: EventSchema) -> R:
        """
        Invoke the wrapped callable with the given payload.

        Args:
            payload: The event instance to pass to the handler.

        Returns:
            For sync handlers, the return value directly.  For async handlers,
            a coroutine that the caller should ``await``.

        """
        return self.call(payload)
