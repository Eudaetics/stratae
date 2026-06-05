"""Direct, in-process synchronous event bus."""

from typing import Any

from stratae.events.envelope import EventEnvelope
from stratae.events.event import BoundEvent, EventSchema
from stratae.events.mixins.publish import BasicPublisher
from stratae.events.mixins.subscribe import Subscriber


class LocalBus(BasicPublisher[None], Subscriber[BoundEvent[Any, None, None]]):
    """
    In-process, synchronous event bus with no routing config.

    The ``BoundEvent`` returned by ``publish`` serves as both the emit handle and
    the subscription key.  Pass it as ``config`` to ``subscribe`` to register a
    handler; call it to emit an event. Each call to ``subscribe`` is an independent
    registration; the same callable may be subscribed multiple times.

    Args:
        use_envelope: When ``True``, each emission opens a scoped
                      ``EventEnvelope`` for correlation tracking.  Defaults to
                      ``False`` for pure in-process dispatch with no envelope
                      overhead.

    Example::

        bus = LocalBus()

        create_book = bus.publish(Book)

        @bus.subscribe(create_book)
        def save_book(book: Book) -> None: ...

        create_book(title="Dune", author="Herbert")

    """

    def __init__(self, *, use_envelope: bool = False) -> None:
        """Initialise the bus with optional envelope tracking."""
        super().__init__()
        self._use_envelope = use_envelope

    def emit_publish[**P](self, payload: EventSchema, event: BoundEvent[P, None, None]) -> None:
        """
        Open a scoped envelope and dispatch the payload to registered handlers.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            event:   The ``BoundEvent`` used as the handler lookup key.

        """
        if self._use_envelope:
            with EventEnvelope.scope():
                self.handle_subscribe(payload, config=event)
        else:
            self.handle_subscribe(payload, config=event)

    def handle_subscribe[**P](
        self, payload: EventSchema, *, config: BoundEvent[P, None, None]
    ) -> None:
        """
        Invoke every handler registered for the given bound event with the payload.

        Args:
            payload: The constructed ``EventSchema`` instance to dispatch.
            config:  The ``BoundEvent`` used as the handler lookup key.

        """
        exceptions: list[Exception] = []
        for handler in self.get_handlers(config):
            try:
                handler(payload)
            except Exception as exc:
                exceptions.append(exc)
        if exceptions:
            raise ExceptionGroup("Handler Errors", exceptions)
