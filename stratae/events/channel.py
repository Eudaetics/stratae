"""Channel abstraction for routing events between publishers and subscribers."""

from __future__ import annotations

from typing import Any, ClassVar


class Channel:
    """
    A named routing destination for the stratae event system.

    A ``Channel`` is the shared reference between publishers and subscribers.
    Publishers emit events on a channel; subscribers register handlers for
    specific event types on that channel.  Multiple event types may flow
    through the same channel, discriminated at dispatch time by event type.

    ``Channel`` carries no dependency on any bus or emitter and may be
    defined at module level and imported independently by both sides.

    Channel names must be unique within a process — creating two channels
    with the same name raises ``ValueError``.

    Example::

        orders = Channel("orders", version=1)
    """

    _registry: ClassVar[dict[str, Channel]] = {}

    def __init__(self, name: str, **kwargs: Any) -> None:
        """
        Create a channel with the given routing name.

        Raises:
            ValueError: If a channel with the same name has already been created.

        Args:
            name:     The routing name (e.g. a topic string) used by adapters
                      to identify this channel on the wire.  Must be unique
                      across all channels in the process.
            **kwargs: Arbitrary metadata (version, priority, etc.) stored as
                      ``.metadata`` and available to adapters.

        """
        if name in Channel._registry:
            raise ValueError(f"A channel named {name!r} already exists.")
        Channel._registry[name] = self

        self.name = name
        self.metadata: dict[str, Any] = kwargs
