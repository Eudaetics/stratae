"""End-to-end tests for events composed with dependency injection and lifecycle."""

from typing import Any

import pytest

from stratae.depends import Depends, Injected, inject
from stratae.events import DirectBus, Event, PubSub, Request
from stratae.lifecycle import Lifecycle, Scope


@pytest.fixture
def lifecycle():
    """Provide a Lifecycle instance for testing."""
    yield Lifecycle([Scope("application", "shared")])


def test_order_flow_with_injected_handlers(lifecycle: Lifecycle):
    """
    End-to-end test simulating order processing over the direct bus.

    - A pub/sub event fans an order notification out to a recording handler
    - A request event prices an order through a single responder
    - Both handlers resolve a shared store through lifecycle-cached injection
    """
    # Arrange: payloads, events, and an application-scoped store
    bus = DirectBus()

    class OrderPlaced:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    class Quote:
        def __init__(self, order_id: int, total: int) -> None:
            self.order_id = order_id
            self.total = total

    class PriceOrder:
        def __init__(self, order_id: int) -> None:
            self.order_id = order_id

    @lifecycle.cache("application")
    def order_store() -> dict[int, dict[str, Any]]:
        return {}

    order_placed = Event(OrderPlaced, PubSub)
    price_order = Event(PriceOrder, Request[Quote])
    place_order = bus.bind(order_placed, factory=OrderPlaced)
    request_quote = bus.bind(price_order, factory=PriceOrder)

    @bus.handle(order_placed)
    @inject
    def _(
        order: OrderPlaced,
        store: Injected[dict[int, dict[str, Any]], Depends(order_store)],
    ) -> None:
        store[order.order_id] = {"status": "placed"}

    @bus.handle(price_order)
    @inject
    def _(
        request: PriceOrder,
        store: Injected[dict[int, dict[str, Any]], Depends(order_store)],
    ) -> Quote:
        assert request.order_id in store
        return Quote(order_id=request.order_id, total=100)

    # Act & Assert: place and price an order within the application scope
    with lifecycle.start("application"):
        place_order(order_id=42)
        quote = request_quote(order_id=42)

        assert quote.order_id == 42
        assert quote.total == 100
        assert order_store()[42]["status"] == "placed"
