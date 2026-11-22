"""
A module typed by its generated `.pyi`, kept current by `test_stubs.py`.

Regenerate with `python -m stratae.scripts.signatures tests.unit.scripts.stub_sample`.
"""

import asyncio
from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING, Annotated, Final, Literal

from stratae.depends import Depends, inject
from stratae.lifecycle import AsyncScope, Scope

if TYPE_CHECKING:
    from decimal import Decimal

__all__ = ["RETRIES", "TIMEOUT", "Account", "Order", "Session", "get_session", "refund"]

TIMEOUT: Final = 30
RETRIES = 3
type OrderId = int
request = Scope("stub_sample_request")
ledgers = AsyncScope("stub_sample_ledgers")


class Stripe:
    """A payment client."""


def get_stripe() -> Stripe:
    """Provide a payment client."""
    return Stripe()


type StripeDep = Annotated[Stripe, Depends(get_stripe)]


class Session:
    """A session over a payment client."""

    opened: bool = True

    def __init__(self, stripe: Stripe) -> None:
        """Open a session over `stripe`."""
        self.stripe = stripe


@request.cache()
@inject
def get_session(stripe: StripeDep) -> Generator[Session]:
    """Provide a session, once per request."""
    yield Session(stripe)


@ledgers.cache()
@inject
async def open_ledger(stripe: StripeDep) -> AsyncGenerator[Session]:
    """Provide a ledger's session, once per request."""
    await asyncio.sleep(0)
    yield Session(stripe)


type SessionDep = Annotated[Session, Depends(get_session)]


class Order:
    """An order."""

    def __init__(self, order_id: OrderId) -> None:
        """Make the order `order_id`."""
        self.order_id = order_id

    @inject
    def total(self, session: SessionDep, tax: "Decimal | None" = None) -> int:
        """Return the order's total."""
        return int(session.opened) + int(tax or 0)

    @property
    def label(self) -> str:
        """The order's label."""
        return f"order {self.order_id}"


@inject
async def refund(session: SessionDep, order: Order, *, reason: str = "late") -> str:
    """Refund `order`."""
    await asyncio.sleep(0)
    return f"{type(session).__name__} {order.label} {reason}"


class Account:
    """An account with an injected constructor."""

    @inject
    def __init__(self, stripe: StripeDep, owner: str, kind: Literal["user", "team"]) -> None:
        """Open an account for `owner`."""
        self.stripe = stripe
        self.owner = owner
        self.kind = kind

    def orders(self) -> list["Order"]:
        """Return the account's orders."""
        return [Order(1)]
