# stratae: generated stub of tests.unit.scripts.stub_sample; regenerate it rather than editing it.
# Its injected functions are typed without their injected parameters.

from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING, Annotated, Final, Literal

from stratae.depends import Depends
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

type StripeDep = Annotated[Stripe, Depends(get_stripe)]

class Session:
    """A session over a payment client."""

    opened: bool = ...

    def __init__(self, stripe: Stripe) -> None:
        """Open a session over `stripe`."""

@request.cache()
def get_session() -> Generator[Session]:
    """Provide a session, once per request."""

@ledgers.cache()
def open_ledger() -> AsyncGenerator[Session]:
    """Provide a ledger's session, once per request."""

type SessionDep = Annotated[Session, Depends(get_session)]

class Order:
    """An order."""

    def __init__(self, order_id: OrderId) -> None:
        """Make the order `order_id`."""

    def total(self, tax: Decimal | None = None) -> int:
        """Return the order's total."""

    @property
    def label(self) -> str:
        """The order's label."""

async def refund(order: Order, *, reason: str = "late") -> str:
    """Refund `order`."""

class Account:
    """An account with an injected constructor."""

    def __init__(self, owner: str, kind: Literal["user", "team"]) -> None:
        """Open an account for `owner`."""

    def orders(self) -> list[Order]:
        """Return the account's orders."""
