# Project Walkthrough

The rest of this guide builds one system end to end: a pipeline that fulfills orders, checking inventory, charging payment, and marking orders shipped or backordered. Along the way it picks up a piece of Stratae exactly when the requirements call for it, and by the end the same code has grown from a batch script into a live endpoint backed by an async payment confirmation.

## Building a new tool

The business needs orders processed in batches: for each order, check whether the item is in stock, charge the customer if so, and mark the order shipped; if not, mark it backordered instead.

:::{dropdown} Sample data used throughout this guide
```{code-block} python
from dataclasses import dataclass
from uuid import UUID, uuid4

@dataclass
class Customer:
    customer_id: UUID
    email: str
    name: str

@dataclass
class Order:
    order_id: UUID
    customer_id: UUID
    region: str  # "US" or "EU"
    sku: str
    quantity: int
    total_cents: int
    gateway: str  # which processor charged it, stamped once fulfilled; "" until then
    status: str
    placed_on: str

CUSTOMERS = [
    Customer(uuid4(), "alice@example.com", "Alice"),
    Customer(uuid4(), "bob@example.com", "Bob"),
    Customer(uuid4(), "carol@example.com", "Carol"),
]

ORDERS = [
    Order(uuid4(), CUSTOMERS[0].customer_id, "US", "SWB-32OZ", 2, 4998, "", "pending", "2026-07-20"),
    Order(uuid4(), CUSTOMERS[1].customer_id, "US", "WEB-100", 1, 1999, "", "pending", "2026-07-20"),
    Order(uuid4(), CUSTOMERS[2].customer_id, "EU", "SWB-32OZ", 5, 12495, "", "pending", "2026-07-21"),
]
```
:::

It starts by checking stock, charging the customer, and marking the order shipped or backordered:

````{example} A plain fulfillment pipeline
```{code-block} python
STOCK = {
    "SWB-32OZ": 3,  # 32oz stainless steel water bottle
    "WEB-100": 0,  # wireless earbuds
}

def check_inventory(order: Order) -> bool:
    return STOCK.get(order.sku, 0) >= order.quantity

def charge_payment(order: Order) -> bool:
    print(f"charging {order.total_cents} cents for order {order.order_id}")
    return True

def fulfill_order(order: Order) -> str:
    if not check_inventory(order):
        return "backordered"
    if not charge_payment(order):
        return "payment_failed"
    return "shipped"

for order in ORDERS:
    print(f"{order.order_id}: {fulfill_order(order)}")
```
```{output}
charging 4998 cents for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f: shipped
c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f: backordered
9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d: backordered
```
````

No Stratae yet, just the logic.

## A second payment gateway

The business is migrating off Stripe onto Adyen. Rather than build per-order routing logic they'll throw away once the migration finishes, ops wants one flag that sends all new orders to whichever processor is currently active, flippable without a deploy. That's not something every function in the pipeline should have to know about or pass along. The easy way to write that is a plain argument:

```{code-block} python
def charge_via_stripe(order: Order) -> bool:
    print(f"charging {order.total_cents} cents via Stripe for order {order.order_id}")
    return True

def charge_via_adyen(order: Order) -> bool:
    print(f"charging {order.total_cents} cents via Adyen for order {order.order_id}")
    return True

def charge_payment(order: Order, gateway_name: str) -> bool:
    if gateway_name == "adyen":
        return charge_via_adyen(order)
    return charge_via_stripe(order)

def fulfill_order(order: Order, gateway_name: str) -> str:
    if not check_inventory(order):
        return "backordered"
    if not charge_payment(order, gateway_name):
        return "payment_failed"
    return "shipped"
```

That works today because there's exactly one call site. But refunds are coming later, and reversing a charge has to go back through whichever gateway originally took it, a second caller that would otherwise have to duplicate this same `gateway_name` lookup and threading. Instead, mark a parameter with `Annotated[T, Depends(provider)]` and decorate the function with `@inject`. Now the decision moves out of the signature entirely. The provider runs, and its result is passed in without any caller ever seeing that parameter:

````{example} Injecting which payment gateway runs
```{code-block} python
import os
from typing import Annotated, Callable
from stratae.depends import Depends, inject

def get_gateway_name() -> str:
    return os.environ.get("PAYMENT_GATEWAY", "stripe")

type PaymentGateway = Callable[[Order], bool]

@inject
def get_payment_gateway(gateway_name: Annotated[str, Depends(get_gateway_name)]) -> PaymentGateway:
    return charge_via_adyen if gateway_name == "adyen" else charge_via_stripe

@inject
def fulfill_order(order: Order, charge_payment: Annotated[PaymentGateway, Depends(get_payment_gateway)]) -> str:
    if not check_inventory(order):
        return "backordered"
    if not charge_payment(order):
        return "payment_failed"
    return "shipped"

for order in ORDERS:
    print(f"{order.order_id}: {fulfill_order(order)}")
```
```{output}
charging 4998 cents via Stripe for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f: shipped
c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f: backordered
9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d: backordered
```
````

`charge_payment` is injected. `order` still isn't, that's still a plain argument here. Only which gateway function runs is decided by `stratae.depends`, based on the `PAYMENT_GATEWAY` cutover flag read in `get_gateway_name`. Both gateway functions take an `Order` and return a `bool`, so `fulfill_order` doesn't need to know or care which one ran, and ops can flip the flag mid-migration without anyone touching this code.

## Warehouses are region-specific

The EU fulfillment operation came from an acquisition, and it still runs on its original warehouse system separate from the US side's homegrown one. The two will eventually get consolidated once there's a replacement. Until then, which one a given order has to check depends on the order itself, its shipping region, not on some run-wide setting the way the payment gateway was.

That's a problem for `Depends`: providers are resolved with no visibility into the plain arguments of the function they're injected into, so `order.region` can't reach `get_warehouse_api` directly, the same way `get_gateway_name` never needed to see any particular order. `stratae.context` bridges that gap. A `Context` is a settable, callable value that plugs into `Depends` like any other provider, and the caller sets it right before the value is needed:

````{example} Routing inventory checks through a per-order region
```{code-block} python
from stratae.context import Context

US_STOCK = {
    "SWB-32OZ": 3,
    "WEB-100": 0,
}

EU_STOCK = {
    "SWB-32OZ": 5,
    "WEB-100": 2,
}

def check_us_stock(order: Order) -> bool:
    print(f"checking US warehouse for order {order.order_id}")
    return US_STOCK.get(order.sku, 0) >= order.quantity

def check_eu_stock(order: Order) -> bool:
    print(f"checking EU warehouse for order {order.order_id}")
    return EU_STOCK.get(order.sku, 0) >= order.quantity

region_ctx = Context[str]("region")

type InventoryChecker = Callable[[Order], bool]

@inject
def get_warehouse_api(region: Annotated[str, Depends(region_ctx)]) -> InventoryChecker:
    return check_eu_stock if region == "EU" else check_us_stock

@inject
def fulfill_order(
    order: Order,
    check_inventory: Annotated[InventoryChecker, Depends(get_warehouse_api)],
    charge_payment: Annotated[PaymentGateway, Depends(get_payment_gateway)],
) -> str:
    if not check_inventory(order):
        return "backordered"
    if not charge_payment(order):
        return "payment_failed"
    return "shipped"

for order in ORDERS:
    with region_ctx.use(order.region):
        print(f"{order.order_id}: {fulfill_order(order)}")
```
```{output}
checking US warehouse for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
charging 4998 cents via Stripe for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f: shipped
checking US warehouse for order c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f
c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f: backordered
checking EU warehouse for order 9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d
charging 12495 cents via Stripe for order 9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d
9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d: shipped
```
````

The third order used to come back backordered in the earlier sections' shared stock table; checked against the EU warehouse's own count, it actually has enough and ships. `region_ctx.use` is set by the loop, right before `fulfill_order` is called, because injected parameters are resolved while the call is being built, before the function body runs, so the value has to already be in place by then. `get_warehouse_api` never sees `order` at all, only whatever `region_ctx` currently holds; `fulfill_order` never sees `region` at all, only the `check_inventory` function it resolves to.

## Guarding refunds

Refunds are a new capability, and any support rep can process one, but only a support lead should actually be able to issue one. Anyone can *call* `issue_refund`, but `stratae.checks`' `require` guards the function itself, so it refuses no matter how it's reached.

Reversing a charge has to go back through whichever gateway actually took it, not whichever gateway happens to be active *now*. The migration might have moved on since the order was charged, so `get_gateway_name`'s live value is the wrong thing to ask. `fulfill_order` has to record which gateway it used at charge time, stamped onto the order itself, and `issue_refund` reads that stamp back instead. That stamp is per-order data reaching a nested provider again, the same problem `order.region` ran into with `get_warehouse_api`, so it gets the same fix:

````{example} Guarding who can issue a refund
```{code-block} python
from stratae.checks import require

@dataclass
class Employee:
    employee_id: UUID
    name: str
    role: str  # "support" or "lead"

def get_caller() -> Employee:
    return Employee(uuid4(), "Bob", "support")

type CallerDep = Annotated[Employee, Depends(get_caller)]

@inject
def caller_can_issue_refund(caller: CallerDep) -> None:
    if caller.role != "lead":
        raise PermissionError(f"{caller.name} cannot issue refunds")

def refund_via_stripe(order: Order) -> bool:
    print(f"refunding {order.total_cents} cents via Stripe for order {order.order_id}")
    return True

def refund_via_adyen(order: Order) -> bool:
    print(f"refunding {order.total_cents} cents via Adyen for order {order.order_id}")
    return True

gateway_ctx = Context[str]("charged_via")

@inject
def get_refund_gateway(gateway_name: Annotated[str, Depends(gateway_ctx)]) -> PaymentGateway:
    return refund_via_adyen if gateway_name == "adyen" else refund_via_stripe

@inject
def fulfill_order(
    order: Order,
    check_inventory: Annotated[InventoryChecker, Depends(get_warehouse_api)],
    gateway_name: Annotated[str, Depends(get_gateway_name)],
    charge_payment: Annotated[PaymentGateway, Depends(get_payment_gateway)],
) -> str:
    if not check_inventory(order):
        return "backordered"
    if not charge_payment(order):
        return "payment_failed"
    order.gateway = gateway_name
    return "shipped"

@require(caller_can_issue_refund)
@inject
def issue_refund(order: Order, refund: Annotated[PaymentGateway, Depends(get_refund_gateway)]) -> bool:
    return refund(order)

with region_ctx.use(ORDERS[0].region):
    print(fulfill_order(ORDERS[0]))

with gateway_ctx.use(ORDERS[0].gateway):
    try:
        issue_refund(ORDERS[0])
    except PermissionError as e:
        print(f"blocked: {e}")
```
```{output}
checking US warehouse for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
charging 4998 cents via Stripe for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
shipped
blocked: Bob cannot issue refunds
```
````

Fulfilling the order stamps `ORDERS[0].gateway` to `"stripe"`, since that's whatever `PAYMENT_GATEWAY` resolved to at charge time. Refunding it sets `gateway_ctx` to that stamp before calling `issue_refund`, the same way the warehouse loop sets `region_ctx` before calling `fulfill_order`. `issue_refund` never runs here regardless, since Bob is on support, not a lead, and `caller_can_issue_refund` raises before `refund` is ever called. `get_warehouse_api` and `get_payment_gateway` didn't need to change; neither depends on `get_caller` at all.

## Making the guard observable

Finance wants a trail: every refund issued gets logged, every blocked attempt raises an alert. `stratae.events` lets `issue_refund` and its guard raise these as plain events, with no idea who's listening or what they'll do about it:

````{example} Logging issued refunds and alerting on blocked attempts
```{code-block} python
from stratae.events import DirectBus, Event, PubSub

class RefundIssued:
    def __init__(self, caller: str) -> None:
        self.caller = caller

class RefundBlocked:
    def __init__(self, caller: str) -> None:
        self.caller = caller

refund_issued = Event(PubSub, RefundIssued)
refund_blocked = Event(PubSub, RefundBlocked)

bus = DirectBus()
notify_issued = bus.bind(refund_issued, factory=RefundIssued)
notify_blocked = bus.bind(refund_blocked, factory=RefundBlocked)

@bus.handle(refund_issued)
def log_refund(e: RefundIssued) -> None:
    print(f"audit log: {e.caller} issued a refund")

@bus.handle(refund_blocked)
def alert_finance(e: RefundBlocked) -> None:
    print(f"blocked: {e.caller} tried to issue a refund")

def get_caller() -> Employee:
    return Employee(uuid4(), "Dana", "lead")

@inject
def caller_can_issue_refund(caller: CallerDep) -> None:
    if caller.role != "lead":
        notify_blocked(caller=caller.name)
        raise PermissionError(f"{caller.name} cannot issue refunds")

@require(caller_can_issue_refund)
@inject
def issue_refund(
    order: Order, refund: Annotated[PaymentGateway, Depends(get_refund_gateway)], caller: CallerDep
) -> bool:
    result = refund(order)
    notify_issued(caller=caller.name)
    return result

os.environ["PAYMENT_GATEWAY"] = "adyen"  # the migration kept moving after this order was charged

with gateway_ctx.use(ORDERS[0].gateway):
    print(issue_refund(ORDERS[0]))
```
```{output}
refunding 4998 cents via Stripe for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
audit log: Dana issued a refund
True
```
````

`PAYMENT_GATEWAY` is `"adyen"` by the time this refund runs, but it still goes out through Stripe, because `get_refund_gateway` reads `gateway_ctx`, set from `ORDERS[0].gateway`, not the live setting. `get_caller` now returns Dana, a lead, so `caller_can_issue_refund` lets `issue_refund` through instead of raising. The audit log line prints after the refund itself, since `notify_issued` only fires once `refund` has actually succeeded. `get_gateway_name`, `fulfill_order`, and `get_warehouse_api` didn't need to change here, so they aren't redefined.

## Testing both branches

QA needs to exercise both the allowed and blocked refund paths without standing up a real staff roster or auth system. `override` replaces a provider's value for the duration of a `with` block, target and all. Swap `get_caller` to a `"support"` `Employee`, and the guard refuses exactly as it would for a real support rep, with no real caller or auth system involved:

````{example} Overriding the caller to test both branches
```{code-block} python
from stratae.depends import override

with override(get_caller, Employee(uuid4(), "Bob", "support")):
    with gateway_ctx.use(ORDERS[0].gateway):
        try:
            issue_refund(ORDERS[0])
        except PermissionError:
            pass

with gateway_ctx.use(ORDERS[0].gateway):
    print(issue_refund(ORDERS[0]))
```
```{output}
blocked: Bob tried to issue a refund
refunding 4998 cents via Stripe for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
audit log: Dana issued a refund
True
```
````

Inside the `with` block, Bob requests the refund and gets refused and alerted on, same as before the caller was ever changed to Dana. Outside it, `get_caller` is back to Dana, unchanged, and the same request succeeds. Neither `issue_refund` nor `caller_can_issue_refund` had to change to test either path.

## Scoping the gateway connection to a run

Every charge so far has quietly implied its own connection to the gateway. The batch job reconnects for every single order, which is fine for three orders and wasteful for three thousand. `stratae.lifecycle` scopes a resource to a unit of work, here the whole run rather than each call. Wrap the connection in `resource` and register it with `.cache()` on the scope itself, and the code after `yield` becomes cleanup, run once the scope exits:

````{example} Caching the gateway connection for the whole run
```{code-block} python
from stratae.lifecycle import Scope, resource

run = Scope("run", isolation="shared")

@run.cache()
@resource
def get_gateway_connection():
    print("connecting to the payment gateway")
    try:
        yield "gateway-connection"  # stand-in for a real client/session object
    finally:
        print("closing the payment gateway connection")

@inject
def charge_via_stripe(order: Order, conn: Annotated[str, Depends(get_gateway_connection)]) -> bool:
    print(f"charging {order.total_cents} cents via Stripe for order {order.order_id}")
    return True

@inject
def charge_via_adyen(order: Order, conn: Annotated[str, Depends(get_gateway_connection)]) -> bool:
    print(f"charging {order.total_cents} cents via Adyen for order {order.order_id}")
    return True

with run.activate():
    for order in ORDERS:
        with region_ctx.use(order.region):
            print(fulfill_order(order))
```
```{output}
connecting to the payment gateway
checking US warehouse for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
charging 4998 cents via Stripe for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
shipped
checking US warehouse for order c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f
backordered
checking EU warehouse for order 9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d
charging 12495 cents via Stripe for order 9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d
shipped
closing the payment gateway connection
```
````

`charge_via_stripe` and `charge_via_adyen` both take a plain `Order` and return a `bool` once `@inject` strips `conn` from their exposed signatures, so `get_payment_gateway` still sees `PaymentGateway = Callable[[Order], bool]` and doesn't need to change. The connection opens once, on the first charge, no matter how many orders the run processes, and closes automatically when `run.activate()` exits, whether that's after one order or a thousand. `fulfill_order`, `get_warehouse_api`, `get_payment_gateway`, and `get_gateway_name` didn't need to change at all; none of them know the gateway functions now depend on a connection.

## Orders move to SQLite

The in-memory `ORDERS` list won't survive what's coming: once an endpoint accepts an order and a queue confirmation finishes it later, possibly in a different process entirely, order state has to outlive a single function call. Swap the flat list for a real table, and read it back through a `get_orders` provider instead of the module-level name:

````{example} Persisting orders and customers in SQLite
```{code-block} python
import sqlite3

DB = sqlite3.connect(":memory:")
DB.execute("""
    CREATE TABLE orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        region TEXT NOT NULL,
        sku TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        total_cents INTEGER NOT NULL,
        gateway TEXT NOT NULL,
        status TEXT NOT NULL,
        placed_on TEXT NOT NULL
    )
""")
DB.execute("""
    CREATE TABLE customers (
        customer_id TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        name TEXT NOT NULL
    )
""")
DB.executemany(
    "INSERT INTO customers VALUES (?, ?, ?)",
    [(str(c.customer_id), c.email, c.name) for c in CUSTOMERS],
)
DB.executemany(
    "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [
        (
            str(o.order_id), str(o.customer_id), o.region, o.sku,
            o.quantity, o.total_cents, o.gateway, o.status, o.placed_on,
        )
        for o in ORDERS
    ],
)
DB.commit()

def _row_to_order(row: tuple) -> Order:
    order_id, customer_id, region, sku, quantity, total_cents, gateway, status, placed_on = row
    return Order(UUID(order_id), UUID(customer_id), region, sku, quantity, total_cents, gateway, status, placed_on)

def get_orders() -> list[Order]:
    rows = DB.execute("SELECT * FROM orders").fetchall()
    return [_row_to_order(row) for row in rows]

def get_customer(customer_id: UUID) -> Customer:
    row = DB.execute("SELECT * FROM customers WHERE customer_id = ?", (str(customer_id),)).fetchone()
    return Customer(UUID(row[0]), row[1], row[2])

def save_order(order: Order) -> None:
    DB.execute(
        "UPDATE orders SET gateway = ?, status = ? WHERE order_id = ?",
        (order.gateway, order.status, str(order.order_id)),
    )
    DB.commit()

with run.activate():
    for order in get_orders():
        with region_ctx.use(order.region):
            order.status = fulfill_order(order)
        save_order(order)

print([o.status for o in get_orders()])
```
```{output}
connecting to the payment gateway
checking US warehouse for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
charging 4998 cents via Stripe for order 3f2a1c9e-8b7d-4e2a-9f1a-6d4c2b1e9a3f
checking US warehouse for order c1d2e3f4-5a6b-7c8d-9e0f-1a2b3c4d5e6f
checking EU warehouse for order 9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d
charging 12495 cents via Stripe for order 9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d
closing the payment gateway connection
['shipped', 'backordered', 'shipped']
```
````

`fulfill_order` returns the same strings it always has; the loop is the only thing that changed, since it now has somewhere durable to write the result. Employees move into their own table the same way, `get_caller` reading Dana's row instead of returning a hardcoded value, not shown here since the shape is identical. `check_inventory`, `caller_can_issue_refund`, `issue_refund`, and everything wired through `Depends` didn't need to change at all; they never knew `ORDERS` was a Python list to begin with. The final `get_orders()` call proves it: a fresh read sees the statuses that were written, not whatever's still sitting in a variable somewhere.

## Going live: from batch job to checkout-triggered endpoint

The business wants fulfillment to start the moment a customer completes checkout, not wait for the next nightly batch. Charging can no longer happen synchronously either way: a real charge request to a gateway comes back "pending," with the actual outcome arriving later, so the endpoint can only kick it off and return, not wait on a final shipped-or-backordered result. `@inject` already strips resolved parameters from a function's exposed signature, and FastAPI inspects that same signature to build a route, so the pipeline's existing providers slot straight into a route with no adapter layer in between:

````{example} Accepting an order at checkout
```{code-block} python
from typing import Any, Coroutine
from uuid import UUID, uuid4
from fastapi import FastAPI
from fastapi.testclient import TestClient

async def request_charge_via_stripe(order: Order) -> None:
    print(f"requesting Stripe charge of {order.total_cents} cents for order {order.order_id}")

async def request_charge_via_adyen(order: Order) -> None:
    print(f"requesting Adyen charge of {order.total_cents} cents for order {order.order_id}")

type ChargeRequest = Callable[[Order], Coroutine[Any, Any, None]]

@inject
def get_charge_requester(gateway_name: Annotated[str, Depends(get_gateway_name)]) -> ChargeRequest:
    return request_charge_via_adyen if gateway_name == "adyen" else request_charge_via_stripe

def create_order_record(order: Order) -> None:
    DB.execute(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            str(order.order_id), str(order.customer_id), order.region, order.sku,
            order.quantity, order.total_cents, order.gateway, order.status, order.placed_on,
        ),
    )
    DB.commit()

def check_inventory_for(order: Order) -> bool:
    return {"EU": check_eu_stock}.get(order.region, check_us_stock)(order)

app = FastAPI()

@app.post("/orders")
@inject
async def create_order(
    customer_id: str,
    region: str,
    sku: str,
    quantity: int,
    total_cents: int,
    gateway_name: Annotated[str, Depends(get_gateway_name)],
    request_charge: Annotated[ChargeRequest, Depends(get_charge_requester)],
) -> dict[str, str]:
    order = Order(uuid4(), UUID(customer_id), region, sku, quantity, total_cents, "", "pending", "2026-07-22")
    if not check_inventory_for(order):
        order.status = "backordered"
        create_order_record(order)
        return {"order_id": str(order.order_id), "status": order.status}
    order.gateway = gateway_name
    create_order_record(order)
    await request_charge(order)
    return {"order_id": str(order.order_id), "status": order.status}

with TestClient(app) as client:
    response = client.post("/orders", params={
        "customer_id": str(CUSTOMERS[0].customer_id),
        "region": "US",
        "sku": "SWB-32OZ",
        "quantity": 1,
        "total_cents": 2499,
    })
    print(response.json())
```
```{output}
checking US warehouse for order 4b6f8a2d-1e3c-4f5a-9b7d-2c8e6f1a3d5b
requesting Stripe charge of 2499 cents for order 4b6f8a2d-1e3c-4f5a-9b7d-2c8e6f1a3d5b
{'order_id': '4b6f8a2d-1e3c-4f5a-9b7d-2c8e6f1a3d5b', 'status': 'pending'}
```
````

FastAPI only ever sees `customer_id`, `region`, `sku`, `quantity`, and `total_cents` when it inspects `create_order`'s signature; `gateway_name` and `request_charge` are already gone by the time it looks, stripped by `@inject` the same way `cur` was in `stratae.integrations.fastapi`'s own example. `check_inventory_for` calls `check_us_stock`/`check_eu_stock` directly rather than going through `get_warehouse_api` and `region_ctx`: FastAPI calls `create_order` itself, so there's no code of ours running immediately beforehand to set the context, the way the batch loop or the refund call sites could. `Context` needs its caller to set it right before the call; a framework calling the function directly removes that hook, short of adding request middleware, which is more machinery than this needs. `check_us_stock`, `check_eu_stock`, and `get_gateway_name` are still the exact same functions from earlier sections, just reached a different way here. `request_charge_via_stripe` and `request_charge_via_adyen` are new, since the old `charge_via_stripe`/`charge_via_adyen` return a settled `bool` and there's no settled answer yet to return; the response comes back `"pending"` because that's genuinely all that's true at this point.

## Payment confirmation finishes the order asynchronously

The payment gateway confirms charges out of band: `request_charge_via_stripe` only started things, and the real outcome, paid or failed, arrives later as a message on a queue. `stratae.integrations.rabbitmq` consumes that confirmation and finishes the order, the same event-driven shape as the refund audit trail, just backed by a broker instead of an in-process bus:

````{example} Finishing an order from a queued confirmation
<!--- skip: next -->
```{code-block} python
import asyncio
from stratae.events import Event, PubSub
from stratae.integrations.rabbitmq import (
    RabbitMQConfig,
    RabbitMQConsumeConfig,
    RabbitMQConsumer,
    RabbitMQPublisher,
)

class PaymentConfirmed:
    def __init__(self, order_id: str, outcome: str) -> None:
        self.order_id = order_id
        self.outcome = outcome  # "paid" or "failed"

payment_confirmed_event = Event(PubSub, PaymentConfirmed)

consumer = RabbitMQConsumer("amqp://guest:guest@localhost/")

def get_order(order_id: str) -> Order:
    row = DB.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
    return _row_to_order(row)

def send_confirmation_email(customer: Customer, order: Order) -> None:
    print(f"emailing {customer.email}: your order {order.order_id} is {order.status}")

@consumer.handle(
    payment_confirmed_event,
    config=RabbitMQConsumeConfig(queue="payment-confirmations"),
)
def on_payment_confirmed(confirmation: PaymentConfirmed) -> None:
    order = get_order(confirmation.order_id)
    order.status = "shipped" if confirmation.outcome == "paid" else "payment_failed"
    save_order(order)
    send_confirmation_email(get_customer(order.customer_id), order)

async def main() -> None:
    async with (
        consumer,  # declares the queue before anything is published
        RabbitMQPublisher("amqp://guest:guest@localhost/") as publisher,
    ):
        confirm_payment = publisher.bind(
            payment_confirmed_event,
            factory=PaymentConfirmed,
            config=RabbitMQConfig("payments", "payment.confirmed"),
        )
        await confirm_payment(order_id="4b6f8a2d-1e3c-4f5a-9b7d-2c8e6f1a3d5b", outcome="paid")
        await asyncio.sleep(0.05)  # wait for delivery

asyncio.run(main())
```
```{output}
emailing alice@example.com: your order 4b6f8a2d-1e3c-4f5a-9b7d-2c8e6f1a3d5b is shipped
```
````

`save_order` and `get_customer` are the exact same functions the SQLite section wrote; nothing about persistence needed to anticipate a queue existing someday. `get_order`, a lookup by id, is a small addition, since fetching one specific order by its id is a need this section introduces, not one the batch job or the endpoint had. `create_order`, `check_inventory_for`, `caller_can_issue_refund`, `issue_refund`, and the refund events are all untouched too, and the checkout endpoint already returned before any of this runs, so none of them even know this section exists. That's the whole payoff from the start: the payment gateway changed how it confirms a charge, from an answer handed back immediately to one that arrives later on a queue, and the only new code is the piece that actually deals with the new shape, a consumer and two request functions. Everything else, fulfillment, refunds, warehouses, checkout, kept working exactly as written.

## Adding a new region later

A third region, APAC, launches with its own warehouse. `get_warehouse_api` is the only place region ever decided anything, so it's the only place that changes:

````{example} Wiring in a third warehouse
```{code-block} python
APAC_STOCK = {
    "SWB-32OZ": 4,
    "WEB-100": 1,
}

def check_apac_stock(order: Order) -> bool:
    print(f"checking APAC warehouse for order {order.order_id}")
    return APAC_STOCK.get(order.sku, 0) >= order.quantity

@inject
def get_warehouse_api(region: Annotated[str, Depends(region_ctx)]) -> InventoryChecker:
    return {"EU": check_eu_stock, "APAC": check_apac_stock}.get(region, check_us_stock)

apac_order = Order(uuid4(), CUSTOMERS[0].customer_id, "APAC", "WEB-100", 1, 1999, "", "pending", "2026-07-23")

with run.activate():
    with region_ctx.use(apac_order.region):
        print(fulfill_order(apac_order))
```
```{output}
connecting to the payment gateway
checking APAC warehouse for order 7c4e9b1a-3f6d-4a8e-9c2b-1d5f7a3e9b6c
charging 1999 cents via Stripe for order 7c4e9b1a-3f6d-4a8e-9c2b-1d5f7a3e9b6c
shipped
closing the payment gateway connection
```
````

`fulfill_order`, `get_payment_gateway`, `caller_can_issue_refund`, `issue_refund`, `create_order`, and the RabbitMQ consumer didn't change, and don't need to know APAC exists. Whichever new provider decides the new case is the only thing that ever has to.

## Wrap-up

Pointers to the deeper guides, [Dependency Injection](dependency-injection), [Checks](checks.md), [Events](events.md), and [Lifecycle](lifecycle.md), plus the [API reference](../api-reference) for full signature-level detail.

## Data model (reference)

Not part of the narrative, just captured here so the decisions don't get lost. IDs are UUIDs throughout, stored as `TEXT` once SQLite exists. Money is cents-as-int, not `float` or `Decimal`, since nothing here does fractional-cent math and int is the frictionless choice across JSON, SQLite, and message payloads alike.

```
Customer
  customer_id: UUID
  email: str
  name: str

Employee
  employee_id: UUID
  name: str
  role: str            # "support" or "lead"

Order
  order_id: UUID
  customer_id: UUID
  region: str          # "US" or "EU", later "APAC"
  sku: str
  quantity: int
  total_cents: int
  gateway: str         # which processor charged it, stamped at fulfillment; "" until then
  status: str          # pending / shipped / backordered / payment_failed / refunded
  placed_on: str        # ISO timestamp
```

SQLite schema, once the "Orders move to SQLite" section introduces it:

```sql
customers
  customer_id  TEXT PRIMARY KEY
  email        TEXT NOT NULL
  name         TEXT NOT NULL

employees
  employee_id  TEXT PRIMARY KEY
  name         TEXT NOT NULL
  role         TEXT NOT NULL      -- support/lead

orders
  order_id     TEXT PRIMARY KEY
  customer_id  TEXT NOT NULL REFERENCES customers(customer_id)
  region       TEXT NOT NULL
  sku          TEXT NOT NULL
  quantity     INTEGER NOT NULL
  total_cents  INTEGER NOT NULL
  gateway      TEXT NOT NULL      -- which processor charged it
  status       TEXT NOT NULL
  placed_on    TEXT NOT NULL

refunds
  refund_id    TEXT PRIMARY KEY
  order_id     TEXT NOT NULL REFERENCES orders(order_id)
  employee_id  TEXT NOT NULL REFERENCES employees(employee_id)
  amount_cents INTEGER NOT NULL
  issued_at    TEXT NOT NULL
```
