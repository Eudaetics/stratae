# Project Walkthrough

The rest of this guide builds one system end to end: a pipeline that fulfills orders, checking inventory, charging payment, and marking orders shipped or backordered. Along the way it picks up a piece of Stratae exactly when the requirements call for it, and by the end the same code has grown from a batch script into a live endpoint backed by an async payment confirmation.

## Building a new tool

The business needs orders processed in batches: for each order, check whether the item is in stock, charge the customer if so, and mark the order shipped; if not, mark it backordered instead. This section introduces the sample order data and a plain-Python version of that pipeline. No Stratae yet, just the logic.

## A second payment gateway

The business signs with a second payment processor, and which one charges a given order is a business decision, not something every function in the pipeline should have to know or pass along. This section shows the naive approach, a `gateway_name` argument threaded through every call, then replaces it with `Depends`/`@inject` so `get_payment_gateway` decides and nothing downstream sees the parameter.

## Warehouses are region-specific

US orders need to check inventory against a US warehouse system, EU orders against an EU one. This section chains a second provider off the first: region decides warehouse, warehouse decides how inventory gets checked, each provider only aware of the one it depends on directly.

## Guarding refunds

Refunds get introduced as a new capability, and only support leads should be able to issue one, not just anyone who can call the function. This section uses `stratae.checks.require` to gate `issue_refund` itself, so the guard holds no matter how the function is reached.

## Making the guard observable

Finance wants a trail: every refund issued gets logged, every blocked attempt raises an alert. This section uses `stratae.events` to decouple that reporting from the guard logic, so `issue_refund` doesn't need to know who's listening.

## Testing both branches

QA needs to exercise both the allowed and blocked refund paths without standing up a real staff roster or auth system. This section uses `override` to swap the caller identity for the duration of a test, then swap it back.

## Scoping the gateway connection to a run

The payment gateway connection is expensive to establish, and the batch job currently reconnects for every single order. This section wraps the connection in `stratae.lifecycle`'s `resource` and caches it on a `Scope` for the whole run, so it opens once and closes when the run ends.

## Orders move to SQLite

The in-memory order list won't survive what's coming: once an endpoint accepts an order and a queue confirmation finishes it later, possibly in a different process entirely, order state has to outlive a single function call. This section swaps `get_orders` and the customer/employee lookups for a SQLite-backed repository; nothing that reads them changes, since they were already sitting behind `Depends` for other reasons. The payoff is durability nobody designed for up front, added without a rewrite.

## Going live: from batch job to checkout-triggered endpoint

The business wants fulfillment to start the moment a customer completes checkout, not wait for the next nightly batch. This section wraps the inventory check and payment request behind a route using `stratae.integrations.fastapi`; the endpoint accepts the order and kicks off charging, returning right away rather than waiting on a final shipped-or-backordered outcome.

## Payment confirmation finishes the order asynchronously

The payment gateway confirms charges out of band: the endpoint's charge request only starts things, and the real outcome, paid or failed, arrives later as a message on a queue. This section uses `stratae.integrations.rabbitmq` to consume that confirmation and finish the order, then emits an event that emails the customer the outcome, the same `stratae.events` pattern already established for the refund audit trail, now with a second listener.

## Adding a new region later

A third region launches with its own warehouse. This is a short capstone showing what has to change to support it, one provider, against everything that doesn't.

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
  status       TEXT NOT NULL
  placed_on    TEXT NOT NULL

refunds
  refund_id    TEXT PRIMARY KEY
  order_id     TEXT NOT NULL REFERENCES orders(order_id)
  employee_id  TEXT NOT NULL REFERENCES employees(employee_id)
  amount_cents INTEGER NOT NULL
  issued_at    TEXT NOT NULL
```
