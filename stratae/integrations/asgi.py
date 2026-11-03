"""
ASGI middleware that opens an envelope around each request.

{py:class}`EnvelopeMiddleware` wraps any ASGI application, such as Starlette or FastAPI. It
runs each HTTP request and WebSocket connection inside its own
{py:class}`Envelope <stratae.events.envelope.Envelope>`. Events emitted while handling it
are children of that envelope, so they share its correlation id, and so does anything that
reads {py:func}`Envelope.current <stratae.events.envelope.Envelope.current>`, such as a log
filter. Lifespan events pass through untouched.

A request that sends a valid UUID in the correlation id header joins that chain. The header
is `x-correlation-id` by default, the same one Stratae's broker adapters send. A missing,
malformed or non-UUID value starts a new chain, and never fails the request. Only the
correlation id is taken from the caller: each request gets a new message id, since consumers
deduplicate on it. Pass `trust_header=False` to ignore the header and start every request's
chain afresh, as a public edge service might.

The response carries the correlation id in the same header, so a caller can quote it when
reporting a problem. Pass `echo_header=False` to leave it off. On a WebSocket, it's sent when
the connection is accepted.

The middleware doesn't touch lifecycle scopes. It works with or without
{py:func}`scoped_route <stratae.integrations.starlette.scoped_route>`. Add it as the
outermost middleware, so that other middleware, exception handlers and 404s run inside the
envelope too.

````{example} Events emitted in a request share its correlation id
```{code-block} python
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient
from stratae.events import DirectBus, PubSub
from stratae.events.envelope import Envelope
from stratae.integrations.asgi import EnvelopeMiddleware

class OrderPlaced:
    def __init__(self, item: str) -> None:
        self.item = item

order_placed = PubSub(OrderPlaced)
bus = DirectBus(use_envelope=True)
announce = bus.bind(order_placed, factory=OrderPlaced)
chains: dict[str, str] = {}

@bus.handle(order_placed)
def on_order_placed(order: OrderPlaced) -> None:
    chains[order.item] = str(Envelope.current().correlation_id)

async def place_order(request: Request) -> JSONResponse:
    announce(item=request.query_params["item"])
    return JSONResponse({"status": "placed"})

app = Starlette(
    routes=[Route("/orders", place_order, methods=["POST"])],
    middleware=[Middleware(EnvelopeMiddleware)],
)

with TestClient(app) as client:
    caller = {"x-correlation-id": "0b9d7a52-6f1e-4c3a-9d2b-5e8f1a7c4d60"}
    lamp = client.post("/orders", params={"item": "lamp"}, headers=caller)
    anvil = client.post("/orders", params={"item": "anvil"}, headers={"x-correlation-id": "?"})

print(chains["lamp"], lamp.headers["x-correlation-id"])
print(anvil.status_code, chains["anvil"] == anvil.headers["x-correlation-id"])
```
```{output}
0b9d7a52-6f1e-4c3a-9d2b-5e8f1a7c4d60 0b9d7a52-6f1e-4c3a-9d2b-5e8f1a7c4d60
200 True
```
````

````{example} Putting the correlation id on every log line
```{code-block} python
import logging
import sys
from uuid import UUID
from stratae.events.envelope import Envelope

class CorrelationFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        envelope = Envelope.current()
        record.correlation_id = envelope.correlation_id if envelope else "-"
        return True

handler = logging.StreamHandler(sys.stdout)
handler.addFilter(CorrelationFilter())
handler.setFormatter(logging.Formatter("%(correlation_id)s %(message)s"))
log = logging.getLogger("orders")
log.addHandler(handler)
log.setLevel(logging.INFO)

log.info("outside a request")
with Envelope.scope(Envelope(correlation_id=UUID("0b9d7a52-6f1e-4c3a-9d2b-5e8f1a7c4d60"))):
    log.info("inside a request")
```
```{output}
- outside a request
0b9d7a52-6f1e-4c3a-9d2b-5e8f1a7c4d60 inside a request
```
````
"""

from typing import Any, Awaitable, Callable, MutableMapping
from uuid import UUID, uuid4

from stratae.events.envelope import CORRELATION_ID_HEADER, Envelope

type ASGIScope = MutableMapping[str, Any]
type Message = MutableMapping[str, Any]
type Receive = Callable[[], Awaitable[Message]]
type Send = Callable[[Message], Awaitable[None]]
type ASGIApp = Callable[[ASGIScope, Receive, Send], Awaitable[None]]

_CONNECTIONS = frozenset({"http", "websocket"})
"""ASGI scope types that get an envelope; anything else, such as lifespan, passes through."""

_RESPONSE_STARTS = frozenset({"http.response.start", "websocket.accept"})
"""ASGI messages that carry the response's headers."""


def _uuid(value: bytes) -> UUID | None:
    """Parse a header value as a UUID, or return `None` if it isn't one."""
    try:
        return UUID(value.decode("latin-1"))
    except ValueError:
        return None


class EnvelopeMiddleware:
    """Run each HTTP request and WebSocket connection inside its own envelope."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        header: str = CORRELATION_ID_HEADER,
        trust_header: bool = True,
        echo_header: bool = True,
    ) -> None:
        """
        Wrap `app`, opening an envelope around each of its requests and connections.

        :param app: The ASGI application to wrap.
        :param header: The header carrying the correlation id, in requests and responses.
        :param trust_header: Whether a request's correlation id header is adopted. If false,
            every request starts a new chain.
        :param echo_header: Whether the response carries the request's correlation id.
        """
        self.app = app
        self._header = header.lower().encode("latin-1")
        self._trust_header = trust_header
        self._echo_header = echo_header

    async def __call__(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
        """Handle one ASGI connection, inside a new envelope if it's a request."""
        if scope["type"] not in _CONNECTIONS:
            await self.app(scope, receive, send)
            return
        envelope = Envelope(correlation_id=self._correlation_id(scope))
        with Envelope.scope(envelope):
            await self.app(scope, receive, self._echoing(envelope, send))

    def _correlation_id(self, scope: ASGIScope) -> UUID:
        """Return the request's correlation id if it's trusted and valid, else a new one."""
        if self._trust_header:
            # ASGI servers send header names lowercased; the first match is the one used.
            for name, value in scope["headers"]:
                if name == self._header:
                    return _uuid(value) or uuid4()
        return uuid4()

    def _echoing(self, envelope: Envelope, send: Send) -> Send:
        """Return `send`, adding the correlation id header to the response if echoing."""
        if not self._echo_header:
            return send
        header = (self._header, str(envelope.correlation_id).encode("latin-1"))

        async def echoing(message: Message) -> None:
            if message["type"] in _RESPONSE_STARTS:
                message = {**message, "headers": [*message.get("headers", ()), header]}
            await send(message)

        return echoing
