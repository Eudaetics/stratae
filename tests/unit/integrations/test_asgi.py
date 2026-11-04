"""
Test the ASGI envelope middleware with raw ASGI calls.

This test suite verifies the following behaviors:

- A request runs inside a new root envelope, and the previous envelope is restored after it.
- Events emitted while handling a request share its correlation id.
- A valid correlation id header is adopted, with a new message id.
- A missing, malformed or non-UUID header starts a new chain, and the request still runs.
- Only the first correlation id header is used.
- With header trust off, the header is ignored.
- A custom header name is matched without regard to case, and is the one echoed.
- The response start carries the correlation id after the app's own headers.
- With echoing off, the response is sent unchanged.
- A WebSocket connection runs inside an envelope, and its accept carries the correlation id.
- Lifespan events pass through with no envelope.
- Concurrent requests never see each other's envelope.
"""

import asyncio
from typing import Any
from uuid import UUID, uuid4

import pytest

from stratae.events import DirectBus, PubSub
from stratae.events.envelope import Envelope
from stratae.integrations.asgi import ASGIScope, EnvelopeMiddleware, Message, Receive, Send

_CALLER = UUID("0b9d7a52-6f1e-4c3a-9d2b-5e8f1a7c4d60")


def _current() -> Envelope:
    """Return the active envelope, failing the test if there's none."""
    envelope = Envelope.current()
    assert envelope is not None, "no active envelope"
    return envelope


class _App:
    """An ASGI app recording the envelope it ran in, and sending a fixed response."""

    def __init__(self, start: Message | None = None) -> None:
        self.envelopes: list[Envelope] = []
        self._start = start or {"type": "http.response.start", "status": 200, "headers": []}

    async def __call__(self, scope: ASGIScope, receive: Receive, send: Send) -> None:
        self.envelopes.append(_current())
        await send(self._start)
        await send({"type": "http.response.body", "body": b"ok"})


def _http(*headers: tuple[bytes, bytes]) -> ASGIScope:
    """Return an HTTP request scope with the given headers."""
    return {"type": "http", "method": "GET", "path": "/", "headers": list(headers)}


async def _receive() -> Message:
    await asyncio.sleep(0)
    return {"type": "http.request", "body": b""}


async def _call(middleware: EnvelopeMiddleware, scope: ASGIScope) -> list[Message]:
    """Run one connection through the middleware, returning the messages it sent."""
    sent: list[Message] = []

    async def send(message: Message) -> None:
        await asyncio.sleep(0)
        sent.append(message)

    await middleware(scope, _receive, send)
    return sent


def _envelope(app: _App) -> Envelope:
    """Return the one envelope the app ran in."""
    [envelope] = app.envelopes
    return envelope


async def test_request_runs_in_new_root_envelope():
    """
    A request should run inside a new root envelope, restoring the previous one after.

    Given: The middleware, called inside an outer envelope
    When: A request with no correlation id header is handled
    Then: The app sees a root envelope unrelated to the outer one, and the outer is restored
    """
    # Arrange
    app = _App()
    middleware = EnvelopeMiddleware(app)

    # Act
    with Envelope.scope() as outer:
        await _call(middleware, _http())
        after = Envelope.current()

    # Assert
    envelope = _envelope(app)
    assert envelope.causation_id is None
    assert envelope.correlation_id != outer.correlation_id
    assert after is outer


async def test_emitted_events_share_correlation_id():
    """
    Events emitted while handling a request should share its correlation id.

    Given: An app that emits an event on a bus that opens envelopes
    When: A request with a correlation id header is handled
    Then: The handler's envelope carries that correlation id, caused by the request
    """
    # Arrange
    event = PubSub(str)
    bus = DirectBus(use_envelope=True)
    emit = bus.bind(event)
    handled: list[Envelope] = []
    bus.handle(event)(lambda payload: handled.append(_current()))
    requests: list[Envelope] = []

    async def app(scope: ASGIScope, receive: Receive, send: Send) -> None:
        await asyncio.sleep(0)
        requests.append(_current())
        emit("placed")

    # Act
    await _call(EnvelopeMiddleware(app), _http((b"x-correlation-id", str(_CALLER).encode())))

    # Assert
    [request], [child] = requests, handled
    assert child.correlation_id == _CALLER
    assert child.causation_id == request.message_id


async def test_valid_header_adopted_with_new_message_id():
    """
    A valid correlation id header should be adopted, with a new message id.

    Given: A request carrying other headers, then a correlation id header
    When: It's handled
    Then: The envelope's correlation id is the caller's, and its message id is new
    """
    # Arrange
    app = _App()
    scope = _http((b"accept", b"*/*"), (b"x-correlation-id", str(_CALLER).encode()))

    # Act
    await _call(EnvelopeMiddleware(app), scope)

    # Assert
    envelope = _envelope(app)
    assert envelope.correlation_id == _CALLER
    assert envelope.message_id != _CALLER
    assert envelope.causation_id is None


@pytest.mark.parametrize("value", [b"", b"?", b"not-a-uuid", b"req-1234", "\xe9".encode("latin-1")])
async def test_invalid_header_starts_new_chain(value: bytes):
    """
    A malformed or non-UUID correlation id header should start a new chain.

    Given: A request whose correlation id header isn't a UUID
    When: It's handled
    Then: The app runs in a new root envelope and its response is sent
    """
    # Arrange
    app = _App()

    # Act
    sent = await _call(EnvelopeMiddleware(app), _http((b"x-correlation-id", value)))

    # Assert
    assert _envelope(app).causation_id is None
    assert [message["type"] for message in sent] == ["http.response.start", "http.response.body"]


async def test_first_header_used():
    """
    Only the first correlation id header should be used.

    Given: A request with a malformed correlation id header, then a valid one
    When: It's handled
    Then: The valid one isn't adopted
    """
    # Arrange
    app = _App()
    scope = _http((b"x-correlation-id", b"?"), (b"x-correlation-id", str(_CALLER).encode()))

    # Act
    await _call(EnvelopeMiddleware(app), scope)

    # Assert
    assert _envelope(app).correlation_id != _CALLER


async def test_untrusted_header_ignored():
    """
    With header trust off, the correlation id header should be ignored.

    Given: The middleware with trust_header=False
    When: A request with a valid correlation id header is handled
    Then: The envelope's correlation id is new, and that's the one echoed
    """
    # Arrange
    app = _App()
    middleware = EnvelopeMiddleware(app, trust_header=False)

    # Act
    sent = await _call(middleware, _http((b"x-correlation-id", str(_CALLER).encode())))

    # Assert
    envelope = _envelope(app)
    assert envelope.correlation_id != _CALLER
    assert sent[0]["headers"] == [(b"x-correlation-id", str(envelope.correlation_id).encode())]


async def test_custom_header_matched_and_echoed():
    """
    A custom header name should be matched regardless of case, and be the one echoed.

    Given: The middleware with header="X-Request-ID"
    When: A request carrying x-request-id is handled
    Then: Its value is adopted, and the response carries it under x-request-id
    """
    # Arrange
    app = _App()
    middleware = EnvelopeMiddleware(app, header="X-Request-ID")

    # Act
    sent = await _call(middleware, _http((b"x-request-id", str(_CALLER).encode())))

    # Assert
    assert _envelope(app).correlation_id == _CALLER
    assert sent[0]["headers"] == [(b"x-request-id", str(_CALLER).encode())]


async def test_response_start_carries_correlation_id():
    """
    The response start should carry the correlation id after the app's own headers.

    Given: An app whose response start has a header of its own
    When: A request with a correlation id header is handled
    Then: The start has both headers, and the body is sent unchanged
    """
    # Arrange
    start = {"type": "http.response.start", "status": 200, "headers": [(b"a", b"1")]}
    app = _App(start)

    # Act
    sent = await _call(EnvelopeMiddleware(app), _http((b"x-correlation-id", str(_CALLER).encode())))

    # Assert
    assert sent[0]["headers"] == [(b"a", b"1"), (b"x-correlation-id", str(_CALLER).encode())]
    assert start["headers"] == [(b"a", b"1")]
    assert sent[1] == {"type": "http.response.body", "body": b"ok"}


async def test_no_echo_sends_response_unchanged():
    """
    With echoing off, the response should be sent unchanged.

    Given: The middleware with echo_header=False
    When: A request is handled
    Then: The response start has no correlation id header
    """
    # Arrange
    start = {"type": "http.response.start", "status": 200, "headers": [(b"a", b"1")]}
    app = _App(start)

    # Act
    sent = await _call(EnvelopeMiddleware(app, echo_header=False), _http())

    # Assert
    assert sent[0] is start


async def test_websocket_accept_carries_correlation_id():
    """
    A WebSocket connection should run in an envelope, and its accept carry the correlation id.

    Given: A WebSocket app that accepts with no headers of its own
    When: A connection with a correlation id header is handled
    Then: The app runs in the caller's chain, and the accept carries the header
    """
    # Arrange
    app = _App({"type": "websocket.accept"})
    scope: ASGIScope = {
        "type": "websocket",
        "path": "/",
        "headers": [(b"x-correlation-id", str(_CALLER).encode())],
    }

    # Act
    sent = await _call(EnvelopeMiddleware(app), scope)

    # Assert
    assert _envelope(app).correlation_id == _CALLER
    assert sent[0] == {
        "type": "websocket.accept",
        "headers": [(b"x-correlation-id", str(_CALLER).encode())],
    }


async def test_lifespan_passes_through():
    """
    Lifespan events should pass through with no envelope.

    Given: A lifespan app
    When: The lifespan scope runs through the middleware
    Then: The app sees no envelope, and is given the server's own send
    """
    # Arrange
    seen: list[Any] = []

    async def app(scope: ASGIScope, receive: Receive, send: Send) -> None:
        await asyncio.sleep(0)
        seen.extend([Envelope.current(), send])

    async def send(message: Message) -> None:
        await asyncio.sleep(0)
        pytest.fail("not sent")

    # Act
    await EnvelopeMiddleware(app)({"type": "lifespan"}, _receive, send)

    # Assert
    assert seen == [None, send]


async def test_concurrent_requests_isolated():
    """
    Concurrent requests should never see each other's envelope.

    Given: An app that waits mid-request until both requests have started
    When: Two requests with different correlation ids run concurrently
    Then: Each sees its own correlation id before and after the wait
    """
    # Arrange
    started = asyncio.Barrier(2)
    seen: dict[UUID, list[UUID]] = {}

    async def app(scope: ASGIScope, receive: Receive, send: Send) -> None:
        caller = UUID(dict(scope["headers"])[b"x-correlation-id"].decode())
        before = _current().correlation_id
        await started.wait()
        seen[caller] = [before, _current().correlation_id]

    middleware = EnvelopeMiddleware(app)
    first, second = uuid4(), uuid4()

    # Act
    await asyncio.gather(
        _call(middleware, _http((b"x-correlation-id", str(first).encode()))),
        _call(middleware, _http((b"x-correlation-id", str(second).encode()))),
    )

    # Assert
    assert seen == {first: [first, first], second: [second, second]}
