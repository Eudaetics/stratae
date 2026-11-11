"""
Unit tests for when both direct buses build an emit's headers.

Every test runs against DirectBus and AsyncDirectBus.

This test suite verifies the following behaviors:

DirectBus and AsyncDirectBus (headers):
- An emit with an envelope or hooks but no middleware builds no headers, and still reaches
  its handler.
- An emit with publish or handle middleware builds its headers, once.
"""

from collections.abc import Callable
from contextlib import nullcontext
from inspect import isawaitable
from typing import Any
from unittest.mock import Mock

import pytest

import stratae.events.direct as direct_module
from stratae.events import (
    AsyncDirectBus,
    DirectBus,
    EnvelopeConfig,
    Hooks,
    MessageHeaders,
    Middleware,
    PubSub,
)


class _Placed:
    def __init__(self, order_id: int) -> None:
        self.order_id = order_id


_placed = PubSub(_Placed)


def _before(event: object, payload: object) -> None:
    return None


def _nothing(call: object) -> nullcontext[None]:
    return nullcontext()


@pytest.fixture(params=[DirectBus, AsyncDirectBus], ids=["DirectBus", "AsyncDirectBus"])
def kind(request: pytest.FixtureRequest) -> Callable[..., DirectBus | AsyncDirectBus]:
    """Return each kind of bus."""
    return request.param


@pytest.fixture
def built(monkeypatch: pytest.MonkeyPatch) -> list[MessageHeaders]:
    """Record every MessageHeaders the direct buses build."""
    made: list[MessageHeaders] = []

    def recording(*args: Any, **kwargs: Any) -> MessageHeaders:
        headers = MessageHeaders(*args, **kwargs)
        made.append(headers)
        return headers

    monkeypatch.setattr(direct_module, "MessageHeaders", recording)
    return made


async def _emit(bus: DirectBus | AsyncDirectBus, hooks: Hooks[Any, Any] | None) -> None:
    """Emit on either bus, awaiting the async bus's result."""
    result = bus.emit(_placed, None, _Placed(1), hooks=hooks)
    if isawaitable(result):
        await result


@pytest.mark.parametrize(
    ("options", "hooks"),
    [
        pytest.param({"envelope": EnvelopeConfig()}, None, id="envelope"),
        pytest.param({}, Hooks(before=(_before,)), id="hooks"),
        pytest.param({"envelope": EnvelopeConfig()}, Hooks(before=(_before,)), id="both"),
    ],
)
async def test_emit_without_middleware_builds_no_headers(
    kind: Callable[..., DirectBus | AsyncDirectBus],
    built: list[MessageHeaders],
    options: dict[str, Any],
    hooks: Hooks[Any, Any] | None,
) -> None:
    """
    An emit nothing reads headers from should build none.

    Given: A bus with an envelope, an emit with hooks, or both, and no middleware
    When: An event is emitted
    Then: Its handler is called, and no MessageHeaders was built
    """
    # Arrange
    bus = kind(**options)
    handler = Mock()
    bus.handle(_placed, handler)

    # Act
    await _emit(bus, hooks)

    # Assert
    handler.assert_called_once()
    assert built == []


@pytest.mark.parametrize(
    "middleware",
    [
        pytest.param(Middleware(publish=(_nothing,)), id="publish"),
        pytest.param(Middleware(handle=(_nothing,)), id="handle"),
    ],
)
async def test_emit_with_middleware_builds_headers_once(
    kind: Callable[..., DirectBus | AsyncDirectBus],
    built: list[MessageHeaders],
    middleware: Middleware,
) -> None:
    """
    An emit whose middleware reads headers should build them once.

    Given: A bus with publish or handle middleware
    When: An event is emitted
    Then: Its handler is called, and one MessageHeaders was built
    """
    # Arrange
    bus = kind(middleware=middleware)
    handler = Mock()
    bus.handle(_placed, handler)

    # Act
    await _emit(bus, None)

    # Assert
    handler.assert_called_once()
    assert len(built) == 1
