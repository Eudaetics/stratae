"""
Integration test for the "Building a service" tutorial, against a live RabbitMQ broker.

The doc tests run the tutorial's in-process examples, and skip the one that connects to
RabbitMQ. This test runs every example in order, including that one, and checks what each
prints against the output the page shows.

This test suite verifies the following behaviors:

- Each of the tutorial's examples prints exactly the output the page shows, from placing an
  order over HTTP to shipping it from the queue on its second attempt.
"""

import asyncio
import contextlib
import io
import os
import re
from pathlib import Path
from typing import Any

import pytest
from aiormq import connect

pytestmark = pytest.mark.rabbitmq

_GUIDE = Path(__file__).parents[2] / "docs" / "tutorials" / "service.md"

_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost/")

_EXAMPLE = re.compile(
    r"```\{code-block\} python\n(?P<code>.*?)\n```\n(?:```\{output\}\n(?P<output>.*?)\n```\n)?",
    re.DOTALL,
)

_QUEUES = (
    "orders.shipping",
    "orders.shipping.retry.1000",
    "orders.shipping.retry.10000",
    "orders.shipping.retry.60000",
    "orders.shipping.dead-letter",
)


def _examples() -> list[tuple[str, str | None]]:
    """Return each example's code, and the output the page shows for it, if any."""
    return [(m["code"], m["output"]) for m in _EXAMPLE.finditer(_GUIDE.read_text())]


def _run(examples: list[tuple[str, str | None]]) -> list[tuple[str, str | None]]:
    """Run the examples in one namespace, returning what each printed and was meant to."""
    namespace: dict[str, Any] = {"__name__": "service_guide"}
    printed: list[tuple[str, str | None]] = []
    for index, (code, expected) in enumerate(examples):
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            exec(compile(code, f"{_GUIDE.name}[{index}]", "exec"), namespace)  # noqa: S102
        printed.append((captured.getvalue().rstrip("\n"), expected))
    return printed


async def _delete_topology() -> None:
    """Delete the queues and exchange the tutorial declares, so each run starts clean."""
    connection = await connect(_URL)
    try:
        channel = await connection.channel()
        for queue in _QUEUES:
            await channel.queue_delete(queue)
        await channel.exchange_delete("orders")
    finally:
        await connection.close()


async def test_service_guide_prints_its_shown_output():
    """
    Every example in the service tutorial should print the output the page shows.

    Given: The tutorial's examples, and a live broker with none of its queues
    When: The examples run in order, in one namespace, as a reader would run them
    Then: Each prints exactly the output shown under it
    """
    # Arrange
    try:
        await asyncio.wait_for(_delete_topology(), timeout=5)
    except OSError:
        pytest.skip(f"no RabbitMQ broker reachable at {_URL}")

    # Act
    try:
        printed = await asyncio.to_thread(_run, _examples())
    finally:
        await _delete_topology()

    # Assert
    shown = [(output, expected) for output, expected in printed if expected is not None]
    assert len(shown) == 2
    for output, expected in shown:
        assert output == expected
