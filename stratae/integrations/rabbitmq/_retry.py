"""How a failure policy's retry delays name their retry queues."""

from __future__ import annotations

from stratae.backoff import Backoff


def retry_delays(backoff: Backoff) -> list[float]:
    """Return each retry delay of a backoff that runs out, in order."""
    retries = backoff.retries or 0
    return [delay for n in range(1, retries + 1) if (delay := backoff.delay(n)) is not None]


def milliseconds(seconds: float) -> int:
    """Return a delay in whole milliseconds, as a queue's message TTL takes it."""
    return max(1, round(seconds * 1000))


def retry_queue_name(queue: str, delay: float) -> str:
    """Return the name of `queue`'s retry queue for `delay` seconds."""
    return f"{queue}.retry.{milliseconds(delay)}"
