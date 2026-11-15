"""How a failure policy's retry delays name their retry queues."""


def milliseconds(seconds: float) -> int:
    """Return a delay in whole milliseconds, as a queue's message TTL takes it."""
    return max(1, round(seconds * 1000))


def retry_queue_name(queue: str, delay: float) -> str:
    """Return the name of `queue`'s retry queue for `delay` seconds."""
    return f"{queue}.retry.{milliseconds(delay)}"
