"""Guard the sync event paths against awaitables that would otherwise be dropped."""

from inspect import isawaitable


def reject_awaitable(result: object, message: str, source: object) -> None:
    """
    Raise TypeError if a sync path got an awaitable, closing it so it isn't left unawaited.

    :param result: What the hook or middleware returned.
    :param message: The error message, formatted with `source` only when raising.
    :param source: The hook or middleware that returned it.
    """
    if not isawaitable(result):
        return
    close = getattr(result, "close", None)
    if close is not None:
        close()
    raise TypeError(message.format(source))
