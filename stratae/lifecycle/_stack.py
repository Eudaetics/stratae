from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Any


def _raise_collected(exc: Exception) -> None:
    """Collect an exception's __context__ chain and raise it as an ExceptionGroup."""
    exceptions: list[Exception] = [exc]
    ctx = exc.__context__
    while ctx:
        if isinstance(ctx, Exception):
            exceptions.append(ctx)
        ctx = getattr(ctx, "__context__", None)
    if len(exceptions) > 1:
        raise ExceptionGroup("Multiple exceptions raised during scope cleanup", exceptions)
    raise exc


def _close_one(ctx: AbstractContextManager[Any], exc: Exception | None) -> Exception | None:
    exc_type = type(exc) if exc else None
    tb = exc.__traceback__ if exc else None
    try:
        suppressed = ctx.__exit__(exc_type, exc, tb)
    except Exception as new_exc:
        return new_exc
    return None if suppressed else exc


async def _aclose_one(
    ctx: AbstractAsyncContextManager[Any], exc: Exception | None
) -> Exception | None:
    exc_type = type(exc) if exc else None
    tb = exc.__traceback__ if exc else None
    try:
        suppressed = await ctx.__aexit__(exc_type, exc, tb)
    except Exception as new_exc:
        return new_exc
    return None if suppressed else exc


class ExitStack:
    """
    Sync exit stack for context managers entered by resources within a scope activation.

    Lives in a scope's reserved slot 0, created lazily on first use; `close`
    unwinds every entered context manager in reverse order when the owning
    scope deactivates.
    """

    __slots__ = ("_contexts",)

    def __init__(self) -> None:
        """Initialize with no registered context managers."""
        self._contexts: list[AbstractContextManager[Any]] = []

    def enter_context[R](self, ctx: AbstractContextManager[R]) -> R:
        """Enter a context manager and register it for cleanup on close()."""
        result = ctx.__enter__()
        self._contexts.append(ctx)
        return result

    def close(self, exc: Exception | None = None) -> bool:
        """Close every registered context manager, seeded with the caller's exception, if any."""
        received = exc is not None
        while self._contexts:
            exc = _close_one(self._contexts.pop(), exc)
        if exc is not None:
            _raise_collected(exc)
        return received


class AsyncExitStack:
    """
    Async exit stack for context managers entered by resources within a scope activation.

    Lives in a scope's reserved slot 0, created lazily on first use; `aclose`
    unwinds every entered context manager in reverse order when the owning
    scope deactivates, awaiting only the async ones.
    """

    __slots__ = ("_contexts",)

    def __init__(self) -> None:
        """Initialize with no registered context managers."""
        self._contexts: list[tuple[bool, Any]] = []

    def enter_context[T](self, ctx: AbstractContextManager[T]) -> T:
        """Enter a sync context manager and register it for cleanup on aclose()."""
        result = ctx.__enter__()
        self._contexts.append((False, ctx))
        return result

    async def enter_async_context[T](self, ctx: AbstractAsyncContextManager[T]) -> T:
        """Enter an async context manager and register it for cleanup on aclose()."""
        result = await ctx.__aenter__()
        self._contexts.append((True, ctx))
        return result

    async def aclose(self, exc: Exception | None = None) -> bool:
        """Close every registered context manager, seeded with the caller's exception, if any."""
        received = exc is not None
        while self._contexts:
            is_async, ctx = self._contexts.pop()
            if is_async:
                exc = await _aclose_one(ctx, exc)
            else:
                exc = _close_one(ctx, exc)
        if exc is not None:
            _raise_collected(exc)
        return received
