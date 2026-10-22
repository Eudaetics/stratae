from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Any, Hashable

from stratae.lifecycle.exceptions import ScopeInactiveError


def _record(
    pending: list[BaseException] | None,
    current: BaseException | None,
    outcome: BaseException | None,
) -> list[BaseException]:
    """
    Record a context manager's exit outcome that differs from the exception it was handed.

    Only called off the common path, once an exit raises something new or suppresses what
    it was handed, so a close where nothing changes never builds the list. `pending` starts
    as the exception that was propagating, if any. A new exception is appended; a
    suppressed one clears everything, since each earlier exception is only reachable
    through the one that was suppressed, same as with `contextlib.ExitStack`.
    """
    if outcome is None:
        return []
    if pending is None:
        return _seed(current, outcome)
    if not any(outcome is exc for exc in pending):
        pending.append(outcome)
    return pending


def _seed(current: BaseException | None, outcome: BaseException) -> list[BaseException]:
    """
    Start the pending list at the first exit outcome that differs from what it was handed.

    `outcome` is never `current` itself - the caller only records an outcome that differs -
    so it needs no duplicate check.
    """
    return [outcome] if current is None else [current, outcome]


def _settle(pending: list[BaseException], received: BaseException | None) -> bool:
    """
    Finish a close in which some exit raised something new or suppressed what it was handed.

    Returns `True` if everything was suppressed and there was a caller's exception to
    suppress, else `False`. Raises the single exception still propagating, or a group of
    every one, most recent first.
    """
    if not pending:
        return received is not None
    if len(pending) == 1:
        raise pending[0]
    raise BaseExceptionGroup("Multiple exceptions raised during scope cleanup", pending[::-1])


def _closed_error() -> ScopeInactiveError:
    return ScopeInactiveError("Cannot enter a resource: its scope activation has ended.")


class ExitStack:
    """
    Sync exit stack for context managers entered by resources within a scope activation.

    Lives in a scope's reserved slot 0, created lazily on first use; `close`
    unwinds every entered context manager in reverse order when the owning
    scope deactivates.
    """

    __slots__ = ("contexts", "closed")

    def __init__(self) -> None:
        """Initialize with no registered context managers."""
        self.contexts: list[AbstractContextManager[Any]] = []
        self.closed = False

    def enter_context[R](self, ctx: AbstractContextManager[R]) -> R:
        """
        Enter a context manager and register it for cleanup on close().

        :raises ScopeInactiveError: If the stack closed while `ctx` was being entered -
            `ctx` is exited at once rather than left open with no cleanup coming.
        """
        result = ctx.__enter__()
        if self.closed:
            ctx.__exit__(None, None, None)
            raise _closed_error()
        self.contexts.append(ctx)
        return result

    def close(self, exc: BaseException | None = None) -> bool:
        """
        Close every registered context manager, seeded with the caller's exception, if any.

        :returns: Whether the caller's exception was suppressed - `False` when it should
            propagate unchanged, or when there was none.
        :raises BaseException: The one new exception a context manager raised while closing,
            or a `BaseExceptionGroup` of every exception still propagating if there are more.
        """
        received = exc
        pending: list[BaseException] | None = None
        contexts = self.contexts
        while contexts:
            ctx = contexts.pop()
            try:
                suppressed = ctx.__exit__(
                    type(exc) if exc else None, exc, exc.__traceback__ if exc else None
                )
            except BaseException as new_exc:
                outcome = new_exc
            else:
                outcome = None if suppressed else exc
            if outcome is not exc:
                pending = _record(pending, exc, outcome)
                exc = outcome
        self.closed = True
        return False if pending is None else _settle(pending, received)


class AsyncExitStack:
    """
    Async exit stack for context managers entered by resources within a scope activation.

    Lives in a scope's reserved slot 0, created lazily on first use; `aclose`
    unwinds every entered context manager in reverse order when the owning
    scope deactivates, awaiting only the async ones.
    """

    __slots__ = ("contexts", "closed", "claims")

    def __init__(self) -> None:
        """Initialize with no registered context managers, and no claimed cache misses."""
        self.contexts: list[tuple[bool, Any]] = []
        self.closed = False
        self.claims: dict[Hashable, Any] = {}
        """This activation's in-progress cache misses, claimed by a context scope's async
        wrappers - see `stratae.lifecycle._inflight`."""

    def enter_context[T](self, ctx: AbstractContextManager[T]) -> T:
        """
        Enter a sync context manager and register it for cleanup on aclose().

        :raises ScopeInactiveError: If the stack closed while `ctx` was being entered -
            `ctx` is exited at once rather than left open with no cleanup coming.
        """
        result = ctx.__enter__()
        if self.closed:
            ctx.__exit__(None, None, None)
            raise _closed_error()
        self.contexts.append((False, ctx))
        return result

    async def enter_async_context[T](self, ctx: AbstractAsyncContextManager[T]) -> T:
        """
        Enter an async context manager and register it for cleanup on aclose().

        :raises ScopeInactiveError: If the stack closed while `ctx` was being entered -
            `ctx` is exited at once rather than left open with no cleanup coming.
        """
        result = await ctx.__aenter__()
        if self.closed:
            await ctx.__aexit__(None, None, None)
            raise _closed_error()
        self.contexts.append((True, ctx))
        return result

    async def aclose(self, exc: BaseException | None = None) -> bool:
        """
        Close every registered context manager, seeded with the caller's exception, if any.

        :returns: Whether the caller's exception was suppressed - `False` when it should
            propagate unchanged, or when there was none.
        :raises BaseException: The one new exception a context manager raised while closing,
            or a `BaseExceptionGroup` of every exception still propagating if there are more.
        """
        received = exc
        pending: list[BaseException] | None = None
        contexts = self.contexts
        while contexts:
            is_async, ctx = contexts.pop()
            exc_type = type(exc) if exc else None
            tb = exc.__traceback__ if exc else None
            try:
                if is_async:
                    suppressed = await ctx.__aexit__(exc_type, exc, tb)
                else:
                    suppressed = ctx.__exit__(exc_type, exc, tb)
            except BaseException as new_exc:
                outcome = new_exc
            else:
                outcome = None if suppressed else exc
            if outcome is not exc:
                pending = _record(pending, exc, outcome)
                exc = outcome
        self.closed = True
        return False if pending is None else _settle(pending, received)
