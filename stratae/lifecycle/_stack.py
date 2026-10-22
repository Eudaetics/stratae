from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import Any, Hashable

from stratae.lifecycle.exceptions import ScopeInactiveError


def _record(
    pending: list[BaseException] | None,
    current: BaseException | None,
    outcome: BaseException | None,
) -> list[BaseException]:
    """
    Record an exit outcome that differs from the exception it was handed.

    A suppressed exception clears the list, as with `contextlib.ExitStack`.
    """
    if outcome is None:
        return []
    if pending is None:
        return _seed(current, outcome)
    if not any(outcome is exc for exc in pending):
        pending.append(outcome)
    return pending


def _seed(current: BaseException | None, outcome: BaseException) -> list[BaseException]:
    """Start the pending list at the first outcome that differs from what it was handed."""
    return [outcome] if current is None else [current, outcome]


def _settle(pending: list[BaseException], received: BaseException | None) -> bool:
    """
    Finish a close in which some exit raised or suppressed.

    Returns whether the caller's exception was suppressed, or raises what's still
    propagating.
    """
    if not pending:
        return received is not None
    if len(pending) == 1:
        raise pending[0]
    raise BaseExceptionGroup("Multiple exceptions raised during scope cleanup", pending[::-1])


def _closed_error() -> ScopeInactiveError:
    return ScopeInactiveError("Cannot enter a resource: its scope activation has ended.")


class ExitStack:
    """Sync exit stack for the resources entered in a scope activation."""

    __slots__ = ("contexts", "closed")

    def __init__(self) -> None:
        """Create an empty stack."""
        self.contexts: list[AbstractContextManager[Any]] = []
        self.closed = False

    def enter_context[R](self, ctx: AbstractContextManager[R]) -> R:
        """
        Enter a context manager, registering it for close().

        :raises ScopeInactiveError: If the stack closed during entry; `ctx` is exited at once.
        """
        result = ctx.__enter__()
        if self.closed:
            ctx.__exit__(None, None, None)
            raise _closed_error()
        self.contexts.append(ctx)
        return result

    def close(self, exc: BaseException | None = None) -> bool:
        """
        Close every registered context manager, starting from the caller's exception.

        :returns: Whether the caller's exception was suppressed.
        :raises BaseException: A new exception from closing, or a `BaseExceptionGroup` if
            several are propagating.
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
    """Async exit stack for the resources entered in a scope activation."""

    __slots__ = ("contexts", "closed", "claims")

    def __init__(self) -> None:
        """Create an empty stack with no claimed cache misses."""
        self.contexts: list[tuple[bool, Any]] = []
        self.closed = False
        self.claims: dict[Hashable, Any] = {}
        """This activation's in-progress cache misses, claimed by a context scope's async
        wrappers - see `stratae.lifecycle._inflight`."""

    def enter_context[T](self, ctx: AbstractContextManager[T]) -> T:
        """
        Enter a sync context manager, registering it for aclose().

        :raises ScopeInactiveError: If the stack closed during entry; `ctx` is exited at once.
        """
        result = ctx.__enter__()
        if self.closed:
            ctx.__exit__(None, None, None)
            raise _closed_error()
        self.contexts.append((False, ctx))
        return result

    async def enter_async_context[T](self, ctx: AbstractAsyncContextManager[T]) -> T:
        """
        Enter an async context manager, registering it for aclose().

        :raises ScopeInactiveError: If the stack closed during entry; `ctx` is exited at once.
        """
        result = await ctx.__aenter__()
        if self.closed:
            await ctx.__aexit__(None, None, None)
            raise _closed_error()
        self.contexts.append((True, ctx))
        return result

    async def aclose(self, exc: BaseException | None = None) -> bool:
        """
        Close every registered context manager, starting from the caller's exception.

        :returns: Whether the caller's exception was suppressed.
        :raises BaseException: A new exception from closing, or a `BaseExceptionGroup` if
            several are propagating.
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
