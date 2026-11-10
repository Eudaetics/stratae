"""
Entry and exit of the resources entered in a scope activation.

An activation's slot 0 holds a list of its entered resources: the generators that cached
generator functions returned, driven directly, each paired with whether it's async.
Deactivation empties the activation's storage before closing them, so an entry finishing
after that sees the emptied storage, exits at once, and raises.
"""

from collections.abc import AsyncGenerator, Generator
from typing import Any

from stratae.lifecycle._slots import SlotStorage
from stratae.lifecycle.exceptions import (
    BaseCleanupErrors,
    CleanupErrors,
    EndedDuringEntryError,
    ResourceDidNotStopError,
    ResourceDidNotYieldError,
)

Contexts = list[tuple[bool, Any]]


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
    failures = pending[::-1]
    errors = [failure for failure in failures if isinstance(failure, Exception)]
    if len(errors) < len(failures):
        raise BaseCleanupErrors(failures)
    raise CleanupErrors(errors)


def exit_generator(gen: Generator[Any, None, None], exc: BaseException | None) -> bool:
    """
    Finish a resource generator as `contextlib.contextmanager`'s `__exit__` would.

    Driving the generator directly skips building a context manager per entry.

    :returns: Whether `exc` was suppressed.
    """
    if exc is None:
        try:
            next(gen)
        except StopIteration:
            return False
        try:
            raise ResourceDidNotStopError()
        finally:
            gen.close()
    tb = exc.__traceback__
    try:
        gen.throw(exc)
    except StopIteration as stop:
        return stop is not exc
    except RuntimeError as err:
        if err is exc:
            err.__traceback__ = tb
            return False
        if isinstance(exc, StopIteration) and err.__cause__ is exc:
            exc.__traceback__ = tb
            return False
        raise
    except BaseException as err:
        if err is not exc:
            raise
        err.__traceback__ = tb
        return False
    try:
        raise ResourceDidNotStopError("throw()")
    finally:
        gen.close()


async def exit_async_generator(gen: AsyncGenerator[Any, None], exc: BaseException | None) -> bool:
    """
    Finish a resource async generator as `contextlib.asynccontextmanager`'s `__aexit__` would.

    :returns: Whether `exc` was suppressed.
    """
    if exc is None:
        try:
            await anext(gen)
        except StopAsyncIteration:
            return False
        try:
            raise ResourceDidNotStopError()
        finally:
            await gen.aclose()
    tb = exc.__traceback__
    try:
        await gen.athrow(exc)
    except StopAsyncIteration as stop:
        return stop is not exc
    except RuntimeError as err:
        if err is exc:
            err.__traceback__ = tb
            return False
        if isinstance(exc, (StopIteration, StopAsyncIteration)) and err.__cause__ is exc:
            exc.__traceback__ = tb
            return False
        raise
    except BaseException as err:
        if err is not exc:
            raise
        err.__traceback__ = tb
        return False
    try:
        raise ResourceDidNotStopError("athrow()")
    finally:
        await gen.aclose()


def enter_generator[T](slots: SlotStorage, gen: Generator[T, None, None]) -> T:
    """
    Advance a resource generator to its yielded value, registering it in the activation.

    :raises ResourceDidNotYieldError: If the generator doesn't yield.
    :raises ScopeInactiveError: If the activation ended during entry; `gen` is finished at
        once.
    """
    try:
        result = next(gen)
    except StopIteration:
        raise ResourceDidNotYieldError() from None
    if not slots:
        exit_generator(gen, None)
        raise EndedDuringEntryError()
    slots[0].append((False, gen))  # slot 0: entered resources
    return result


async def enter_async_generator[T](slots: SlotStorage, gen: AsyncGenerator[T, None]) -> T:
    """
    Advance a resource async generator to its yielded value, registering it in the activation.

    :raises ResourceDidNotYieldError: If the generator doesn't yield.
    :raises ScopeInactiveError: If the activation ended during entry; `gen` is finished at
        once.
    """
    try:
        result = await anext(gen)
    except StopAsyncIteration:
        raise ResourceDidNotYieldError() from None
    if not slots:
        await exit_async_generator(gen, None)
        raise EndedDuringEntryError()
    slots[0].append((True, gen))  # slot 0: entered resources
    return result


def close(contexts: Contexts, exc: BaseException | None = None) -> bool:
    """
    Exit every resource in `contexts`, last entered first, starting from the caller's exception.

    :returns: Whether the caller's exception was suppressed.
    :raises BaseException: A new exception from closing, or a `BaseExceptionGroup` if
        several are propagating.
    """
    received = exc
    pending: list[BaseException] | None = None
    while contexts:
        gen = contexts.pop()[1]
        try:
            suppressed = exit_generator(gen, exc)
        except BaseException as new_exc:
            outcome = new_exc
        else:
            outcome = None if suppressed else exc
        if outcome is not exc:
            pending = _record(pending, exc, outcome)
            exc = outcome
    return False if pending is None else _settle(pending, received)


async def aclose(contexts: Contexts, exc: BaseException | None = None) -> bool:
    """
    Exit every resource in `contexts`, last entered first, starting from the caller's exception.

    :returns: Whether the caller's exception was suppressed.
    :raises BaseException: A new exception from closing, or a `BaseExceptionGroup` if
        several are propagating.
    """
    received = exc
    pending: list[BaseException] | None = None
    while contexts:
        is_async, gen = contexts.pop()
        try:
            if is_async:
                suppressed = await exit_async_generator(gen, exc)
            else:
                suppressed = exit_generator(gen, exc)
        except BaseException as new_exc:
            outcome = new_exc
        else:
            outcome = None if suppressed else exc
        if outcome is not exc:
            pending = _record(pending, exc, outcome)
            exc = outcome
    return False if pending is None else _settle(pending, received)
