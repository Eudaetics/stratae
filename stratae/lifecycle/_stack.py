"""
Entry and exit of the resources entered in a scope activation.

An activation's slot 0 holds a list of its entered resources: context managers, and
resource generators driven directly, each paired with whether its exit is awaited.
Deactivation empties the activation's storage before closing them, so an entry finishing
after that sees the emptied storage, exits at once, and raises.
"""

from contextlib import AbstractAsyncContextManager, AbstractContextManager
from types import AsyncGeneratorType, GeneratorType, TracebackType
from typing import Any, AsyncGenerator, Generator

from stratae.lifecycle._slots import SlotStorage
from stratae.lifecycle.exceptions import ScopeInactiveError

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
    raise BaseExceptionGroup("Multiple exceptions raised during scope cleanup", pending[::-1])


def _closed_error() -> ScopeInactiveError:
    return ScopeInactiveError("Cannot enter a resource: its scope activation has ended.")


def _exit_generator(gen: Generator[Any, None, None], exc: BaseException | None) -> bool:
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
            raise RuntimeError("generator didn't stop")
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
        raise RuntimeError("generator didn't stop after throw()")
    finally:
        gen.close()


async def _exit_async_generator(gen: AsyncGenerator[Any, None], exc: BaseException | None) -> bool:
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
            raise RuntimeError("generator didn't stop")
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
        raise RuntimeError("generator didn't stop after athrow()")
    finally:
        await gen.aclose()


def _exc_info(
    exc: BaseException | None,
) -> tuple[type[BaseException] | None, BaseException | None, TracebackType | None]:
    """Return the `__exit__` arguments for `exc`."""
    if exc is None:
        return None, None, None
    return type(exc), exc, exc.__traceback__


def _exit_sync(ctx: Any, exc: BaseException | None) -> bool:
    """Exit a sync resource: a driven generator, or a context manager."""
    if ctx.__class__ is GeneratorType:
        return _exit_generator(ctx, exc)
    return ctx.__exit__(*_exc_info(exc))


def enter_context[T](slots: SlotStorage, ctx: AbstractContextManager[T]) -> T:
    """
    Enter a sync context manager, registering it in the activation `slots`.

    :raises ScopeInactiveError: If the activation ended during entry; `ctx` is exited at once.
    """
    result = ctx.__enter__()
    if not slots:
        ctx.__exit__(None, None, None)
        raise _closed_error()
    slots[0].append((False, ctx))
    return result


def enter_generator[T](slots: SlotStorage, gen: Generator[T, None, None]) -> T:
    """
    Advance a resource generator to its yielded value, registering it in the activation.

    :raises RuntimeError: If the generator doesn't yield.
    :raises ScopeInactiveError: If the activation ended during entry; `gen` is finished at
        once.
    """
    try:
        result = next(gen)
    except StopIteration:
        raise RuntimeError("generator didn't yield") from None
    if not slots:
        _exit_generator(gen, None)
        raise _closed_error()
    slots[0].append((False, gen))
    return result


async def enter_async_context[T](slots: SlotStorage, ctx: AbstractAsyncContextManager[T]) -> T:
    """
    Enter an async context manager, registering it in the activation `slots`.

    :raises ScopeInactiveError: If the activation ended during entry; `ctx` is exited at once.
    """
    result = await ctx.__aenter__()
    if not slots:
        await ctx.__aexit__(None, None, None)
        raise _closed_error()
    slots[0].append((True, ctx))
    return result


async def enter_async_generator[T](slots: SlotStorage, gen: AsyncGenerator[T, None]) -> T:
    """
    Advance a resource async generator to its yielded value, registering it in the activation.

    :raises RuntimeError: If the generator doesn't yield.
    :raises ScopeInactiveError: If the activation ended during entry; `gen` is finished at
        once.
    """
    try:
        result = await anext(gen)
    except StopAsyncIteration:
        raise RuntimeError("generator didn't yield") from None
    if not slots:
        await _exit_async_generator(gen, None)
        raise _closed_error()
    slots[0].append((True, gen))
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
        ctx = contexts.pop()[1]
        try:
            suppressed = _exit_sync(ctx, exc)
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
        is_async, ctx = contexts.pop()
        try:
            if not is_async:
                suppressed = _exit_sync(ctx, exc)
            elif ctx.__class__ is AsyncGeneratorType:
                suppressed = await _exit_async_generator(ctx, exc)
            else:
                suppressed = await ctx.__aexit__(*_exc_info(exc))
        except BaseException as new_exc:
            outcome = new_exc
        else:
            outcome = None if suppressed else exc
        if outcome is not exc:
            pending = _record(pending, exc, outcome)
            exc = outcome
    return False if pending is None else _settle(pending, received)
