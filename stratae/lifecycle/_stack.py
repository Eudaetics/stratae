"""
Entry and exit of the resources entered in a scope activation.

An activation's slot 0 holds a list of its entered resources: the generators that cached
generator functions returned, driven directly. Each is held with whether it's async and the
exit that finishes it: {py:func}`exit_generator` or {py:func}`exit_async_generator`, unless
whoever entered it gave another, as a scope's middleware does.
Deactivation empties the activation's storage before closing them, so an entry finishing
after that sees the emptied storage, exits at once, and raises.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from typing import Any

from stratae.lifecycle._slots import SlotStorage
from stratae.lifecycle.exceptions import (
    EndedDuringEntryError,
    ResourceDidNotStopError,
    ResourceDidNotYieldError,
)

Entered = list[tuple[bool, Any, Callable[[Any, BaseException | None], Any]]]


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


def enter_generator[T](
    slots: SlotStorage,
    gen: Generator[T, None, None],
    leave: Callable[[Generator[T, None, None], BaseException | None], bool] = exit_generator,
) -> T:
    """
    Advance a resource generator to its yielded value, registering it in the activation.

    :param leave: What finishes it when the activation closes.

    :raises ResourceDidNotYieldError: If the generator doesn't yield.
    :raises ScopeInactiveError: If the activation ended during entry; `gen` is finished at
        once.
    """
    try:
        result = next(gen)
    except StopIteration:
        raise ResourceDidNotYieldError() from None
    if not slots:
        leave(gen, None)
        raise EndedDuringEntryError()
    slots[0].append((False, gen, leave))  # slot 0: entered resources
    return result


async def enter_async_generator[T](
    slots: SlotStorage,
    gen: AsyncGenerator[T, None],
    leave: Callable[
        [AsyncGenerator[T, None], BaseException | None], Awaitable[bool]
    ] = exit_async_generator,
) -> T:
    """
    Advance a resource async generator to its yielded value, registering it in the activation.

    :param leave: What finishes it when the activation closes.

    :raises ResourceDidNotYieldError: If the generator doesn't yield.
    :raises ScopeInactiveError: If the activation ended during entry; `gen` is finished at
        once.
    """
    try:
        result = await anext(gen)
    except StopAsyncIteration:
        raise ResourceDidNotYieldError() from None
    if not slots:
        await leave(gen, None)
        raise EndedDuringEntryError()
    slots[0].append((True, gen, leave))  # slot 0: entered resources
    return result


def close(entered: Entered, exc: BaseException | None = None) -> bool:
    """
    Exit every resource in `entered`, last entered first, starting from the caller's exception.

    Each exit is handed what's propagating: the caller's exception, `None` once an exit
    suppressed it, or the error an exit raised in its place. The resources left after an exit
    raises are closed inside that error's handler, as the next `with` out would close them. So
    Python links each error to the one it replaced, as `__context__`, and the last one raised
    replaces the caller's exception.

    :returns: Whether the caller's exception was suppressed.
    :raises BaseException: The last error an exit raised.
    """
    received = exc
    while entered:
        _, gen, leave = entered.pop()
        try:
            if leave(gen, exc):
                exc = None
        except BaseException as new_exc:
            if close(entered, new_exc):
                return received is not None
            raise
    return received is not None and exc is None


async def aclose(entered: Entered, exc: BaseException | None = None) -> bool:
    """
    Exit every resource in `entered`, as {py:func}`close` does, awaiting async exits.

    :returns: Whether the caller's exception was suppressed.
    :raises BaseException: The last error an exit raised.
    """
    received = exc
    while entered:
        is_async, gen, leave = entered.pop()
        try:
            if await leave(gen, exc) if is_async else leave(gen, exc):
                exc = None
        except BaseException as new_exc:
            if await aclose(entered, new_exc):
                return received is not None
            raise
    return received is not None and exc is None
