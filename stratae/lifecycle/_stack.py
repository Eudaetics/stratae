"""
Entry and exit of the resources entered in a scope activation.

An activation's slot 0 holds a list of what it entered, driven directly. A resource, the
generator a cached generator function returned, is held as it is. Its kind picks its exit:
{py:func}`exit_generator` or {py:func}`exit_async_generator`. A scope's middleware is held
with whether it's async and the exit that finishes it, since its exit refuses a swallowed
error.
Deactivation empties the activation's storage before closing them, so an entry finishing
after that sees the emptied storage, exits at once, and raises.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from types import AsyncGeneratorType, GeneratorType
from typing import Literal

from stratae.lifecycle._slots import SlotStorage
from stratae.lifecycle.exceptions import (
    EndedDuringEntryError,
    ResourceDidNotStopError,
    ResourceDidNotYieldError,
)

type SyncExit = Callable[[Generator[object, None, object], BaseException | None], bool]
"""Finishes an entered generator, given what's propagating; returns whether it suppressed it."""

type AsyncExit = Callable[[AsyncGenerator[object, None], BaseException | None], Awaitable[bool]]
"""Finishes an entered async generator, as {py:data}`SyncExit` does, to await."""

type Native[Value] = GeneratorType[Value, None, object]
"""A generator a generator function returned, yielding `Value`."""

type AsyncNative[Value] = AsyncGeneratorType[Value, None]
"""An async generator an async generator function returned, yielding `Value`."""

type Resource = Native[object]
"""An entered resource: the generator a cached generator function returned."""

type AsyncResource = AsyncNative[object]
"""An entered async resource: the async generator a cached function returned."""

type SyncLayerEntry = tuple[Literal[False], Generator[object, None, object], SyncExit]
"""An entered middleware generator and its exit, flagged as sync."""

type AsyncLayerEntry = tuple[Literal[True], AsyncGenerator[object, None], AsyncExit]
"""An entered middleware async generator and its exit, flagged as async."""

type Entered = list[Resource | SyncLayerEntry]
"""What a sync activation entered, in the order it entered them."""

type AsyncEntered = list[Resource | AsyncResource | SyncLayerEntry | AsyncLayerEntry]
"""What an async activation entered, in the order it entered them."""


def exit_generator(gen: Generator[object, None, object], exc: BaseException | None) -> bool:
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


async def exit_async_generator(
    gen: AsyncGenerator[object, None], exc: BaseException | None
) -> bool:
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


def enter_generator[T](slots: SlotStorage, gen: Native[T]) -> T:
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
    slots[0].append(gen)  # slot 0: entered resources
    return result


async def enter_async_generator[T](slots: SlotStorage, gen: AsyncNative[T]) -> T:
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
    slots[0].append(gen)  # slot 0: entered resources
    return result


def enter_layer(slots: SlotStorage, gen: Generator[object, None, object], leave: SyncExit) -> None:
    """
    Advance a middleware generator to its yield, registering it in the activation with its exit.

    :raises ResourceDidNotYieldError: If the generator doesn't yield.
    :raises ScopeInactiveError: If the activation ended during entry; `gen` is finished at
        once.
    """
    try:
        next(gen)
    except StopIteration:
        raise ResourceDidNotYieldError() from None
    if not slots:
        leave(gen, None)
        raise EndedDuringEntryError()
    slots[0].append((False, gen, leave))  # slot 0: entered resources


async def enter_async_layer(
    slots: SlotStorage, gen: AsyncGenerator[object, None], leave: AsyncExit
) -> None:
    """
    Advance a middleware async generator to its yield, as {py:func}`enter_layer` does.

    :raises ResourceDidNotYieldError: If the generator doesn't yield.
    :raises ScopeInactiveError: If the activation ended during entry; `gen` is finished at
        once.
    """
    try:
        await anext(gen)
    except StopAsyncIteration:
        raise ResourceDidNotYieldError() from None
    if not slots:
        await leave(gen, None)
        raise EndedDuringEntryError()
    slots[0].append((True, gen, leave))  # slot 0: entered resources


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
        entry = entered.pop()
        try:
            if (
                exit_generator(entry, exc)
                if type(entry) is GeneratorType
                else entry[2](entry[1], exc)
            ):
                exc = None
        except BaseException as new_exc:
            if close(entered, new_exc):
                return received is not None
            raise
    return received is not None and exc is None


async def aclose(entered: AsyncEntered, exc: BaseException | None = None) -> bool:
    """
    Exit every resource in `entered`, as {py:func}`close` does, awaiting async exits.

    :returns: Whether the caller's exception was suppressed.
    :raises BaseException: The last error an exit raised.
    """
    received = exc
    while entered:
        entry = entered.pop()
        try:
            if type(entry) is GeneratorType:
                suppressed = exit_generator(entry, exc)
            elif type(entry) is AsyncGeneratorType:
                suppressed = await exit_async_generator(entry, exc)
            elif entry[0] is True:
                suppressed = await entry[2](entry[1], exc)
            else:
                suppressed = entry[2](entry[1], exc)
            if suppressed:
                exc = None
        except BaseException as new_exc:
            if await aclose(entered, new_exc):
                return received is not None
            raise
    return received is not None and exc is None
