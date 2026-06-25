"""Scope container for cache and exit stack."""

from contextlib import AsyncExitStack, ExitStack
from typing import Callable

from stratae.cache import Cache


def _handle_exception_group(exc: Exception) -> None:
    """Flatten an ExceptionGroup into a list of exceptions."""
    exceptions: list[Exception] = [exc]
    ctx = exc.__context__
    while ctx:
        if isinstance(ctx, Exception):
            exceptions.append(ctx)
        ctx = getattr(ctx, "__context__", None)
    if len(exceptions) > 1:
        raise ExceptionGroup("Multiple exceptions during scope cleanup", exceptions)
    else:
        raise


class ActiveScope:
    """Container class for a lifecycle scope's cache and exit stack."""

    __slots__ = ("cache", "_exit_stack")

    def __init__(self, cache_factory: Callable[[], Cache]):
        """Initialize the ActiveScope with a cache and exit stack."""
        self.cache = cache_factory()
        self._exit_stack: ExitStack | None = None

    def clear(self) -> None:
        """Clear the scope's cache."""
        self.cache.clear()
        if self._exit_stack:
            try:
                self._exit_stack.close()
            except Exception as exc:
                _handle_exception_group(exc)

    @property
    def exit_stack(self) -> ExitStack:
        """Get the scope's exit stack."""
        if not self._exit_stack:
            self._exit_stack = ExitStack()
        return self._exit_stack


class AsyncActiveScope:
    """Asynchronous container class for a lifecycle scope's cache and exit stack."""

    __slots__ = ("cache", "_exit_stack")

    def __init__(self, cache_factory: Callable[[], Cache]):
        """Initialize the AsyncActiveScope with a cache and exit stack."""
        self.cache = cache_factory()
        self._exit_stack: AsyncExitStack | None = None

    async def clear(self) -> None:
        """Asynchronously clear the scope's cache."""
        await self.cache.aclear()
        if self._exit_stack:
            try:
                await self._exit_stack.aclose()
            except Exception as exc:
                _handle_exception_group(exc)

    @property
    def exit_stack(self) -> AsyncExitStack:
        """Get the scope's exit stack."""
        if self._exit_stack is None:
            self._exit_stack = AsyncExitStack()
        return self._exit_stack
