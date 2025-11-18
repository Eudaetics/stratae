"""Scope container for cache and exit stack."""

from contextlib import AsyncExitStack, ExitStack

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


class Scope:
    """Container class for a lifecycle scope's cache and exit stack."""

    def __init__(self, cache: Cache, exit_stack: ExitStack):
        """Initialize the Scope with a cache and exit stack."""
        self._cache = cache
        self._exit_stack = exit_stack

    def clear(self) -> None:
        """Clear the scope's cache."""
        self._cache.clear()
        try:
            self._exit_stack.close()
        except Exception as exc:
            _handle_exception_group(exc)

    @property
    def cache(self) -> Cache:
        """Get the scope's cache."""
        return self._cache

    @property
    def exit_stack(self) -> ExitStack:
        """Get the scope's exit stack."""
        return self._exit_stack


class AsyncScope:
    """Asynchronous container class for a lifecycle scope's cache and exit stack."""

    def __init__(self, cache: Cache, exit_stack: AsyncExitStack):
        """Initialize the AsyncScope with a cache and exit stack."""
        self._cache = cache
        self._exit_stack = exit_stack

    async def clear(self) -> None:
        """Asynchronously clear the scope's cache."""
        await self._cache.aclear()
        try:
            await self._exit_stack.aclose()
        except Exception as exc:
            _handle_exception_group(exc)

    @property
    def cache(self) -> Cache:
        """Get the scope's cache."""
        return self._cache

    @property
    def exit_stack(self) -> AsyncExitStack:
        """Get the scope's exit stack."""
        return self._exit_stack
