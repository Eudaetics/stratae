"""Wrapper around contextvars for named context providers."""

from __future__ import annotations

from contextvars import ContextVar, Token


class _ContextScope[T]:
    """Stateful context manager for a single context value."""

    def __init__(self, provider: Context[T], value: T):
        """Initialize the context scope with provider and value."""
        self._provider = provider
        self._value = value
        self._token: Token[T]

    def __enter__(self):
        """Enter the context, setting the value."""
        self._token = self._provider.set(self._value)
        return self._value

    def __exit__(self, *_):
        """Exit the context, resetting the value."""
        self._provider.reset(self._token)


class Context[T]:
    """Named context provider using contextvars."""

    def __init__(self, name: str):
        """Initialize the ContextProvider with a name."""
        self._name = name
        self._var: ContextVar[T] = ContextVar(name)

    def __call__(self) -> T:
        """Get the current context value."""
        try:
            return self._var.get()
        except LookupError as lookup_err:
            raise RuntimeError(
                f"Context '{self._name}' is not set. Use `with {self._name}.use(value):` to set it."
            ) from lookup_err

    def get(self, default: T | None = None) -> T | None:
        """Get current value, or default if not set."""
        return self._var.get(default)

    def set(self, value: T) -> Token[T]:
        """Set the context value."""
        return self._var.set(value)

    def reset(self, token: Token[T]) -> None:
        """Reset the context value to a previous state."""
        self._var.reset(token)

    def use(self, value: T) -> _ContextScope[T]:
        """Create a context scope for the given value."""
        return _ContextScope(self, value)
