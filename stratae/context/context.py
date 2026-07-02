"""
Callable, injectable values backed by contextvars.

`Context` wraps a `ContextVar` so a value set at one point in the call
stack (or in a parent async task) can be read elsewhere without threading
it through every function signature in between. Because instances are
callable, a `Context[T]` doubles as a `Depends()` provider.
"""

from contextvars import ContextVar, Token


class _NoDefault:
    __slots__ = ()


_NO_DEFAULT = _NoDefault()
IGNORE = _NoDefault()
"""Sentinel passed as a 'default' to skip the constructor default and require a set value."""


class _ContextScope[T]:
    """
    Stateful context manager for a single context value.

    Returned by `Context.use`. Each call to `use` creates a new instance, so
    nested or concurrent (e.g. across async tasks) scopes on the same
    `Context` each track their own token rather than overwriting shared
    state on the `Context` itself.
    """

    def __init__(self, provider: "Context[T]", value: T):
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
    """
    A named, settable value backed by a ContextVar, usable as a Depends() provider.

    Accepts an optional default at construction, used whenever the value is
    unset and no default is given to that particular `get`/call. A default
    passed to `get`/`__call__` overrides the constructor's default for that
    call only.

    Example:
        user_id = Context[int]("user_id")

        @inject
        def get_current_user(uid: int = Depends(user_id)) -> User:
            return fetch_user(uid)

        with user_id.use(123):
            get_current_user()

    """

    def __init__(self, name: str, default: T | _NoDefault = _NO_DEFAULT):
        """
        Initialize the context with a name and an optional fallback default.

        The constructor-level default is used whenever the variable is unset
        and no call-specific default is given to `__call__`/`get`.

        Args:
            name: Name used for the underlying `ContextVar` and in error
                messages when the context is accessed while unset.
            default: Fallback value used when the variable is unset and no
                call-specific default is given. Defaults to `_NO_DEFAULT`,
                meaning there is no fallback and an unset access raises.

        """
        if default is IGNORE:
            raise ValueError("IGNORE cannot be used as a constructor default.")
        self._name = name
        self._var: ContextVar[T] = ContextVar(name)
        self._default = default

    def __call__(self, default: T | _NoDefault = _NO_DEFAULT) -> T:
        """
        Get the current context value.

        Resolution order when the variable is unset: the `default` passed
        here, then the default given at construction time, then a
        `RuntimeError` if neither is set.

        Args:
            default: Fallback value to use if the variable is unset. Defaults
                to `_NO_DEFAULT`, meaning fall back to the constructor's
                default (if any) instead.

        """
        try:
            if not isinstance(default, _NoDefault):
                return self._var.get(default)
            if default is not IGNORE and not isinstance(self._default, _NoDefault):
                return self._var.get(self._default)
            return self._var.get()
        except LookupError as lookup_err:
            raise RuntimeError(
                f"Context '{self._name}' is not set. Use `with {self._name}.use(value):` to set it."
            ) from lookup_err

    def get(self, default: T | _NoDefault = _NO_DEFAULT) -> T:
        """
        Get the current value, or a default if unset.

        Equivalent to calling the context directly. Falls back to `default`
        if given, otherwise to the default set at construction time.

        Args:
            default: Fallback value to use if the variable is unset. Defaults
                to `_NO_DEFAULT`, meaning fall back to the constructor's
                default (if any) instead.

        """
        return self(default)

    def set(self, value: T) -> Token[T]:
        """
        Set the context value.

        Args:
            value: Value to assign to the context variable.

        """
        return self._var.set(value)

    def reset(self, token: Token[T]) -> None:
        """
        Reset the context value to a previous state.

        Args:
            token: Token returned by a prior call to `set`, identifying the
                state to restore.

        """
        self._var.reset(token)

    def use(self, value: T) -> _ContextScope[T]:
        """
        Create a context scope for the given value.

        Args:
            value: Value to set for the duration of the returned context
                manager.

        """
        return _ContextScope(self, value)
