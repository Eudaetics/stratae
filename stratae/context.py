"""
Callable, injectable values backed by contextvars.

`Context` wraps a `contextvars.ContextVar`: set a value once with `.set()`
or `with ctx.use(value):`, and any code running within that section
can read it back by calling the `Context` instance directly. Because
`Context` instances are callable, they work as `Depends()` providers,
letting runtime values (a request's user ID, a feature flag, a connection)
flow into injected functions without changing their signatures.

Examples:
    Setting and reading a value across nested scopes:

    .. code-block:: python

        from stratae.context import Context

        user_id = Context[int]("user_id")

        with user_id.use(123):
            assert user_id.get() == 123
            with user_id.use(42):
                assert user_id() == 42
            assert user_id() == 123

    An A/B test, where the `Context` holds the function to run:

    .. code-block:: python

        from typing import Callable

        from stratae.context import Context
        from stratae.depends import Depends, Injected, inject

        def classic_checkout() -> str: ...
        def one_click_checkout() -> str: ...

        checkout_renderer = Context[Callable[[], str]](
            "checkout_renderer", default=classic_checkout
        )

        @inject
        def checkout_page(
            render: Injected[Callable[[], str], Depends(checkout_renderer)],
        ) -> str:
            return render()

        checkout_page()  # control: classic checkout

        with checkout_renderer.use(one_click_checkout):
            checkout_page()  # experiment group: one-click checkout

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

    __slots__ = ("_provider", "_value", "_token")

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

    Type Parameters:
        T: Type of the stored value.

    Example:
        .. code-block:: python

            user_id = Context[int]("user_id")

            @inject
            def get_current_user(uid: Injected[int, Depends(user_id)]) -> User:
                return fetch_user(uid)

            with user_id.use(123):
                get_current_user()

    """

    __slots__ = ("_name", "_var", "_default")

    def __init__(self, name: str, default: T | _NoDefault = _NO_DEFAULT):
        """
        Initialize the context with a name and an optional fallback default.

        The constructor-level default is used whenever the variable is unset
        and no call-specific default is given to `__call__`/`get`.

        Args:
            name: Name used for the underlying `ContextVar` and in error
                messages when the context is accessed while unset.
            default: Fallback value used when the variable is unset and no
                call-specific default is given. When omitted, there is no
                fallback and an unset access raises.

        Raises:
            ValueError: If `IGNORE` is passed as the default; it is only
                meaningful as a call-specific default to `get`/`__call__`.

        """
        if default is IGNORE:
            raise ValueError("IGNORE cannot be used as a constructor default.")
        self._name = name
        self._var: ContextVar[T] = ContextVar(name)
        self._default = default

    def get(self, default: T | _NoDefault = _NO_DEFAULT) -> T:
        """
        Get the current value, or a default if unset.

        Equivalent to calling the context directly. Falls back to `default`
        if given, otherwise to the default set at construction time.

        Args:
            default: Fallback value to use if the variable is unset. When
                omitted, falls back to the constructor's default, if any.
                Pass `IGNORE` to bypass the constructor's default and
                require a set value.

        Returns:
            The currently set value if any, otherwise the call-specific
            default if given, otherwise the constructor default.

        Raises:
            RuntimeError: If the variable is unset and no default is
                available.

        """
        if default is _NO_DEFAULT:
            default = self._default
        try:
            return self._var.get() if isinstance(default, _NoDefault) else self._var.get(default)
        except LookupError as lookup_err:
            raise RuntimeError(
                f"Context '{self._name}' is not set. Use `with {self._name}.use(value):` to set it."
            ) from lookup_err

    __call__ = get
    """An alias of get. Calling an instance is equivalent to instance.get()"""

    def set(self, value: T) -> Token[T]:
        """
        Set the context value.

        Args:
            value: Value to assign to the context variable.

        Returns:
            A token that can be passed to `reset` to restore the previous
            state.

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

        Returns:
            A context manager that sets the value on entry (yielding the
            value) and restores the previous state on exit. Each call
            returns a fresh scope, so nested or concurrent uses are safe.

        """
        return _ContextScope(self, value)
