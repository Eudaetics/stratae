"""
Callable, injectable values backed by contextvars.

{py:class}`Context` wraps a `contextvars.ContextVar` inside a callable.
Set a value once with {py:func}`Context.set` or {py:func}`Context.use`,
and any code running within that section can read it back by calling the
{py:class}`Context` instance directly. Because {py:class}`Context`
instances are callable, they work as {py:func}`Depends <stratae.depends.inject.Depends>` providers,
letting runtime values (a request's user ID, a feature flag, a connection)
flow into injected functions without changing their signatures.

````{example} A/B testing a recommendation model
```{code-block} python
from typing import Annotated, Callable
from stratae.context import Context
from stratae.depends import Depends, inject

def rank_v1(item: str) -> float:
    return 0.42

def rank_v2(item: str) -> float:
    return 0.91

recommender = Context[Callable[[str], float]]("recommender", default=rank_v1)

@inject
def score(
    item: str, rank: Annotated[Callable[[str], float], Depends(recommender)]
) -> None:
    print(f"score={rank(item)}")

score("wireless headphones")  # control group, using rank_v1

with recommender.use(rank_v2):
    score("wireless headphones")  # experiment group, using rank_v2

# back to the control default
score("wireless headphones")
```
```{output}
score=0.42
score=0.91
score=0.42
```
````

````{note}
On Python 3.14+, `contextvars.Token` itself supports the context manager
protocol, so {py:func}`Context.set` alone gives the same ergonomics as
{py:func}`Context.use`:
<!--- skip: next if(__import__("sys").version_info < (3, 14), "needs Python 3.14+") -->
```{code-block} python
:caption: Using set() directly as a context manager (Python 3.14+)

from stratae.context import Context

user_id = Context[int]("user_id")

with user_id.set(99):
    assert user_id() == 99
```

On earlier versions, {py:func}`Context.set` returns a plain token that
cannot be used as a context manager. Use {py:func}`Context.use` to
set up the context manager automatically on any supported Python version.
````
"""

from contextvars import ContextVar, Token


class _NoDefault:
    __slots__ = ()


_NO_DEFAULT = _NoDefault()
IGNORE = _NoDefault()
"""
Sentinel passed as a 'default' to skip the constructor default and require a set value.

{py:class}`Context` uses similar default behavior as defined in
[`ContextVar`](https://docs.python.org/3/library/contextvars.html#contextvars.ContextVar.get).
If there is no value set in the current context, then {py:func}`Context.get` returns first
the default provided to {py:func}`Context.get`, then the constructor level default, and
finally raises a LookupError.
The `IGNORE` sentinel provides a third option. If the {py:func}`Context.get` is called with
`IGNORE`, then it will raise a LookupError if no value is set and ignore the constructor
level default.
"""


class _ContextScope[T]:
    """
    Stateful context manager for a single context value.

    Returned by {py:func}`Context.use`. Each call to {py:func}`Context.use`
    creates a new instance, so nested or concurrent (e.g. across async
    tasks) scopes on the same {py:class}`Context` each track their own
    token rather than overwriting shared state on the {py:class}`Context`
    itself.
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
    unset and no default is given to that particular {py:func}`Context.get`
    call. A default passed to {py:func}`Context.get`/`__call__` overrides
    the constructor's default for that call only.
    """

    __slots__ = ("_name", "_var", "_default")

    def __init__(self, name: str, default: T | _NoDefault = _NO_DEFAULT):
        """
        Initialize the context with a name and an optional fallback default.

        The constructor-level default is used whenever the variable is unset
        and no call-specific default is given to `__call__`/{py:func}`Context.get`.

        :param name: Name used for the underlying `ContextVar` and in error
            messages when the context is accessed while unset.
        :param default: Fallback value used when the variable is unset and no
            call-specific default is given. When omitted, there is no
            fallback and an unset access raises.
        :raises ValueError: If `IGNORE` is passed as the default; it is only
            meaningful as a call-specific default to {py:func}`Context.get`/`__call__`.

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

        :param default: Fallback value to use if the variable is unset. When
            omitted, falls back to the constructor's default, if any.
            Pass `IGNORE` to bypass the constructor's default and
            require a set value.
        :returns: The currently set value if any, otherwise the call-specific
            default if given, otherwise the constructor default.
        :raises RuntimeError: If the variable is unset and no default is
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
    """An alias of {py:func}`Context.get`. Calling an instance is equivalent to instance.get()"""

    def set(self, value: T) -> Token[T]:
        """
        Set the context value.

        :param value: Value to assign to the context variable.
        :returns: A token that can be passed to {py:func}`Context.reset` to
            restore the previous state.

        """
        return self._var.set(value)

    def reset(self, token: Token[T]) -> None:
        """
        Reset the context value to a previous state.

        :param token: Token returned by a prior call to {py:func}`Context.set`,
            identifying the state to restore.

        """
        self._var.reset(token)

    def use(self, value: T) -> _ContextScope[T]:
        """
        Create a context scope for the given value.

        :param value: Value to set for the duration of the returned context
            manager.
        :returns: A context manager that sets the value on entry (yielding
            the value) and restores the previous state on exit. Each call
            returns a fresh scope, so nested or concurrent uses are safe.

        """
        return _ContextScope(self, value)
