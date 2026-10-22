"""
Callable, injectable values backed by contextvars.

{py:class}`Context` wraps a `contextvars.ContextVar`. Set it with {py:func}`Context.set`
or {py:func}`Context.use`, and read it by calling the instance. Being callable, it works as
a {py:func}`Depends <stratae.depends.Depends>` provider for runtime values like the current
user.

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
On Python 3.14+, the token from {py:func}`Context.set` is itself a context manager:
<!--- skip: next if(__import__("sys").version_info < (3, 14), "needs Python 3.14+") -->
```{code-block} python
from stratae.context import Context

user_id = Context[int]("user_id")

with user_id.set(99):
    assert user_id() == 99
```

On earlier versions, use {py:func}`Context.use`.
````
"""

from contextvars import ContextVar, Token


class _NoDefault:
    __slots__ = ()


_NO_DEFAULT = _NoDefault()
IGNORE_DEFAULT = _NoDefault()
"""
Sentinel passed as a 'default' to skip the constructor default and require a set value.

{py:class}`Context` uses similar default behavior as defined in
[`ContextVar`](https://docs.python.org/3/library/contextvars.html#contextvars.ContextVar.get).
If there is no value set in the current context, then {py:func}`Context.get` returns first
the default provided to {py:func}`Context.get`, then the constructor level default, and
finally raises a RuntimeError.
The `IGNORE_DEFAULT` sentinel provides a third option. If the {py:func}`Context.get` is called with
`IGNORE_DEFAULT`, then it will raise a RuntimeError if no value is set and ignore the constructor
level default.
"""


class _ContextScope[T]:
    """Context manager returned by {py:func}`Context.use`, tracking its own token."""

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
    """A named value backed by a ContextVar, usable as a Depends() provider."""

    __slots__ = ("_name", "_var", "_default")

    def __init__(self, name: str, default: T | _NoDefault = _NO_DEFAULT):
        """
        Create a context.

        :param name: Name for the `ContextVar` and error messages.
        :param default: Value returned while unset. If omitted, an unset read raises.
        :raises ValueError: If `default` is `IGNORE_DEFAULT`.
        """
        if default is IGNORE_DEFAULT:
            raise ValueError("IGNORE_DEFAULT cannot be used as a constructor default.")
        self._name = name
        self._var: ContextVar[T] = ContextVar(name)
        self._default = default

    def get(self, default: T | _NoDefault = _NO_DEFAULT) -> T:
        """
        Get the current value. Equivalent to calling the context.

        :param default: Value returned if unset, overriding the constructor's default. Pass
            `IGNORE_DEFAULT` to require a set value.
        :returns: The set value, otherwise a default.
        :raises RuntimeError: If unset with no default.
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

        :param value: The value to set.
        :returns: A token for {py:func}`Context.reset`.
        """
        return self._var.set(value)

    def reset(self, token: Token[T]) -> None:
        """
        Restore the value from before a {py:func}`Context.set` call.

        :param token: The token that call returned.
        """
        self._var.reset(token)

    def use(self, value: T) -> _ContextScope[T]:
        """
        Set a value for the duration of a `with` block.

        :param value: The value to set.
        :returns: A context manager that yields `value` and restores the previous state on exit.
        """
        return _ContextScope(self, value)
