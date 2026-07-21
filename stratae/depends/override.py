"""
Context managers for temporarily replacing injected dependency values.

{py:func}`override` swaps a single provider's value and {py:func}`overrides`
swaps several at once. Both return context managers and the previous state is
restored on exit. Overrides are stored per
{py:class}`Provider <stratae.depends._provide.Provider>` in context-local
state, so concurrent tasks can hold different overrides for the same
provider without interfering.

```{rubric} Example:
```
```{code-block} python
:caption: Swap a database provider for a fake within a scope

from stratae.depends import Depends, Injected, inject, override

class Database:
    def __init__(self, name: str):
        self.name = name

def get_db() -> Database:
    return Database("production")

@inject
def get_db_name(db: Injected[Database, Depends(get_db)]) -> str:
    return db.name

assert get_db_name() == "production"  # db resolved by calling get_db()

with override(get_db, Database("test")):
    assert get_db_name() == "test"  # db is the test within this scope

assert get_db_name() == "production"
```

See {py:func}`override` and {py:func}`overrides` for additional examples.

"""

from contextvars import Token
from typing import Any, Callable

from stratae.depends._provide import Provider

_OverrideMap = dict[Callable[..., Any], Any]


class _ReusableAwaitable:
    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __await__(self):
        if False:  # noqa: S5797
            yield
        return self.value


class _Override:
    __slots__ = ("dep", "value", "token")

    def __init__(self, dep: Provider, value: Any):
        self.dep = dep
        self.value = value
        self.token: Token[Any]

    def __enter__(self):
        with self.dep.lock:
            if self.dep.override_count == 0:
                self.dep.provide = self.dep.provide_override
            self.dep.override_count += 1
            self.token = self.dep.override.set(
                _ReusableAwaitable(self.value) if self.dep.is_async else self.value
            )

    def __exit__(self, *_: object) -> None:
        with self.dep.lock:
            self.dep.override.reset(self.token)
            self.dep.override_count -= 1
            if self.dep.override_count == 0:
                self.dep.provide = self.dep.dependency


class _Overrides:
    __slots__ = ("_items",)

    def __init__(self, mapping: _OverrideMap) -> None:
        self._items: list[_Override] = [
            _Override(Provider.find(func), value) for func, value in mapping.items()
        ]

    def __enter__(self) -> None:
        entered: list[_Override] = []
        try:
            for o in self._items:
                o.__enter__()
                entered.append(o)
        except BaseException:
            for o in reversed(entered):
                o.__exit__()
            raise

    def __exit__(self, *_: object) -> None:
        for o in self._items:
            o.__exit__()


def override(func: Callable[..., Any], value: Any) -> _Override:
    """
    Temporarily replace a single dependency's value within a scope.

    :param func: The provider callable that was passed to
        {py:func}`Depends <stratae.depends.inject.Depends>`.
    :param value: Value injected in place of calling the provider. Used
        as-is; it is not called, even if the provider is async.
    :returns: A context manager holding the override between entry and exit.
    :raises DependencyNotFoundError: If `func` was never passed to
        {py:func}`Depends <stratae.depends.inject.Depends>`.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Force a feature flag on for a single test

    from stratae.depends import Depends, Injected, inject, override

    def is_beta_enabled() -> bool:
        return False

    @inject
    def get_banner(enabled: Injected[bool, Depends(is_beta_enabled)]) -> str:
        return "Beta banner" if enabled else "No banner"

    assert get_banner() == "No banner"

    with override(is_beta_enabled, True):
        assert get_banner() == "Beta banner"  # flag forced on within this scope

    assert get_banner() == "No banner"
    ```

    """
    return _Override(Provider.find(func), value)


def overrides(mapping: _OverrideMap) -> _Overrides:
    """
    Temporarily replace several dependencies' values within one scope.

    :param mapping: Map of provider callables, as passed to
        {py:func}`Depends <stratae.depends.inject.Depends>`, to their
        replacement values.
    :returns: A context manager applying every override on entry and
        restoring the previous state on exit. If applying one override
        fails, those already applied are unwound before the error
        propagates.
    :raises ValueError: If `mapping` is empty.
    :raises DependencyNotFoundError: If any key was never passed to
        {py:func}`Depends <stratae.depends.inject.Depends>`.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Swap both a database and mailer provider for fakes within one scope

    from stratae.depends import Depends, Injected, inject, overrides

    class Database:
        def __init__(self, name: str):
            self.name = name

    class Mailer:
        def __init__(self, name: str):
            self.name = name

    def get_db() -> Database:
        return Database("production")

    def get_mailer() -> Mailer:
        return Mailer("production")

    @inject
    def get_names(
        db: Injected[Database, Depends(get_db)],
        mailer: Injected[Mailer, Depends(get_mailer)],
    ) -> tuple[str, str]:
        return db.name, mailer.name

    assert get_names() == ("production", "production")

    with overrides({get_db: Database("test"), get_mailer: Mailer("test")}):
        assert get_names() == ("test", "test")  # both are test mocks within this scope

    assert get_names() == ("production", "production")
    ```

    """
    if not mapping:
        raise ValueError("overrides() requires at least one entry.")
    return _Overrides(mapping)
