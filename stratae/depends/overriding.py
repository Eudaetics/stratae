"""
Context managers for temporarily replacing injected dependency values.

{py:func}`override` replaces one provider's value and {py:func}`overrides` replaces several,
restoring them on exit. Overrides are context-local, so concurrent tasks don't interfere.

````{example} Swapping dependencies for test doubles
```{code-block} python
from typing import Annotated
from stratae.depends import Depends, inject, overrides

class UserRepository:
    def __init__(self, users: dict[int, str]):
        self._users = users

    def get_email(self, user_id: int) -> str:
        return self._users[user_id]

class EmailService:
    def __init__(self, sender: str):
        self.sender = sender

    def send(self, to: str, subject: str) -> str:
        return f"from={self.sender} to={to} subject={subject!r}"

def get_user_repository() -> UserRepository:
    return UserRepository({1: "jane@example.com"})

def get_email_service() -> EmailService:
    return EmailService(sender="notifications@example.com")

@inject
def notify_user(
    user_id: int,
    repo: Annotated[UserRepository, Depends(get_user_repository)],
    mailer: Annotated[EmailService, Depends(get_email_service)],
) -> str:
    return mailer.send(repo.get_email(user_id), "Welcome!")

print(notify_user(1))

with overrides({
    get_user_repository: UserRepository({1: "test@example.com"}),
    get_email_service: EmailService(sender="test@example.com"),
}):
    print(notify_user(1))

print(notify_user(1))
```
```{output}
from=notifications@example.com to=jane@example.com subject='Welcome!'
from=test@example.com to=test@example.com subject='Welcome!'
from=notifications@example.com to=jane@example.com subject='Welcome!'
```
````
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
    __slots__ = ("dep", "value", "tokens")

    def __init__(self, dep: Provider, value: Any):
        self.dep = dep
        self.value = value
        self.tokens: list[Token[Any]] = []

    def __enter__(self):
        with self.dep.lock:
            if self.dep.override_count == 0:
                self.dep.provide = self.dep.provide_override
            self.dep.override_count += 1
            self.tokens.append(
                self.dep.override.set(
                    _ReusableAwaitable(self.value) if self.dep.is_async else self.value
                )
            )

    def __exit__(self, *_: object) -> None:
        with self.dep.lock:
            try:
                self.dep.override.reset(self.tokens.pop())
            finally:
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
        for o in reversed(self._items):
            o.__exit__()


def override(func: Callable[..., Any], value: Any) -> _Override:
    """
    Temporarily replace one dependency's value.

    :param func: A provider passed to {py:func}`Depends <stratae.depends.injection.Depends>`.
    :param value: The value to inject. It's used as-is, never called.
    :returns: A reentrant context manager, not safe to share between concurrent tasks.
    :raises DependencyNotFoundError: If `func` was never passed to `Depends`.
    """
    return _Override(Provider.find(func), value)


def overrides(mapping: _OverrideMap) -> _Overrides:
    """
    Temporarily replace several dependencies' values.

    :param mapping: Providers mapped to replacement values.
    :returns: A context manager like {py:func}`override`'s, applying every override.
    :raises ValueError: If `mapping` is empty.
    :raises DependencyNotFoundError: If any provider was never passed to `Depends`.
    """
    if not mapping:
        raise ValueError("overrides() requires at least one entry.")
    return _Overrides(mapping)
