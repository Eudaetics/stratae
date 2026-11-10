"""
Context managers for temporarily replacing injected dependency values.

{py:func}`override` names a provider, and `.to(value)` gives the value to inject in its place.
The value's type is checked against what the provider returns, or resolves to if it's async.
Entering the override replaces the provider, and exiting restores it. Overrides are
context-local, so concurrent tasks don't interfere. A task or thread started inside the
block copies its context. It keeps the override for as long as it runs, even after the block
exits. To replace several providers, enter several overrides in one `with` statement. If one
fails to enter, those already entered are restored.

````{example} Swapping dependencies for test doubles
```{code-block} python
from typing import Annotated
from stratae.depends import Depends, inject, override

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

with (
    override(get_user_repository).to(UserRepository({1: "test@example.com"})),
    override(get_email_service).to(EmailService(sender="test@example.com")),
):
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

from collections.abc import Awaitable, Callable, Generator
from contextvars import Token
from typing import Any, overload
from weakref import finalize

from stratae.depends._provide import Provider


class _Held:
    """
    An override value as its provider's ContextVar holds it, and awaitable for an async one.

    It lives as long as some context can see it, including contexts copied by tasks started
    inside the override. Its finalizer tells the provider when to leave the override path.
    """

    __slots__ = ("__weakref__", "value")

    def __init__(self, value: Any) -> None:
        self.value = value

    def __await__(self) -> Generator[None, None, Any]:
        if False:  # noqa: S5797
            yield
        return self.value


class _Override[T]:
    """A provider's replacement value, applied while the context manager is entered."""

    __slots__ = ("dep", "tokens", "value")

    def __init__(self, dep: Provider, value: T) -> None:
        self.dep: Provider = dep
        self.value: T = value
        self.tokens: list[Token[Any]] = []

    def __enter__(self) -> None:
        held = _Held(self.value)
        self.dep.hold()
        finalize(held, self.dep.release)
        self.tokens.append(self.dep.override.set(held))

    def __exit__(self, *_: object) -> None:
        # Popped only once reset succeeds, so an exit from the wrong context can be retried.
        self.dep.override.reset(self.tokens[-1])
        self.tokens.pop()


class _Target[T]:
    """A provider named by {py:func}`override`, waiting for its replacement value."""

    __slots__ = ("_dep",)

    def __init__(self, dep: Provider) -> None:
        self._dep = dep

    def to(self, value: T) -> _Override[T]:
        """
        Give the value to inject in place of the provider.

        :param value: The value to inject, of the type the provider returns, or resolves to
            if it's async. It's used as-is, never called.
        :returns: A reentrant context manager, not safe to share between concurrent tasks.
        """
        return _Override(self._dep, value)


@overload
def override[T](func: Callable[..., Awaitable[T]]) -> _Target[T]: ...
@overload
def override[T](func: Callable[..., T]) -> _Target[T]: ...
def override(func: Callable[..., Any]) -> _Target[Any]:
    """
    Name a provider to replace temporarily; `.to(value)` gives the replacement.

    :param func: A provider passed to {py:func}`Depends <stratae.depends.injection.Depends>`.
    :returns: An object whose `to` method takes the replacement value.
    """
    return _Target(Provider(func))
