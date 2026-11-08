"""
Type-checking tests for override(provider).to(value).

Each `# pyright: ignore[...]` here marks an expected type error. The project enables
`reportUnnecessaryTypeIgnoreComment`, so if one of these stops being an error, the type check
fails. A line without one must type-check.

This test suite verifies the following behaviors:

Accepted values:
- A plain provider takes its return type, or a subclass.
- An async provider takes the type it resolves to, not an awaitable.
- A Context provider takes its value type.
- A cached sync or async resource takes the value it yields.

Refused values:
- A value of the wrong type is a type error, for plain, async, Context, and cached sync and
  async resource providers.
- A provider without .to() is a type error in a with statement, and raises at runtime.
"""

import asyncio
from collections.abc import AsyncGenerator, Generator

import pytest

from stratae.context import Context
from stratae.depends import Depends, override
from stratae.lifecycle import AsyncScope, Scope
from stratae.lifecycle.resource import async_resource, resource


class _Repo:
    pass


class _FakeRepo(_Repo):
    pass


class _Session:
    pass


def _get_repo() -> _Repo:
    return _Repo()


async def _get_repo_async() -> _Repo:
    await asyncio.sleep(0)
    return _Repo()


_current_user = Context[str]("override_typing_user")
_app = Scope("override_typing_app")
_request = AsyncScope("override_typing_request")


@_app.cache()
@resource
def _get_session() -> Generator[_Session]:
    yield _Session()


@_request.cache()
@async_resource
async def _get_session_async() -> AsyncGenerator[_Session]:
    await asyncio.sleep(0)
    yield _Session()


# The registry holds providers weakly, as an injected function's annotations would hold them.
_repo_provider = Depends(_get_repo)
_repo_async_provider = Depends(_get_repo_async)
_user_provider = Depends(_current_user)
_session_provider = Depends(_get_session)
_session_async_provider = Depends(_get_session_async)


def test_matching_values_accepted() -> None:
    """
    A value of the provider's type should type-check, for every kind of provider.

    Given: Plain, async, Context, and cached sync and async resource providers
    When: Each is overridden with a value of the type it gives
    Then: The type check passes, and each override applies its value
    """
    # Arrange
    repo, fake, session = _Repo(), _FakeRepo(), _Session()

    # Act / Assert
    with override(_get_repo).to(repo):
        assert _repo_provider.provide() is repo
    with override(_get_repo).to(fake):
        assert _repo_provider.provide() is fake
    with override(_get_repo_async).to(repo), override(_current_user).to("jane"):
        assert _user_provider.provide() == "jane"
    with override(_get_session).to(session), override(_get_session_async).to(session):
        assert _session_provider.provide() is session


def test_wrong_values_refused() -> None:
    """
    A value of the wrong type for its provider should be a type error.

    Given: Plain, async, Context, and cached sync and async resource providers
    When: Each is overridden with a value of another type
    Then: Each `.to()` call is a type error, marked by its expected-error comment
    """
    # Act / Assert
    override(_get_repo).to("wrong")  # pyright: ignore[reportArgumentType]
    override(_get_repo_async).to("wrong")  # pyright: ignore[reportArgumentType]
    override(_current_user).to(42)  # pyright: ignore[reportArgumentType]
    override(_get_session).to(_Repo())  # pyright: ignore[reportArgumentType]
    override(_get_session_async).to(_Repo())  # pyright: ignore[reportArgumentType]


def test_missing_value_refused() -> None:
    """
    A provider named without `.to()` should not be usable as an override.

    Given: A provider passed to override, with no value given
    When: It's used in a with statement
    Then: It's a type error, and it raises TypeError at runtime
    """
    # Act / Assert
    with pytest.raises(TypeError), override(_get_repo):  # pyright: ignore[reportGeneralTypeIssues]
        pass
