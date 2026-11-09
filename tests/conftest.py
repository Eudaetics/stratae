"""Pytest fixtures providing Scope/AsyncScope objects, and snapshot checks, for tests."""

import os
from collections.abc import Callable

import pytest

from stratae.lifecycle import AsyncScope, Scope

_UPDATE_SNAPSHOTS = os.environ.get("STRATAE_UPDATE_SNAPSHOTS") == "1"


@pytest.fixture
def snapshot(request: pytest.FixtureRequest) -> Callable[[str, str], None]:
    """
    Provide a check of text against a stored snapshot.

    Call it as `snapshot(name, text)`. The snapshot is `snapshots/<name>.txt`, beside the test
    file. Set `STRATAE_UPDATE_SNAPSHOTS=1` to record snapshots instead of checking them.
    """
    folder = request.path.parent / "snapshots"

    def check(name: str, text: str) -> None:
        path = folder / f"{name}.txt"
        if _UPDATE_SNAPSHOTS:
            folder.mkdir(exist_ok=True)
            path.write_text(text + "\n")
            return
        assert path.exists(), f"No snapshot {path}. Record it with STRATAE_UPDATE_SNAPSHOTS=1."
        assert text + "\n" == path.read_text()

    return check


@pytest.fixture
def application_scope():
    """Provide a shared-isolation, dense-storage application Scope."""
    return Scope("application", "shared")


@pytest.fixture
def request_scope():
    """Provide a context-isolation, dense-storage request Scope."""
    return Scope("request", "context")


@pytest.fixture
def async_application_scope():
    """Provide a shared-isolation, dense-storage application AsyncScope."""
    return AsyncScope("application", "shared")


@pytest.fixture
def async_request_scope():
    """Provide a context-isolation, dense-storage request AsyncScope."""
    return AsyncScope("request", "context")
