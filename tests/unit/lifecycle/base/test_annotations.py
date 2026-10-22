"""Test that caching a function doesn't evaluate its annotations."""

import sys
from typing import Any

import pytest

requires_314 = pytest.mark.skipif(
    sys.version_info < (3, 14), reason="annotations are only evaluated lazily on 3.14+"
)

_SOURCES = {
    "sync": "@scope.cache()\ndef get(user: Later) -> Later:\n    return 1\n",
    "async": "@scope.cache()\nasync def get(user: Later) -> Later:\n    return 1\n",
    "resource": "@scope.cache()\n@resource\ndef get(user: Later):\n    yield 1\n",
}


@requires_314
@pytest.mark.parametrize("kind", list(_SOURCES))
def test_cache_allows_annotations_defined_later(kind: str):
    """
    A cached function may be annotated with a class defined later in its module.

    Given: A function whose annotations name a class that isn't defined yet
    When: It is decorated with a scope's cache
    Then: Decoration succeeds, without evaluating the annotations
    """
    # Arrange
    namespace: dict[str, Any] = {}
    source = (
        "from stratae.lifecycle import AsyncScope, resource\n"
        "scope = AsyncScope('request')\n" + _SOURCES[kind] + "class Later: ...\n"
    )

    # Act
    exec(compile(source, "<test>", "exec"), namespace)  # noqa: S102

    # Assert
    assert callable(namespace["get"])
