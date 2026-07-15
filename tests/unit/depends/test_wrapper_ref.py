"""
Memory tests for the dependency injection registries.

Locally defined injected functions must be collectable once user code drops them: a
long-running process that defines and injects functions inside handlers would otherwise
grow the DependsWrapper registry forever. The registry holds its wrappers weakly and the
Resolver keeps no state, so the only references to an injection graph are the user's own.
"""

import gc
import typing
import weakref
from typing import Any
from weakref import ReferenceType

from stratae.depends import Depends, DependsWrapper, inject
from stratae.depends.inject import Injected


def test_dropped_injection_graph_is_collected():
    """
    A locally defined injected function is garbage collected once dropped.

    Given: an injected function, its dependency, and the dependency's DependsWrapper,
        all defined locally with no surviving user references,
    When: garbage collection runs,
    Then: the whole graph should be collected, including the registry's DependsWrapper.
    """

    # Arrange
    # The graph is built in a nested call so its frame is gone before collection runs;
    # only these weak references survive into the test frame.
    def build() -> tuple[ReferenceType[Any], ReferenceType[Any], ReferenceType[DependsWrapper]]:
        def local_dep() -> int:
            return 1

        @inject
        def local_func(dep: Injected[int, Depends(local_dep)]) -> int:
            return dep

        assert local_func() == 1
        return (
            weakref.ref(local_func),
            weakref.ref(local_dep),
            weakref.ref(DependsWrapper.find(local_dep)),
        )

    wrapper_ref, dep_ref, depends_ref = build()

    assert wrapper_ref() is not None
    assert dep_ref() is not None
    assert depends_ref() is not None

    # Act
    gc.collect()

    # Assert
    assert wrapper_ref() is None

    # typing memoizes Annotated parameterizations. Since the ref is inside the Injected
    # alias for Annotated, kick the values out of the cache
    for cleanup in getattr(typing, "_cleanups", []):
        cleanup()
    gc.collect()

    assert dep_ref() is None
    assert depends_ref() is None


def test_live_consumer_keeps_dependency_registered():
    """
    A dependency stays registered while an injected consumer is alive.

    Given: an injected function that is still referenced,
    When: garbage collection runs,
    Then: the dependency's DependsWrapper should survive and remain findable.
    """

    # Arrange
    def local_dep() -> int:
        return 2

    @inject
    def local_func(dep: Injected[int, Depends(local_dep)]) -> int:
        return dep

    depends_ref = weakref.ref(DependsWrapper.find(local_dep))

    # Act
    gc.collect()

    # Assert
    assert depends_ref() is not None
    assert DependsWrapper.find(local_dep) is depends_ref()
    assert local_func() == 2
