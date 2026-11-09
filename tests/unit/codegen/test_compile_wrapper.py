"""
Unit tests for compiling generated wrappers.

This test suite verifies the following behaviors:

compile_wrapper:
- It returns the `wrapper` function the source defines, running in the given namespace.
- inspect.getsource on the wrapper's code returns the generated source.
- A traceback through the wrapper shows its source line.
- The wrapper's source is dropped once the wrapper is garbage collected.
- Two wrappers of the same target keep their own sources.
"""

import gc
import inspect
import linecache
import traceback

import pytest

from stratae.codegen import compile_wrapper

_SOURCE = "def wrapper(x):\n    return __func__(x) + 1"
_RAISING = "def wrapper():\n    raise ValueError('generated')"


def _target(x: int) -> int:
    return x * 2


def test_returns_wrapper_running_in_namespace():
    """
    compile_wrapper should return the wrapper, resolving names from the namespace.

    Given: Source defining `wrapper`, which calls `__func__` from its namespace.
    When: The source is compiled and the wrapper called.
    Then: The wrapper calls the namespace's `__func__`.
    """
    # Act
    wrapper = compile_wrapper(_SOURCE, _target, {"__func__": _target})

    # Assert
    assert wrapper(3) == 7


def test_getsource_returns_generated_source():
    """
    inspect.getsource on the wrapper's code should return the generated source.

    Given: A compiled wrapper.
    When: inspect.getsource is called on its code object.
    Then: It returns the source the wrapper was compiled from.
    """
    # Arrange
    wrapper = compile_wrapper(_SOURCE, _target, {"__func__": _target})

    # Act
    source = inspect.getsource(wrapper.__code__)

    # Assert
    assert source == _SOURCE


def test_traceback_shows_source_line():
    """
    A traceback through the wrapper should show its source line.

    Given: A compiled wrapper that raises.
    When: It's called and the traceback formatted.
    Then: The traceback names the target and shows the raising line.
    """
    # Arrange
    wrapper = compile_wrapper(_RAISING, _target, {})

    # Act
    with pytest.raises(ValueError, match="generated") as raised:
        wrapper()

    # Assert
    formatted = "".join(traceback.format_exception(raised.value))
    assert "_target#" in formatted
    assert "raise ValueError('generated')" in formatted


def test_source_dropped_when_wrapper_collected():
    """
    The wrapper's source should be dropped once the wrapper is garbage collected.

    Given: A compiled wrapper whose source is registered.
    When: The wrapper is deleted and garbage collected.
    Then: Its source is no longer registered.
    """
    # Arrange
    wrapper = compile_wrapper(_SOURCE, _target, {"__func__": _target})
    filename = wrapper.__code__.co_filename
    assert filename in linecache.cache

    # Act
    del wrapper
    gc.collect()

    # Assert
    assert filename not in linecache.cache


def test_wrappers_of_same_target_keep_own_source():
    """
    Two wrappers of the same target should keep their own sources.

    Given: Two different sources, both wrapping the same target.
    When: Both are compiled.
    Then: Each wrapper's code returns its own source.
    """
    # Act
    first = compile_wrapper(_SOURCE, _target, {"__func__": _target})
    second = compile_wrapper(_RAISING, _target, {})

    # Assert
    assert inspect.getsource(first.__code__) == _SOURCE
    assert inspect.getsource(second.__code__) == _RAISING
