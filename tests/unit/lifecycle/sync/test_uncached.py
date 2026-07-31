"""Test suite for Scope.cache's `.uncached` escape hatch to call the original function directly."""

from unittest.mock import Mock

from stratae.lifecycle.resource import resource
from stratae.lifecycle.scope import Scope


class SimpleObject:
    """Create a simple object with a value for testing."""

    def __init__(self, value: int):
        """Initialize the SimpleObject with a value."""
        self.value = value


def test_uncached_is_the_original_function(scope: Scope):
    """
    `.uncached` is the exact pre-decoration function, not a fresh wrapper.

    Given: A plain function decorated with `scope.cache()`,
    When: Comparing its `.uncached` attribute to the original function object,
    Then: They are the same object.
    """

    # Arrange
    def get_value() -> int:
        return 1

    original = get_value
    cached = scope.cache()(get_value)

    # Assert
    assert cached.uncached is original


def test_uncached_parameter_types(scope: Scope):
    """
    `.uncached` accepts the full range of parameter kinds, forwarded exactly like caching does.

    Given: A cached function with positional-only, positional-or-keyword, *args,
           keyword-only, and **kwargs parameters,
    When: `.uncached` is called with a matching mix of positional and keyword arguments,
    Then: The arguments are forwarded correctly, and each call computes a fresh,
          uncached result.
    """

    # Arrange
    class Container:
        def __init__(self, value: int):
            self.value = value

    @scope.cache()
    def foo(x: int, /, y: int, z: int = 0, *args: int, c: int = 1, **kwargs: int) -> Container:
        return Container(x + y + z + c + sum(args) + sum(kwargs.values()))

    # Act
    result1 = foo.uncached(1, 2)
    result2 = foo.uncached(1, 2)
    complex_result = foo.uncached(1, 1, 1, 1, 1, 1, c=1, f=1)

    # Assert
    assert result1.value == 4
    assert result1 is not result2
    assert complex_result.value == 8


def test_uncached_plain_function_outside_scope(scope: Scope):
    """
    `.uncached` is callable without the owning scope active, and isn't cached.

    Given: A plain function cached in a scope,
    When: `.uncached` is called multiple times while the scope is inactive,
    Then: No error is raised, and the function runs fresh on every call.
    """
    # Arrange
    call_counter = Mock()

    @scope.cache()
    def get_object() -> SimpleObject:
        call_counter()
        return SimpleObject(1)

    # Act
    obj1 = get_object.uncached()
    obj2 = get_object.uncached()

    # Assert
    assert obj1 is not obj2
    assert call_counter.call_count == 2


def test_uncached_plain_function_inside_scope(scope: Scope):
    """
    `.uncached` bypasses the cache even while the owning scope is active.

    Given: A plain function cached in a scope,
    When: The scope is active, the cached call is made twice, then `.uncached` is
          called twice,
    Then: The cached calls return the same value, but each `.uncached` call computes
          and returns its own fresh, independent value.
    """
    # Arrange
    call_counter = Mock()

    @scope.cache()
    def get_object() -> SimpleObject:
        call_counter()
        return SimpleObject(1)

    # Act
    with scope.activate():
        cached1 = get_object()
        cached2 = get_object()
        uncached1 = get_object.uncached()
        uncached2 = get_object.uncached()

    # Assert
    assert cached1 is cached2
    assert uncached1 is not uncached2
    assert uncached1 is not cached1
    assert call_counter.call_count == 3


def test_uncached_resource_outside_scope(scope: Scope):
    """
    `.uncached` for a `resource`-tagged function is the plain, unentered context manager.

    Given: A `resource`-tagged generator function cached in a scope,
    When: `.uncached` is called and entered as a context manager without the scope
          being active,
    Then: The resource is entered and cleaned up for that call alone, exactly like
          using the undecorated `@resource` function directly.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()

    @scope.cache()
    @resource
    def get_object():
        call_counter()
        try:
            yield SimpleObject(1)
        finally:
            cleanup_counter()

    # Act
    with get_object.uncached() as obj1:
        assert obj1.value == 1
        cleanup_counter.assert_not_called()

    # Assert
    call_counter.assert_called_once()
    cleanup_counter.assert_called_once()


def test_uncached_resource_inside_scope(scope: Scope):
    """
    `.uncached` for a resource bypasses the scope's cache and exit stack even when active.

    Given: A `resource`-tagged function cached in an active scope,
    When: The cached call enters and caches the resource for the scope's lifetime,
          then `.uncached` is entered and exited as its own context manager,
    Then: `.uncached`'s resource is a distinct instance, cleaned up at its own `with`
          block exit rather than deferred to scope deactivation.
    """
    # Arrange
    call_counter = Mock()
    cleanup_counter = Mock()

    @scope.cache()
    @resource
    def get_object():
        call_counter()
        try:
            yield SimpleObject(1)
        finally:
            cleanup_counter()

    # Act & Assert
    with scope.activate():
        cached = get_object()
        assert call_counter.call_count == 1
        cleanup_counter.assert_not_called()

        with get_object.uncached() as uncached:
            assert uncached is not cached
            assert call_counter.call_count == 2
            cleanup_counter.assert_not_called()

        assert cleanup_counter.call_count == 1

    assert cleanup_counter.call_count == 2
