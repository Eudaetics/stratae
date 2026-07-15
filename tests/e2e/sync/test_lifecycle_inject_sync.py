"""Tests for async sync lifecycle management with dependency injection."""

from unittest.mock import Mock

import pytest

from stratae.context import Context
from stratae.depends import Depends, Injected, inject
from stratae.lifecycle import Lifecycle, resource


def test_lifecycle_inject_sync(lifecycle: Lifecycle):
    """
    Test lifecycle management with dependency injection with sync dependencies.

    Given: A Lifecycle with application scope.
    When: A dependency is injected at application scope.
    Then: The dependency should be created once per application scope.
    """

    # Arrange
    @lifecycle.cache("application")
    @inject
    def get_db(db: Injected[object, Depends(lambda: object())]) -> object:
        return db

    # Act / Assert
    with lifecycle.start("application"):
        assert get_db() is get_db()


def test_lifecycle_inject_nested_sync(lifecycle: Lifecycle):
    """
    Test nested lifecycle management with dependency injection with sync dependencies.

    Given: A Lifecycle with application and request scopes.
    When: A dependency is injected at application scope and another at request scope.
    Then: The application scope dependency should be shared across requests, while the request
          scope dependency should be unique per request.
    """

    # Arrange
    @lifecycle.cache("application")
    @inject
    def get_app_db(db: Injected[object, Depends(lambda: object())]) -> object:
        return db

    @lifecycle.cache("request")
    @inject
    def get_request_db(db: Injected[object, Depends(lambda: object())]) -> object:
        return db

    # Act / Assert
    with lifecycle.start("application"):
        app_db_instance = get_app_db()
        assert isinstance(app_db_instance, object)
        assert get_app_db() is app_db_instance
        with lifecycle.start("request"):
            request_db_instance_1 = get_request_db()
            assert get_app_db() is app_db_instance
            assert get_request_db() is request_db_instance_1
        with lifecycle.start("request"):
            request_db_instance_2 = get_request_db()
            assert get_app_db() is app_db_instance
            assert get_request_db() is request_db_instance_2
            assert request_db_instance_1 is not request_db_instance_2
        assert get_app_db() is app_db_instance


def test_lifecycle_inject_sync_generator(lifecycle: Lifecycle):
    """
    Test lifecycle management with dependency injection using sync generator dependencies.

    Given: A Lifecycle with application scope.
    When: A generator dependency is injected at application scope.
    Then: The generator should yield the same instance within the same application scope.
    """

    # Arrange
    class SimpleObject:
        pass

    mock_cleanup = Mock()

    @lifecycle.cache("application")
    @inject
    @resource
    def get_resource(db: Injected[SimpleObject, Depends(lambda: SimpleObject())]):
        try:
            yield db
        finally:
            mock_cleanup()

    # Act / Assert
    with lifecycle.start("application"):
        assert isinstance(get_resource(), SimpleObject)
        assert get_resource() is get_resource()
        mock_cleanup.assert_not_called()
    mock_cleanup.assert_called_once()


def test_lifecycle_inject_nested_sync_generator(lifecycle: Lifecycle):
    """
    Test nested lifecycle management with dependency injection using sync generator dependencies.

    Given: A Lifecycle with application and request scopes.
    When: A generator dependency is injected at application scope and another at request scope.
    Then: The application scope generator should yield the same instance across requests, while
          the request scope generator should yield unique instances per request.
    """

    # Arrange
    class SimpleObject:
        pass

    mock_app_cleanup = Mock()
    mock_request_cleanup = Mock()

    @lifecycle.cache("application")
    @inject
    @resource
    def get_app_resource(resource: Injected[SimpleObject, Depends(lambda: SimpleObject())]):
        try:
            yield resource
        finally:
            mock_app_cleanup()

    @lifecycle.cache("request")
    @inject
    @resource
    def get_request_resource(resource: Injected[SimpleObject, Depends(lambda: SimpleObject())]):
        try:
            yield resource
        finally:
            mock_request_cleanup()

    # Act / Assert
    with lifecycle.start("application"):
        app_resource_instance = get_app_resource()
        assert isinstance(app_resource_instance, SimpleObject)

        with lifecycle.start("request"):
            request_resource_instance_1 = get_request_resource()
            assert isinstance(request_resource_instance_1, SimpleObject)
            assert get_app_resource() is app_resource_instance
            assert get_request_resource() is request_resource_instance_1
        with lifecycle.start("request"):
            request_resource_instance_2 = get_request_resource()
            assert isinstance(request_resource_instance_2, SimpleObject)
            assert get_app_resource() is app_resource_instance
            assert get_request_resource() is request_resource_instance_2
            assert request_resource_instance_1 is not request_resource_instance_2
    mock_app_cleanup.assert_called_once()
    assert mock_request_cleanup.call_count == 2


def test_lifecycle_inject_sync_with_exception(lifecycle: Lifecycle):
    """
    Test using sync dependencies that raise an exception.

    Given: A Lifecycle with application scope.
    When: A dependency is injected at application scope that raises an exception.
    Then: The exception should propagate correctly.
    """

    # Arrange
    class SimpleObject:
        pass

    @lifecycle.cache("application")
    @inject
    @resource
    def get_resource(_: Injected[SimpleObject, Depends(lambda: SimpleObject())]):
        yield
        raise ValueError("Simulated exception")

    # Act / Assert
    with pytest.raises(ValueError):
        with lifecycle.start("application"):
            get_resource()


def test_lifecycle_inject_sync_gen_with_exception(lifecycle: Lifecycle):
    """
    Test using sync generator dependencies that raise an exception.

    Given: A Lifecycle with application scope.
    When: A generator dependency is injected at application scope that raises an exception.
    Then: The exception should propagate correctly.
    """

    # Arrange
    class SimpleObject:
        pass

    mock_cleanup = Mock()
    mock_side_effect = Mock()

    @lifecycle.cache("application")
    @inject
    @resource
    def get_resource(db: Injected[SimpleObject, Depends(lambda: SimpleObject())]):
        try:
            yield db
            mock_side_effect()
        finally:
            mock_cleanup()
            raise ValueError("Simulated exception after yield")

    # Act / Assert
    with pytest.raises(ValueError, match="Simulated exception after yield"):
        with lifecycle.start("application"):
            resource_instance = get_resource()
            assert isinstance(resource_instance, SimpleObject)

    assert mock_side_effect.call_count == 1
    assert mock_cleanup.call_count == 1


def test_lifecycle_inject_sync_gen_with_multiple_exceptions(lifecycle: Lifecycle):
    """
    Test using multiple sync generator dependencies that raise an exception.

    Given: A Lifecycle with application scope.
    When: Generator dependencies are injected at application scope that raise an exception.
    Then: The exceptions should propagate correctly as an ExceptionGroup.
    """

    # Arrange
    class SimpleObject:
        pass

    mock_cleanup = Mock()
    mock_side_effect = Mock()
    mock_catch = Mock()

    @lifecycle.cache("application")
    @inject
    @resource
    def get_one(db: Injected[SimpleObject, Depends(lambda: SimpleObject())]):
        try:
            yield db
            mock_side_effect()
        except Exception:
            mock_catch()
            raise
        finally:
            mock_cleanup()
            raise ValueError("Simulated exception after yield")

    @lifecycle.cache("application")
    @inject
    @resource
    def get_two(db: Injected[SimpleObject, Depends(lambda: SimpleObject())]):
        try:
            yield db
            mock_side_effect()
        finally:
            mock_cleanup()
            raise AttributeError("Simulated exception after yield")

    # Act / Assert
    with pytest.raises(ExceptionGroup) as exceptions:
        with lifecycle.start("application"):
            resource_instance = get_one()
            resource_instance_2 = get_two()
            assert isinstance(resource_instance, SimpleObject)
            assert isinstance(resource_instance_2, SimpleObject)

    assert len(exceptions.value.exceptions) == 2
    assert any(
        isinstance(exc, ValueError) and str(exc) == "Simulated exception after yield"
        for exc in exceptions.value.exceptions
    )
    assert any(
        isinstance(exc, AttributeError) and str(exc) == "Simulated exception after yield"
        for exc in exceptions.value.exceptions
    )
    assert mock_side_effect.call_count == 1
    assert mock_cleanup.call_count == 2
    assert mock_catch.call_count == 1


def test_lifecycle_outer_cache_and_inject_change(lifecycle: Lifecycle):
    """
    Test lifecycle caching when cache is the outer decorator and injected values change.

    The order of the decorators for caching and dependency injection is important. When the cache
    decorator is the outermost decorator, the cache key generation happens before dependency
    injection. If a dependency returns a value that changes, the cache will not reflect that
    change unless the cache key generation accounts for it.

    Given: A function with a cache decorator outside of dependency injection.
    When: A dependency is injected with different argument values.
    Then: The first cached value should be returned regardless of value changes.
    """
    # Arrange
    mock = Mock()

    @lifecycle.cache("application")
    @inject
    def get_value(x: Injected[int, Depends(lambda: mock.call_count)]) -> int:
        mock()
        return x + 1

    # Act / Assert
    with lifecycle.start("application"):
        assert get_value() == 1
        assert get_value() == 1


def test_lifecycle_outer_cache_inject_change_with_custom_key(lifecycle: Lifecycle):
    """
    Test lifecycle caching with custom cache key when injection values change.

    The order of the decorators for caching and dependency injection is important. When the cache
    decorator is the outermost decorator, the cache key generation happens before dependency
    injection. If a dependency is based on a value that changes, the cache will not reflect that
    change unless the cache key generation accounts for it.

    Given: A Lifecycle with application scope.
    When: A dependency is injected at application scope with different argument values and a
          custom cache key that includes the context value.
    Then: The dependency should be cached based on the custom cache key.
    """
    # Arrange
    mock = Mock()

    @lifecycle.cache("application", cache_key=lambda: mock.call_count)
    @inject
    def get_value(x: Injected[int, Depends(lambda: mock.call_count)]) -> int:
        mock()
        return x + 1

    # Act / Assert
    with lifecycle.start("application"):
        assert get_value() == 1
        assert get_value() == 2


def test_lifecycle_inner_cache_inject(lifecycle: Lifecycle):
    """
    Test lifecycle caching when cache is the inner decorator and injected values change.

    The order of the decorators for caching and dependency injection is important. When the cache
    decorator is the innermost decorator, the cache key generation happens after dependency
    injection. If a dependency returns a value that changes, the cache will reflect that
    change.

    Given: A function with a cache decorator inside of dependency injection.
    When: A dependency is injected with different argument values.
    Then: The cached value should reflect the context changes.
    """
    # Arrange
    x = Context[int]("x")
    counter = Mock()

    @inject
    @lifecycle.cache("application")
    def get_value(x: Injected[int, Depends(x)]) -> int:
        counter()
        return x * 2

    # Act / Assert
    with lifecycle.start("application"):
        with x.use(10):
            assert get_value() == 20
            assert get_value() == 20
            assert counter.call_count == 1
        with x.use(20):
            assert get_value() == 40
            assert counter.call_count == 2
