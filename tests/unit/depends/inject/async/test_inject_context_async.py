"""Test inject with context managers."""

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from unittest.mock import Mock

from stratae.depends import Depends, inject


async def test_inject_contextmanager_async():
    """
    Test injection on a function that is an async context manager.

    Given: a function that is decorated as an async context manager
    When: the function is injected with dependencies
    Then: the injection should work correctly within the context manager.
    """
    # Arrange
    mock_cleanup = Mock()

    # Act (the decorator)
    @inject
    @asynccontextmanager
    async def cm_func(dep: int = Depends(lambda: 42)):
        """Async context manager function that uses dependency injection."""
        yield dep
        mock_cleanup()

    async with cm_func() as value:
        # Assert
        assert value == 42
        mock_cleanup.assert_not_called()
    mock_cleanup.assert_called_once()


async def test_inject_contextmanager_dep_async():
    """
    Test injection of an async context manager as a dependency.

    Given: a context manager function used as a dependency
    When: another function is injected with this context manager dependency
    Then: the injection should work correctly and the context manager should be used.
    """
    # Arrange
    mock_cleanup = Mock()

    @asynccontextmanager
    async def cm_dep():
        """Async context manager dependency."""
        yield 42
        mock_cleanup()

    @inject
    async def func_with_cm(cm: AbstractAsyncContextManager[int] = Depends(cm_dep)):
        """Test function that uses the context manager dependency."""
        async with cm_dep() as value:
            return value

    # Act
    result = await func_with_cm()

    # Assert
    assert result == 42
    mock_cleanup.assert_called_once()
