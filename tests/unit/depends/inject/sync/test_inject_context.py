"""Test inject with context managers."""

from contextlib import AbstractContextManager, contextmanager
from unittest.mock import Mock

from stratae.depends import Depends, inject


def test_inject_contextmanager():
    """
    Test injection on a function that is a context manager.

    Given: a function that is decorated as a context manager
    When: the function is injected with dependencies
    Then: the injection should work correctly within the context manager.
    """
    # Arrange
    mock_cleanup = Mock()

    # Act (the decorator)
    @inject
    @contextmanager
    def cm_func(dep: int = Depends(lambda: 42)):
        """Context manager function that uses dependency injection."""
        yield dep
        mock_cleanup()

    with cm_func() as value:
        # Assert
        assert value == 42
        mock_cleanup.assert_not_called()
    mock_cleanup.assert_called_once()


def test_inject_contextmanager_dep():
    """
    Test injection of a context manager as a dependency.

    Given: a context manager function used as a dependency
    When: another function is injected with this context manager dependency
    Then: the injection should work correctly and the context manager should be used.
    """
    # Arrange
    mock_cleanup = Mock()

    @contextmanager
    def cm_dep():
        """Context manager dependency."""
        yield 42
        mock_cleanup()

    @inject
    def func_with_cm(cm: AbstractContextManager[int] = Depends(cm_dep)):
        """Test function that uses the context manager dependency."""
        with cm_dep() as value:
            return value

    # Act
    result = func_with_cm()

    # Assert
    assert result == 42
    mock_cleanup.assert_called_once()
