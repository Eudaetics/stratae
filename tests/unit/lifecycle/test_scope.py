"""Validation of Scope construction."""

from stratae.cache import MemoryCache, ThreadSafeMemoryCache
from stratae.lifecycle import Scope


def test_scope_init():
    """Scope constructor should work with positional args and no cache provided."""
    scope = Scope("request", "none")

    assert scope.name == "request"
    assert scope.isolation == "none"
    assert isinstance(scope.cache, MemoryCache)


def test_scope_init_custom_cache():
    """Passing a custom cache should store that same cache."""
    cache = ThreadSafeMemoryCache()
    scope = Scope("request", "none", cache)

    assert scope.name == "request"
    assert scope.isolation == "none"
    assert scope.cache is cache


def test_scope_keyword_init():
    """Fields should be assignable with keyword arguments."""
    cache = MemoryCache()
    scope = Scope(name="request", isolation="none", cache=cache)

    assert scope.name == "request"
    assert scope.isolation == "none"
    assert scope.cache is cache
