"""Caching tools for managing single or multiple caches."""

from stratae.cache.cache import Cache
from stratae.cache.memory import MemoryCache
from stratae.cache.util import get_function_key

__all__ = ["Cache", "MemoryCache", "get_function_key"]
