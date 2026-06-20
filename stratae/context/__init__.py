"""
Context variables for sharing values across a call stack without threading parameters.

`Context` wraps a `contextvars.ContextVar`: set a value once with `.set()`
or `with ctx.use(value):`, and any code running underneath that point
can read it back by calling the `Context` instance directly. Because
`Context` instances are callable, they work as `Depends()` providers,
letting runtime values (a request's user ID, a feature flag, a connection)
flow into injected functions without changing their signatures.

Example:
    from stratae.context import Context

    user_id = Context[int]("user_id")

    with user_id.use(123):
        assert user_id.get() == 123
        with user_id.use(42):
            assert user_id() == 42
        assert user_id() == 123

"""

from .context import IGNORE, Context

__all__ = ["IGNORE", "Context"]
