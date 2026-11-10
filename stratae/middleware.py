"""
The middleware contract shared by lifecycle scopes and event buses.

A middleware is a callable that takes what it wraps and returns a context manager. A
{py:class}`Scope <stratae.lifecycle.scope.Scope>` passes itself, around each activation. A bus
passes a {py:class}`PublishCall <stratae.events.middleware.PublishCall>` around each emit, or a
{py:class}`HandleCall <stratae.events.middleware.HandleCall>` around each handler call. Middleware
is given as a tuple, outermost first. Scopes and buses follow the same rules for it.

- Each context manager is entered in order and exited in reverse, like nested `with`
  statements. An exit sees the error the wrapped work ended with, or `None`.
- A middleware can't swallow an error. An exit that returns true for an ordinary error raises
  {py:exc}`MiddlewareError` from it instead, so the work still fails. Anything else, such as
  `KeyboardInterrupt` or the task's cancellation, is raised again as it is. A cancellation the
  middleware made itself, by timing the work out, isn't the task's, so swallowing the timeout
  raises `MiddlewareError` too.
- A middleware that raises on entry refuses the work. The middleware already entered see that
  error on exit.
- A sync scope or bus enters sync context managers. An async one enters async context managers
  as well, in the task doing the work.
- A sync context manager whose `__enter__` or `__exit__` is an `async def` is refused, and its
  coroutine closed.
- A middleware that's a class is checked when the scope or bus is built. Any other callable is
  checked each time it's called.

Code that should decide whether an error propagates isn't middleware. On a scope, it's a
resource: a cached generator function, which can suppress the activation's error.

Where an exit's own error goes is the one thing that differs. On a bus, it replaces the call's
reply or error, as in nested `with` statements. On a scope, middleware is held as the
activation's first resources, so it follows the rule for a resource's cleanup: alone after a
clean activation, or grouped with the activation's error.
"""


class MiddlewareError(Exception):
    """
    Raised when a middleware tries to swallow an error.

    Raised from that error, which middleware can observe on exit, but not suppress.
    """

    def __init__(self, middleware: object, error: BaseException) -> None:
        """
        Name the middleware and the error it tried to swallow.

        :param middleware: The middleware whose exit returned true.
        :param error: The error it tried to swallow.
        """
        super().__init__(
            f"middleware {middleware!r} tried to swallow {error!r}, but middleware can't "
            "swallow errors"
        )


class MiddlewareNotEnterableError(TypeError):
    """Raised when a middleware is a class whose instances a scope or bus can't enter."""

    def __init__(self, owner: str, middleware: type, asynchronous: bool) -> None:
        """
        Name the scope or bus and the class.

        :param owner: The scope or bus.
        :param middleware: The class.
        :param asynchronous: Whether the scope or bus enters async context managers too.
        """
        kind = "context manager" if asynchronous else "sync context manager"
        super().__init__(f"{owner} can't enter {middleware!r}: it isn't a {kind}")


class MiddlewareResultError(TypeError):
    """Raised when a middleware returns something a scope or bus can't enter."""

    def __init__(self, middleware: object, result: object, asynchronous: bool) -> None:
        """
        Name the middleware and what it returned.

        :param middleware: The middleware.
        :param result: What it returned.
        :param asynchronous: Whether the scope or bus enters async context managers too.
        """
        kind = "context manager" if asynchronous else "sync context manager"
        super().__init__(f"middleware {middleware!r} returned {result!r}, which isn't a {kind}")


class MiddlewareAsyncMethodError(TypeError):
    """Raised when a sync context manager's `__enter__` or `__exit__` is an `async def`."""

    def __init__(self, middleware: object) -> None:
        """:param middleware: The middleware that returned the context manager."""
        super().__init__(
            f"middleware {middleware!r} has an async __enter__ or __exit__; an async context "
            "manager defines __aenter__ and __aexit__"
        )
