"""
Middleware around a call: functions taking the call and `next`, composed into a chain.

A middleware wraps a call, as web frameworks' middleware wraps a request. It's a function
taking the call and `next`, the rest of the chain: the next middleware, or else the call itself.
It usually passes the call on and returns what comes back: `def middleware(call, next): return
next(call)`. A sync owner runs sync functions. An async owner runs `async def`s, which await
`next`: `async def middleware(call, next): return await next(call)`. So a timeout is `async
with asyncio.timeout(5): return await next(call)`. An owner refuses a middleware of the wrong
kind with {py:exc}`MiddlewareKindError` when it's built, rather than on each call.

A middleware decides what happens to the call. It can raise before `next` to refuse it, return
without calling `next` to skip it, call `next` again to retry, return a different reply, or catch
the call's error and return in its place. What a call is, and what each of those means for it,
is the owner's to say. {py:mod}`stratae.events.middleware` says it for a bus's emits and handler
calls.

{py:func}`chain` composes middleware around the innermost call once, when the owner is built.
Each call then runs the chain with the call's value, and with what the innermost call needs for
it, which no middleware sees.

A scope's activation isn't a call: its body is a `with` block, not a function `next` could run.
So a scope's middleware is a generator function instead, as
{py:mod}`stratae.lifecycle.scope` describes.
"""

from collections.abc import Callable

from stratae.callables import Gives, call_result

type Next[Call, Result] = Callable[[Call], Result]
"""The rest of the chain, as a middleware is given it: the next middleware, or else the call."""

type Finish[Call, Run, Result] = Callable[[Call, Run], Result]
"""
The innermost call of a chain: given the call value the last middleware passed on, and what
the chain was run with for this call, such as an emit's message and how to send it.
"""

type Layer[Call, Result] = Callable[[Call, Next[Call, Result]], Result]
"""One middleware: given the call and `next`, returning what the chain returns."""


class MiddlewareKindError(TypeError):
    """Raised when an owner is given middleware of a kind it can't run."""

    def __init__(self, owner: str, middleware: object, wanted: str) -> None:
        """
        Name the owner, the middleware, and the kind it takes.

        :param owner: What runs the middleware, such as a bus or a scope.
        :param middleware: The middleware.
        :param wanted: The kind of middleware it takes, such as "an async def".
        """
        super().__init__(f"{owner} middleware must be {wanted}, not {middleware!r}")


def refuse_kind(
    middleware: tuple[Callable[..., object], ...], owner: str, asynchronous: bool
) -> None:
    """
    Refuse a middleware `owner` can't run.

    An async owner takes only an `async def`, and a sync owner only a function returning a value.
    So a generator function, or a `contextlib` context manager, is refused by either.

    :param middleware: The middleware.
    :param owner: What runs it, for the error message.
    :param asynchronous: Whether `owner` runs middleware asynchronously.
    :raises MiddlewareKindError: If a middleware is of the wrong kind.
    """
    wanted = Gives.COROUTINE if asynchronous else Gives.VALUE
    for item in middleware:
        if call_result(item).gives is not wanted:
            kind = "an async def" if asynchronous else "a sync function returning a value"
            raise MiddlewareKindError(owner, item, kind)


def chain[Call, Run, Result](
    middleware: tuple[Layer[Call, Result], ...], finish: Finish[Call, Run, Result]
) -> Finish[Call, Run, Result]:
    """
    Compose middleware around `finish`, outermost first, sync or async alike.

    The chain is built once, and run per call with that call's value and context.

    :param middleware: The middleware, outermost first.
    :param finish: The innermost call.
    :returns: The composed chain, taking the call value and the context to finish it with.
    """
    inner = finish
    for item in reversed(middleware):
        inner = _step(item, inner)
    return inner


def _step[Call, Run, Result](
    item: Layer[Call, Result], inner: Finish[Call, Run, Result]
) -> Finish[Call, Run, Result]:
    """Return `inner` with `item` run around it."""
    return lambda call, context: item(call, lambda passed: inner(passed, context))
