"""
Unit tests for bus middleware and the runners adapters use for it.

This test suite verifies the following behaviors:

Middleware:
- Lists given for either slot are frozen as tuples.
- A lone middleware, or a string, in place of a tuple raises a clear TypeError.
- An entry that isn't callable raises a clear TypeError, for Middleware and Hooks alike.
- require refuses a class that isn't a sync context manager for a sync bus.
- require refuses a class that isn't a context manager of either kind for an async bus, and
  accepts a sync one.
- require leaves callables other than classes to be checked when they're called.
- require can check only one half.

run_middleware:
- Enters context managers outermost first, runs the call once, then exits innermost first,
  each seeing the event and payload, and the reply once the call returned.
- A contextlib.contextmanager middleware sees the reply, and the error.
- Middleware sees the routing config as call.config, and the call type it's asked for.
- An exit returning true, or a contextmanager catching the error, raises MiddlewareError from
  it instead of swallowing it, and outer exits see that; after a successful call, what an exit
  returns is ignored.
- Trying to swallow something that isn't an Exception, such as KeyboardInterrupt, raises it
  again as it is.
- An entry that raises refuses the call; the middleware already entered see that error.
- An exit that raises replaces the reply, or replaces the error, chained from it as in nested
  with statements, and the outer exits see the new error, and no reply.
- An exit that re-raises the call's own error leaves it as is.
- A middleware returning something other than a sync context manager raises TypeError; an
  async def middleware's coroutine is closed.
- A sync context manager with an async __enter__ or __exit__ raises TypeError, closing the
  coroutine, whether the call succeeded or failed.

run_middleware_async:
- Enters async context managers outermost first around an awaited call, then exits them.
- Enters sync context managers too, mixed with async ones.
- An asynccontextmanager middleware sees the reply, and the error.
- An exit returning true raises MiddlewareError instead of swallowing the error, including a
  timeout's cancellation it converted and swallowed; after a success it's ignored.
- Swallowing the task's real cancellation, or another BaseException, raises it again, so a
  cancelled emit's task still ends cancelled.
- An entry that raises refuses the call, and exits that raise replace the error, chained.
- An exit that re-raises the call's own error leaves it as is; one that raises after a
  successful call fails it, and outer exits see no reply.
- asyncio.timeout works as a middleware: a slow call raises TimeoutError, a fast one replies.
- Cancelling the emit runs every exit with the cancellation, which propagates.
- A middleware returning something that isn't a context manager raises TypeError; an async
  def middleware's coroutine is closed.
- A sync context manager with an async __enter__ or __exit__ raises TypeError, on success or
  failure.
"""

import asyncio
from contextlib import asynccontextmanager, contextmanager
from inspect import CORO_CLOSED, getcoroutinestate
from typing import Any, AsyncGenerator, Coroutine, Generator, cast

import pytest

from stratae.events import HandleCall, Hooks, Middleware, PublishCall, PubSub
from stratae.events.exceptions import MiddlewareError
from stratae.events.middleware import run_middleware, run_middleware_async


class _Order:
    def __init__(self, item: str) -> None:
        self.item = item


_order_placed = PubSub(_Order)


def _logging(log: list[str], label: str) -> Any:
    """Return a sync context manager class logging its entry and exit, with what it sees."""

    class Logging:
        def __init__(self, call: PublishCall) -> None:
            self.call = call

        def __enter__(self) -> None:
            log.append(f"{label} in {self.call.event.name} {self.call.payload.item}")

        def __exit__(self, kind: object, error: object, traceback: object) -> None:
            log.append(f"{label} out {self.call.reply!r} {error!r}")

    return Logging


def _alogging(log: list[str], label: str) -> Any:
    """Return an async context manager class logging its entry and exit, with what it sees."""

    class Logging:
        def __init__(self, call: PublishCall) -> None:
            self.call = call

        async def __aenter__(self) -> None:
            log.append(f"{label} in")

        async def __aexit__(self, kind: object, error: object, traceback: object) -> None:
            log.append(f"{label} out {self.call.reply!r} {error!r}")

    return Logging


class _Stop(BaseException):
    """A BaseException that isn't an Exception, like KeyboardInterrupt, safe to raise in tests."""


class _Suppressing:
    """A context manager whose exit returns True, trying to swallow the error."""

    def __init__(self, call: PublishCall) -> None: ...

    def __enter__(self) -> None: ...

    def __exit__(self, *exc: object) -> bool:
        return True


class _ASuppressing:
    """An async context manager whose exit returns True, trying to swallow the error."""

    def __init__(self, call: PublishCall) -> None: ...

    async def __aenter__(self) -> None: ...

    async def __aexit__(self, *exc: object) -> bool:
        return True


class _Refusing:
    """A context manager that raises on entry, refusing the call."""

    def __init__(self, call: PublishCall) -> None: ...

    def __enter__(self) -> None:
        raise PermissionError("rate limited")

    def __exit__(self, *exc: object) -> None: ...


class _ARefusing:
    """An async context manager that raises on entry, refusing the call."""

    def __init__(self, call: PublishCall) -> None: ...

    async def __aenter__(self) -> None:
        raise PermissionError("rate limited")

    async def __aexit__(self, *exc: object) -> None: ...


def _raising_on_exit(error: BaseException) -> Any:
    """Return a context manager class whose exit raises `error`."""

    class Raising:
        def __init__(self, call: PublishCall) -> None: ...

        def __enter__(self) -> None: ...

        def __exit__(self, *exc: object) -> None:
            raise error

    return Raising


def _araising_on_exit(error: BaseException) -> Any:
    """Return an async context manager class whose exit raises `error`."""

    class Raising:
        def __init__(self, call: PublishCall) -> None: ...

        async def __aenter__(self) -> None: ...

        async def __aexit__(self, *exc: object) -> None:
            raise error

    return Raising


def _run(middleware: tuple[Any, ...], call: Any, *args: Any) -> Any:
    """Run `call(*args)` inside sync publish middleware for an order lamp."""
    return run_middleware(middleware, PublishCall, _order_placed, None, _Order("lamp"), call, *args)


async def _arun(middleware: tuple[Any, ...], call: Any, *args: Any) -> Any:
    """Await `call(*args)` inside async publish middleware for an order lamp."""
    return await run_middleware_async(
        middleware, PublishCall, _order_placed, None, _Order("lamp"), call, *args
    )


def _fail() -> None:
    raise LookupError("out of stock")


async def _afail() -> None:
    raise LookupError("out of stock")


async def _reply() -> str:
    return "sent"


# region: Middleware


def test_middleware_freezes_lists() -> None:
    """
    Middleware given as lists should be stored as tuples.

    Given: Publish and handle middleware passed as lists
    When: Middleware is built
    Then: Both slots are tuples of the same middleware
    """
    # Arrange
    publish = _logging([], "publish")
    handle = _logging([], "handle")

    # Act
    middleware = Middleware(publish=cast(Any, [publish]), handle=cast(Any, [handle]))

    # Assert
    assert middleware.publish == (publish,)
    assert middleware.handle == (handle,)


@pytest.mark.parametrize(
    "given",
    [pytest.param(_logging([], "trace"), id="lone-middleware"), pytest.param("trace", id="str")],
)
def test_lone_middleware_raises_clear_error(given: object) -> None:
    """
    A single middleware, or a string, given where a tuple belongs should raise a TypeError.

    Given: One middleware class, or a string, passed as publish= instead of a tuple
    When: Middleware is built
    Then: A TypeError says publish= takes a tuple of middleware
    """
    # Act / Assert
    with pytest.raises(TypeError, match="publish= takes a tuple of middleware"):
        Middleware(publish=cast(Any, given))


def test_uncallable_middleware_raises_clear_error() -> None:
    """
    An entry that isn't callable should raise a clear TypeError when Middleware is built.

    Given: A tuple holding a middleware and None
    When: Middleware is built with it as handle=
    Then: A TypeError names the entry that isn't callable
    """
    # Act / Assert
    with pytest.raises(TypeError, match="handle= takes a tuple of middleware, but holds None"):
        Middleware(handle=cast(Any, (_logging([], "trace"), None)))


def test_uncallable_hook_raises_clear_error() -> None:
    """
    Hooks share the check: an entry that isn't callable should raise a clear TypeError.

    Given: A string inside the before= tuple
    When: Hooks is built
    Then: A TypeError names the entry that isn't callable
    """
    # Act / Assert
    with pytest.raises(TypeError, match="before= takes a tuple of hooks, but holds 'audit'"):
        Hooks(before=cast(Any, ("audit",)))


def test_require_refuses_async_class_for_sync_bus() -> None:
    """
    ``require`` should refuse a class that isn't a sync context manager for a sync bus.

    Given: Handle middleware that's an async context manager class
    When: require is called for a sync bus
    Then: A TypeError names the bus and the middleware
    """
    # Arrange
    middleware = Middleware(handle=(_alogging([], "trace"),))

    # Act / Assert
    with pytest.raises(TypeError, match="DirectBus enters middleware synchronously"):
        middleware.require("DirectBus", asynchronous=False)


def test_require_refuses_non_manager_class_for_async_bus() -> None:
    """
    ``require`` should refuse a class that isn't a context manager of either kind.

    Given: Publish middleware that's a plain class
    When: require is called for an async bus
    Then: A TypeError names the bus and says it isn't a context manager
    """

    # Arrange
    class Plain:
        def __init__(self, call: PublishCall) -> None: ...

    middleware = Middleware(publish=(cast(Any, Plain),))

    # Act / Assert
    with pytest.raises(TypeError, match="AsyncDirectBus can't enter .*isn't a context manager"):
        middleware.require("AsyncDirectBus", asynchronous=True)


def test_require_accepts_sync_class_for_async_bus() -> None:
    """
    ``require`` should accept a sync context manager class for an async bus.

    Given: Publish middleware that's a sync context manager class
    When: require is called for an async bus
    Then: It doesn't raise
    """
    # Arrange
    middleware = Middleware(publish=(_logging([], "trace"),))

    # Act / Assert
    middleware.require("AsyncDirectBus", asynchronous=True)


def test_require_leaves_functions_to_the_call() -> None:
    """
    ``require`` should leave a middleware that isn't a class to be checked when it's called.

    Given: Publish middleware that's a plain function returning None
    When: require is called for a sync bus
    Then: It doesn't raise; only running it can show what it returns
    """

    # Arrange
    def trace(call: PublishCall) -> Any: ...

    middleware = Middleware(publish=(trace,))

    # Act / Assert
    middleware.require("DirectBus", asynchronous=False)


def test_require_checks_only_selected_half() -> None:
    """
    ``require`` should check only the halves asked for.

    Given: Sync publish middleware and async handle middleware classes
    When: require checks only publish for a sync bus
    Then: It doesn't raise
    """
    # Arrange
    middleware = Middleware(publish=(_logging([], "p"),), handle=(_alogging([], "h"),))

    # Act / Assert
    middleware.require("DirectBus", asynchronous=False, handle=False)


# endregion

# region: run_middleware


def test_middleware_wraps_call_outermost_first() -> None:
    """
    Middleware should be entered outermost first and exited innermost first, around one call.

    Given: Two logging context managers around a call that logs and returns a reply
    When: run_middleware runs it
    Then: Both enter in order seeing the event and payload, the call runs once, and both exit
        in reverse, seeing the reply and no error
    """
    # Arrange
    log: list[str] = []

    def call(label: str) -> str:
        log.append(label)
        return "sent"

    middleware = (_logging(log, "outer"), _logging(log, "inner"))

    # Act
    reply = _run(middleware, call, "call")

    # Assert
    assert reply == "sent"
    assert log == [
        "outer in _Order lamp",
        "inner in _Order lamp",
        "call",
        "inner out 'sent' None",
        "outer out 'sent' None",
    ]


def test_contextmanager_middleware_sees_reply_and_error() -> None:
    """
    A contextlib.contextmanager middleware should see the reply, or the error it re-raises.

    Given: A contextmanager middleware recording call.reply before and after the call, or the
        error
    When: run_middleware runs a call that replies, then one that raises
    Then: It saw no reply before, the reply after, then the error, which still propagates
    """
    # Arrange
    seen: list[object] = []

    @contextmanager
    def record(call: PublishCall) -> Generator[None, None, None]:
        seen.append(call.reply)
        try:
            yield
        except LookupError as error:
            seen.append(error)
            raise
        seen.append(call.reply)

    # Act
    _run((record,), lambda: "sent")
    with pytest.raises(LookupError) as raised:
        _run((record,), _fail)

    # Assert
    assert seen == [None, "sent", None, raised.value]


def test_middleware_sees_config_and_call_type() -> None:
    """
    Middleware should see the routing config as call.config, and the call type asked for.

    Given: A contextmanager middleware recording call.config and the call's type
    When: run_middleware runs with a config and HandleCall
    Then: The middleware saw that config, in a HandleCall
    """
    # Arrange
    seen: list[object] = []

    @contextmanager
    def record(call: HandleCall) -> Generator[None, None, None]:
        seen.extend((call.config, type(call)))
        yield

    # Act
    run_middleware((record,), HandleCall, _order_placed, "orders", _Order("lamp"), lambda: None)

    # Assert
    assert seen == ["orders", HandleCall]


@pytest.mark.parametrize("swallow", ["exit-returns-true", "contextmanager"])
def test_exit_cannot_swallow_error(swallow: str) -> None:
    """
    An exit that tries to swallow the error should raise MiddlewareError from it instead.

    Given: A logging middleware outside one that swallows: an exit returning True, or a
        contextmanager catching the error without re-raising
    When: run_middleware runs a call that raises
    Then: MiddlewareError is raised from the call's error, and the outer exit saw it
    """
    # Arrange
    log: list[str] = []

    @contextmanager
    def catch(call: PublishCall) -> Generator[None, None, None]:
        try:
            yield
        except LookupError:
            pass

    inner = catch if swallow == "contextmanager" else _Suppressing

    # Act
    with pytest.raises(MiddlewareError, match="tried to swallow LookupError") as raised:
        _run((_logging(log, "outer"), inner), _fail)

    # Assert
    assert isinstance(raised.value.__cause__, LookupError)
    assert log[-1].startswith("outer out None MiddlewareError(")


@pytest.mark.parametrize("error", [KeyboardInterrupt, _Stop], ids=["KeyboardInterrupt", "other"])
def test_swallowed_base_exception_raised_again(error: type[BaseException]) -> None:
    """
    An exit swallowing something that isn't an Exception should have it raised again, as is.

    Given: A logging middleware outside one whose exit returns True
    When: run_middleware runs a call raising KeyboardInterrupt, or another BaseException
    Then: That same error propagates, not MiddlewareError, and the outer exit saw it
    """
    # Arrange
    log: list[str] = []

    def stop() -> None:
        raise error

    # Act
    with pytest.raises(error):
        _run((_logging(log, "outer"), _Suppressing), stop)

    # Assert
    assert log[-1] == f"outer out None {error.__name__}()"


def test_exit_returning_true_after_success_is_ignored() -> None:
    """
    What an exit returns after a successful call should be ignored, as a with statement does.

    Given: A middleware whose exit returns True
    When: run_middleware runs a call that replies
    Then: The reply is returned
    """
    # Act / Assert
    assert _run((_Suppressing,), lambda: "sent") == "sent"


def test_raising_entry_refuses_call() -> None:
    """
    A middleware raising on entry should refuse the call; those already entered see the error.

    Given: A logging middleware, then one that raises on entry, then another logging one
    When: run_middleware runs a call that records it ran
    Then: The error propagates, the call never ran, the last middleware was never entered,
        and the first one's exit saw the error
    """
    # Arrange
    log: list[str] = []
    calls: list[str] = []
    middleware = (_logging(log, "outer"), _Refusing, _logging(log, "inner"))

    # Act
    with pytest.raises(PermissionError, match="rate limited"):
        _run(middleware, calls.append, "call")

    # Assert
    assert calls == []
    assert log == ["outer in _Order lamp", "outer out None PermissionError('rate limited')"]


def test_raising_exit_replaces_reply() -> None:
    """
    A middleware raising on exit after a successful call should fail it, as a with would.

    Given: A logging middleware outside one whose exit raises
    When: run_middleware runs a call that replies
    Then: The exit's error propagates, unchained, and the outer exit saw it, with no reply
    """
    # Arrange
    log: list[str] = []
    error = ValueError("reply rejected")

    # Act
    with pytest.raises(ValueError, match="reply rejected") as raised:
        _run((_logging(log, "outer"), _raising_on_exit(error)), lambda: "sent")

    # Assert
    assert raised.value.__context__ is None
    assert log == ["outer in _Order lamp", "outer out None ValueError('reply rejected')"]


def test_raising_exits_replace_error_chained() -> None:
    """
    Exits raising new errors should each replace the error, chained, as nested with would.

    Given: A logging middleware outside two whose exits raise new errors
    When: run_middleware runs a call that raises
    Then: The outer new error propagates, chained from the inner new error, which is chained
        from the call's error, and the logging exit saw the outer new one
    """
    # Arrange
    log: list[str] = []
    middle = ValueError("middle")
    inner = KeyError("inner")
    middleware = (_logging(log, "outer"), _raising_on_exit(middle), _raising_on_exit(inner))

    # Act
    with pytest.raises(ValueError, match="middle") as raised:
        _run(middleware, _fail)

    # Assert
    assert raised.value.__context__ is inner
    assert isinstance(inner.__context__, LookupError)
    assert log[-1] == "outer out None ValueError('middle')"


def test_reraised_error_is_left_as_is() -> None:
    """
    An exit re-raising the call's own error should leave that error as is.

    Given: A logging middleware outside one whose exit re-raises the error it's given
    When: run_middleware runs a call that raises
    Then: That same error propagates, unchained, and the outer exit saw it
    """
    # Arrange
    log: list[str] = []

    class Reraise:
        def __init__(self, call: PublishCall) -> None: ...

        def __enter__(self) -> None: ...

        def __exit__(self, kind: object, error: BaseException, traceback: object) -> None:
            raise error

    # Act
    with pytest.raises(LookupError, match="out of stock") as raised:
        _run((_logging(log, "outer"), Reraise), _fail)

    # Assert
    assert raised.value.__context__ is None
    assert log[-1] == "outer out None LookupError('out of stock')"


def test_non_manager_middleware_raises() -> None:
    """
    A middleware returning something that isn't a sync context manager should raise TypeError.

    Given: A logging middleware outside one returning an async context manager
    When: run_middleware runs a call that records it ran
    Then: A TypeError says a sync bus can't enter it, the call never ran, and the outer exit
        saw the TypeError
    """
    # Arrange
    log: list[str] = []
    calls: list[str] = []

    # Act
    with pytest.raises(TypeError, match="a sync bus can't enter"):
        _run((_logging(log, "outer"), _alogging(log, "async")), calls.append, "call")

    # Assert
    assert calls == []
    assert log[-1].startswith("outer out None TypeError(")


def test_async_def_middleware_is_closed_on_sync_path() -> None:
    """
    An async def middleware should be refused on the sync path, its coroutine closed.

    Given: A middleware that returns a coroutine, as an async def middleware would
    When: run_middleware runs a call
    Then: A TypeError is raised, and the coroutine is closed rather than left unawaited
    """
    # Arrange
    coroutines: list[Coroutine[Any, Any, None]] = []

    async def body() -> None: ...

    def async_def(call: PublishCall) -> Any:
        coroutines.append(body())
        return coroutines[0]

    # Act
    with pytest.raises(TypeError, match="isn't a sync context manager"):
        _run((async_def,), lambda: None)

    # Assert
    assert getcoroutinestate(coroutines[0]) == CORO_CLOSED


@pytest.mark.parametrize(
    ("method", "call"),
    [("__enter__", lambda: "sent"), ("__exit__", lambda: "sent"), ("__exit__", _fail)],
    ids=["enter", "exit", "exit-unwinding"],
)
def test_async_enter_or_exit_raises_on_sync_path(method: str, call: Any) -> None:
    """
    A sync context manager with an async __enter__ or __exit__ should raise TypeError.

    Given: A class whose __enter__, or __exit__, is an async def
    When: run_middleware runs a call that replies, or for __exit__ also one that raises
    Then: A TypeError says the method is async, and its coroutine is closed
    """
    # Arrange
    coroutines: list[Coroutine[Any, Any, None]] = []

    async def body() -> None: ...

    def made(name: str) -> Any:
        if name == method:
            coroutines.append(body())
            return coroutines[0]
        return None

    class Mistaken:
        def __init__(self, call: PublishCall) -> None: ...

        def __enter__(self) -> Any:
            return made("__enter__")

        def __exit__(self, *exc: object) -> Any:
            return made("__exit__")

    # Act
    with pytest.raises(TypeError, match="async __enter__ or __exit__"):
        _run((Mistaken,), call)

    # Assert
    assert getcoroutinestate(coroutines[0]) == CORO_CLOSED


# endregion

# region: run_middleware_async


async def test_async_middleware_wraps_call_outermost_first() -> None:
    """
    Async middleware should be entered outermost first and exited innermost first.

    Given: Two async logging context managers around an awaited call
    When: run_middleware_async runs it
    Then: Both enter in order, and exit in reverse seeing the reply
    """
    # Arrange
    log: list[str] = []

    async def call() -> str:
        log.append("call")
        return "sent"

    # Act
    reply = await _arun((_alogging(log, "outer"), _alogging(log, "inner")), call)

    # Assert
    assert reply == "sent"
    assert log == ["outer in", "inner in", "call", "inner out 'sent' None", "outer out 'sent' None"]


async def test_async_bus_enters_sync_managers_too() -> None:
    """
    An async bus should enter sync context managers alongside async ones.

    Given: A sync logging middleware outside an async one, outside another sync one
    When: run_middleware_async runs a call that raises
    Then: All three enter in order, and exit in reverse seeing the error
    """
    # Arrange
    log: list[str] = []
    middleware = (_logging(log, "outer"), _alogging(log, "middle"), _logging(log, "inner"))

    # Act
    with pytest.raises(LookupError):
        await _arun(middleware, _afail)

    # Assert
    error = "LookupError('out of stock')"
    assert log == [
        "outer in _Order lamp",
        "middle in",
        "inner in _Order lamp",
        f"inner out None {error}",
        f"middle out None {error}",
        f"outer out None {error}",
    ]


async def test_async_bus_exits_sync_managers_after_success() -> None:
    """
    An async bus should exit sync and async context managers alike after a successful call.

    Given: A sync logging middleware outside an async one
    When: run_middleware_async runs a call that replies
    Then: Both exit in reverse, seeing the reply
    """
    # Arrange
    log: list[str] = []

    # Act
    await _arun((_logging(log, "outer"), _alogging(log, "inner")), _reply)

    # Assert
    assert log[-2:] == ["inner out 'sent' None", "outer out 'sent' None"]


async def test_asynccontextmanager_middleware_sees_reply_and_error() -> None:
    """
    An asynccontextmanager middleware should see the reply, or the error it re-raises.

    Given: An asynccontextmanager middleware recording call.reply, or the error
    When: run_middleware_async runs a call that replies, then one that raises
    Then: It saw the reply, then the error, which still propagates
    """
    # Arrange
    seen: list[object] = []

    @asynccontextmanager
    async def record(call: PublishCall) -> AsyncGenerator[None, None]:
        try:
            yield
        except LookupError as error:
            seen.append(error)
            raise
        seen.append(call.reply)

    # Act
    await _arun((record,), _reply)
    with pytest.raises(LookupError) as raised:
        await _arun((record,), _afail)

    # Assert
    assert seen == ["sent", raised.value]


@pytest.mark.parametrize("inner", [_ASuppressing, _Suppressing], ids=["async", "sync"])
async def test_async_exit_cannot_swallow_error(inner: Any) -> None:
    """
    An exit returning True on the async path should raise MiddlewareError from the error.

    Given: An async logging middleware outside one, async or sync, whose exit returns True
    When: run_middleware_async runs a call that raises
    Then: MiddlewareError is raised from the call's error, and the outer exit saw it
    """
    # Arrange
    log: list[str] = []

    # Act
    with pytest.raises(MiddlewareError) as raised:
        await _arun((_alogging(log, "outer"), inner), _afail)

    # Assert
    assert isinstance(raised.value.__cause__, LookupError)
    assert log[-1].startswith("outer out None MiddlewareError(")


async def test_swallowed_real_cancellation_raised_again() -> None:
    """
    An exit swallowing the task's real cancellation should have it raised again.

    Given: An async middleware whose exit returns True, around a call that never finishes
    When: The task running it is cancelled
    Then: The cancellation propagates, and the task ends cancelled
    """
    # Arrange
    started = asyncio.Event()

    async def wait() -> None:
        started.set()
        await asyncio.Event().wait()

    task = asyncio.ensure_future(_arun((_ASuppressing,), wait))
    await started.wait()

    # Act
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Assert
    assert task.cancelled()


async def test_async_swallowed_base_exception_raised_again() -> None:
    """
    On the async path, an exit swallowing another BaseException should have it raised again.

    Given: An async middleware whose exit returns True
    When: run_middleware_async runs a call raising a BaseException that isn't an Exception
    Then: That same error propagates, not MiddlewareError
    """

    # Arrange
    async def stop() -> None:
        raise _Stop

    # Act / Assert
    with pytest.raises(_Stop):
        await _arun((_ASuppressing,), stop)


async def test_async_failed_exit_clears_reply() -> None:
    """
    On the async path, an exit failing a successful call should leave outer exits no reply.

    Given: An async logging middleware outside one whose exit raises
    When: run_middleware_async runs a call that replies
    Then: The exit's error propagates, and the outer exit saw it, with no reply
    """
    # Arrange
    log: list[str] = []

    # Act
    with pytest.raises(ValueError):
        await _arun((_alogging(log, "outer"), _araising_on_exit(ValueError("no"))), _reply)

    # Assert
    assert log == ["outer in", "outer out None ValueError('no')"]


async def test_async_exit_returning_true_after_success_is_ignored() -> None:
    """
    What an async exit returns after a successful call should be ignored.

    Given: An async middleware whose exit returns True
    When: run_middleware_async runs a call that replies
    Then: The reply is returned
    """
    # Act / Assert
    assert await _arun((_ASuppressing,), _reply) == "sent"


async def test_swallowed_timeout_raises_middleware_error() -> None:
    """
    A middleware swallowing its own timeout should fail the call, not look like a cancellation.

    Given: An asynccontextmanager middleware that times out the call and swallows TimeoutError
    When: run_middleware_async runs a call that never finishes
    Then: MiddlewareError is raised, an ordinary exception, from the timeout's cancellation
    """

    # Arrange
    @asynccontextmanager
    async def time_out(call: PublishCall) -> AsyncGenerator[None, None]:
        try:
            async with asyncio.timeout(0.01):
                yield
        except TimeoutError:
            pass

    async def slow() -> None:
        await asyncio.Event().wait()

    # Act
    with pytest.raises(MiddlewareError) as raised:
        await _arun((time_out,), slow)

    # Assert
    assert isinstance(raised.value.__cause__, asyncio.CancelledError)


async def test_async_raising_entry_refuses_call() -> None:
    """
    On the async path, a middleware raising on entry should refuse the call.

    Given: An async logging middleware outside one raising on entry
    When: run_middleware_async runs a call recording it ran
    Then: The entry's error propagates, the call never ran, and the outer exit saw the error
    """
    # Arrange
    log: list[str] = []
    calls: list[str] = []

    async def call() -> None:
        calls.append("call")

    # Act
    with pytest.raises(PermissionError):
        await _arun((_alogging(log, "outer"), _ARefusing), call)

    # Assert
    assert calls == []
    assert log == ["outer in", "outer out None PermissionError('rate limited')"]


async def test_async_raising_exits_replace_error_chained() -> None:
    """
    On the async path, exits raising new errors should each replace the error, chained.

    Given: Two async middleware whose exits raise new errors
    When: run_middleware_async runs a call that raises
    Then: The outer new error propagates, chained from the inner one, which is chained from
        the call's error
    """
    # Arrange
    outer, inner = ValueError("outer"), KeyError("inner")

    # Act
    with pytest.raises(ValueError, match="outer") as raised:
        await _arun((_araising_on_exit(outer), _araising_on_exit(inner)), _afail)

    # Assert
    assert raised.value.__context__ is inner
    assert isinstance(inner.__context__, LookupError)


async def test_async_reraised_and_reply_replacing_exits() -> None:
    """
    On the async path, a re-raised error should stay as is, and a raising exit fail a success.

    Given: An async middleware re-raising the error it's given, and separately one whose exit
        raises after a successful call
    When: run_middleware_async runs a failing call with the first, and a replying one with the
        second
    Then: The call's own error propagates unchained, then the exit's error, also unchained
    """

    # Arrange
    class Reraise:
        def __init__(self, call: PublishCall) -> None: ...

        async def __aenter__(self) -> None: ...

        async def __aexit__(self, kind: object, error: BaseException, traceback: object) -> None:
            raise error

    # Act
    with pytest.raises(LookupError) as reraised:
        await _arun((Reraise,), _afail)
    with pytest.raises(ValueError) as replaced:
        await _arun((_araising_on_exit(ValueError("rejected")),), _reply)

    # Assert
    assert reraised.value.__context__ is None
    assert replaced.value.__context__ is None


async def test_timeout_middleware() -> None:
    """
    asyncio.timeout should work as a middleware, timing out a slow call only.

    Given: A middleware returning asyncio.timeout(0.01)
    When: run_middleware_async runs a call that never finishes, then one that replies
    Then: The slow call raises TimeoutError, and the fast one replies
    """

    # Arrange
    def deadline(call: PublishCall) -> asyncio.Timeout:
        return asyncio.timeout(0.01)

    async def slow() -> None:
        await asyncio.Event().wait()

    # Act
    with pytest.raises(TimeoutError):
        await _arun((deadline,), slow)
    reply = await _arun((deadline,), _reply)

    # Assert
    assert reply == "sent"


async def test_cancelled_emit_exits_every_middleware() -> None:
    """
    Cancelling the emit should exit every middleware with the cancellation, which propagates.

    Given: Sync and async logging middleware around a call that never finishes
    When: The task running it is cancelled
    Then: The cancellation propagates, and both exits saw a CancelledError
    """
    # Arrange
    log: list[str] = []
    started = asyncio.Event()

    async def wait() -> None:
        started.set()
        await asyncio.Event().wait()

    task = asyncio.ensure_future(_arun((_logging(log, "outer"), _alogging(log, "inner")), wait))
    await started.wait()

    # Act
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Assert
    assert log[-2:] == ["inner out None CancelledError()", "outer out None CancelledError()"]


async def test_non_manager_middleware_raises_async() -> None:
    """
    A middleware returning something that isn't a context manager should raise TypeError.

    Given: A middleware returning None, and an async def middleware returning a coroutine
    When: run_middleware_async runs a call with each
    Then: Both raise TypeError, and the coroutine is closed rather than left unawaited
    """
    # Arrange
    coroutines: list[Coroutine[Any, Any, None]] = []

    async def body() -> None: ...

    def nothing(call: PublishCall) -> Any:
        return None

    def async_def(call: PublishCall) -> Any:
        coroutines.append(body())
        return coroutines[0]

    # Act
    with pytest.raises(TypeError, match="returned None, which isn't a context manager"):
        await _arun((nothing,), _reply)
    with pytest.raises(TypeError, match="isn't a context manager"):
        await _arun((async_def,), _reply)

    # Assert
    assert getcoroutinestate(coroutines[0]) == CORO_CLOSED


@pytest.mark.parametrize(
    ("method", "call"),
    [("__enter__", _reply), ("__exit__", _reply), ("__exit__", _afail)],
    ids=["enter", "exit", "exit-unwinding"],
)
async def test_async_methods_on_sync_manager_raise_on_async_path(method: str, call: Any) -> None:
    """
    A sync context manager with an async __enter__ or __exit__ should raise on an async bus.

    Given: A class with only __enter__ and __exit__, one of them an async def
    When: run_middleware_async runs a call that replies, or for __exit__ also one that raises
    Then: A TypeError says the method is async, and its coroutine is closed
    """
    # Arrange
    coroutines: list[Coroutine[Any, Any, None]] = []

    async def body() -> None: ...

    def made(name: str) -> Any:
        if name == method:
            coroutines.append(body())
            return coroutines[0]
        return None

    class Mistaken:
        def __init__(self, call: PublishCall) -> None: ...

        def __enter__(self) -> Any:
            return made("__enter__")

        def __exit__(self, *exc: object) -> Any:
            return made("__exit__")

    # Act
    with pytest.raises(TypeError, match="async __enter__ or __exit__"):
        await _arun((Mistaken,), call)

    # Assert
    assert getcoroutinestate(coroutines[0]) == CORO_CLOSED


# endregion
