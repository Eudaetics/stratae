"""
Scope activation and per-scope function-result caching.

{py:class}`Scope` and {py:class}`AsyncScope` hold the cached results for one scope.
Activate one with `with scope:` (`async with` for {py:class}`AsyncScope`), or with
{py:meth}`Scope.activate` and {py:meth}`Scope.deactivate` when the two happen in different
callbacks. `@scope.cache()` caches a function's result for the active activation. A
generator function is a resource. Its generator is entered when cached, and its yielded
value is cached in its place. A decorator over a generator function is seen through when it
records what it wraps, as `functools.wraps` does.

`isolation="shared"` uses one cache across all threads and tasks, for application-wide
state. `isolation="context"` (the default) gives each execution context its own cache, for
request-scoped state. `storage="sparse"` allocates slots lazily, for scopes that register
many functions but use few per activation.

A shared scope computes each cached entry once, even under concurrent misses; later callers
wait for it, or recompute if it raised. A call that would wait on its own unfinished
computation raises {py:exc}`CacheCycleError <stratae.lifecycle.exceptions.CacheCycleError>`.
Async cached functions in a context scope get the same treatment across tasks sharing its
activation. A task that outlives the activation gets
{py:exc}`ScopeInactiveError <stratae.lifecycle.exceptions.ScopeInactiveError>`.

A context scope assumes one thread uses an activation at a time. Threads can share one
through a copied context, as `asyncio.to_thread` does; pass `threads=True` to extend the
compute-once guarantee across them. Without it, overlapping threads may each compute an
entry, leaving callers with different values, and awaiting an async cached entry that an
event loop on another thread is computing raises
{py:exc}`LifecycleConfigurationError <stratae.lifecycle.exceptions.LifecycleConfigurationError>`.

A scope can require a parent scope with `requires`; activating it raises unless the parent
is active.

Work that belongs around a whole activation, such as timing it or recording how it ended,
goes in the scope's `middleware`: a tuple of callables, each given the scope and returning a
context manager, as an event bus's middleware is. Each activation enters the context managers
in order once the scope is active, so they can use its cached functions. They're held as the
activation's first resources, so it exits them last, in reverse, after its other resources
have closed. An exit sees the activation's error, including one from a resource's cleanup,
such as a failed commit, but can't suppress it. An exit that tries raises
{py:exc}`MiddlewareError <stratae.middleware.MiddlewareError>` from the error instead. Code
that should decide whether an error propagates is a resource, not middleware. An error an
exit raises follows the rule for a resource's cleanup: alone after a clean activation, or
grouped with the activation's error. One that raises on entry refuses the activation: the
ones already entered are exited with that error, and it propagates. A {py:class}`Scope`
enters sync context managers. An {py:class}`AsyncScope` enters sync or async ones, which is
why its `activate()` is awaited, as its `deactivate()` is. Scopes and event buses follow the
same middleware contract, described in {py:mod}`stratae.middleware`.

````{example} Timing each unit of work, and reporting how it ended
```{code-block} python
from contextlib import contextmanager
from stratae.lifecycle import Scope

clock = iter([10.0, 10.25, 20.0, 20.5])  # stands in for time.monotonic

@contextmanager
def timed(scope: Scope):
    started = next(clock)
    try:
        yield
    except Exception as error:
        print(f"{scope.name} failed after {next(clock) - started:.2f}s: {error}")
        raise
    print(f"{scope.name} took {next(clock) - started:.2f}s")

work = Scope("work", middleware=(timed,))

with work:
    pass

try:
    with work:
        raise LookupError("out of stock")
except LookupError:
    pass
```
```{output}
work took 0.25s
work failed after 0.50s: 'out of stock'
```
````

Work that should only follow a commit belongs to the resource that commits, since only it
knows the commit succeeded. Code after a resource's `yield` runs only when the activation
ends cleanly, so the resource can run what was registered with it there.

````{example} Logging an order only once its unit of work commits
```{code-block} python
from collections.abc import Callable
from stratae.lifecycle import Scope

work = Scope("work")

class UnitOfWork:
    def __init__(self) -> None:
        self.after_commit: list[Callable[[], None]] = []

@work.cache()
def unit_of_work():
    unit = UnitOfWork()
    yield unit
    print("committed")
    for callback in unit.after_commit:
        callback()

def place_order(item: str) -> None:
    unit_of_work().after_commit.append(lambda: print(f"order placed: {item}"))
    if item == "unicorn":
        raise LookupError("out of stock")

with work:
    place_order("lamp")

try:
    with work:
        place_order("unicorn")
except LookupError:
    pass
```
```{output}
committed
order placed: lamp
```
````

````{example} Three scope tiers working together: application, request, and block
```{code-block} python
import asyncio
from itertools import count
from stratae.lifecycle import AsyncScope

application = AsyncScope("application", isolation="shared")
request = AsyncScope("request", storage="sparse", requires=application)
block = AsyncScope("block")

class ConnectionPool:
    def __init__(self):
        print("Opening connection pool")

@application.cache()
async def get_pool() -> ConnectionPool:
    return ConnectionPool()

_next_request_id = count(1)

@request.cache()
async def get_request_id() -> int:
    return next(_next_request_id)

_next_block_id = count(1)

@block.cache()
async def get_block_id() -> int:
    return next(_next_block_id)

async def process_order(order_id: int) -> None:
    async with block:
        await get_pool()
        req_id, block_id = await get_request_id(), await get_block_id()
        print(f"request {req_id} block {block_id}: processing {order_id}")

async def log_audit(order_id: int) -> None:
    async with block:
        await get_pool()
        req_id, block_id = await get_request_id(), await get_block_id()
        print(f"request {req_id} block {block_id}: logging {order_id}")

async def handle_order(order_id: int) -> None:
    async with request:
        req_id = await get_request_id()
        print(f"request {req_id}: handling {order_id}")
        await asyncio.gather(process_order(order_id), log_audit(order_id))

async def main() -> None:
    async with application:
        await handle_order(101)
        await handle_order(102)

asyncio.run(main())
```
```{output}
request 1: handling 101
Opening connection pool
request 1 block 1: processing 101
request 1 block 2: logging 101
request 2: handling 102
request 2 block 3: processing 102
request 2 block 4: logging 102
```
````
"""

import threading
from collections.abc import AsyncGenerator, Callable, Generator, Hashable
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from types import CoroutineType, TracebackType
from typing import Any, ClassVar, Literal, NoReturn, cast, get_args

from stratae._middleware import (
    frozen,
    manager,
    refuse_coroutine,
    refuse_result,
    refuse_swallow,
    refuse_unenterable,
    task_cancelling,
    type_of,
)
from stratae.lifecycle._decorators import AsyncCacheDecorator, CacheDecorator
from stratae.lifecycle._slots import ScopeVar, SlotStorage, SlotStore
from stratae.lifecycle._stack import aclose, close, enter_async_generator, enter_generator
from stratae.lifecycle.exceptions import (
    AlreadyDeactivatedError,
    CacheKeyConflictError,
    DependentScopeActiveError,
    InvalidScopeOptionError,
    NothingToDeactivateError,
    RequiredScopeInactiveError,
    SharedRequiresContextError,
    SharedScopeThreadsError,
    WrongContextDeactivationError,
)

IsolationType = Literal["shared", "context"]
StorageType = Literal["dense", "sparse"]

type ScopeMiddleware = Callable[["Scope"], AbstractContextManager[object]]
"""Given the scope, returns the sync context manager to enter around one of its activations."""

type AsyncScopeMiddleware = Callable[
    ["AsyncScope"], AbstractContextManager[object] | AbstractAsyncContextManager[object]
]
"""Given the scope, returns the context manager, sync or async, to enter around an activation."""


def _validate_types(name: str, isolation: IsolationType, storage: StorageType) -> None:
    if isolation not in frozenset(get_args(IsolationType)):
        raise InvalidScopeOptionError(name, "isolation")
    if storage not in frozenset(get_args(StorageType)):
        raise InvalidScopeOptionError(name, "storage")


def _validate_threads(name: str, isolation: IsolationType, threads: bool) -> None:
    if threads and isolation == "shared":
        raise SharedScopeThreadsError(name)


def _validate_requires(
    name: str, isolation: IsolationType, requires: "BaseScope[Any] | None"
) -> None:
    if requires and isolation == "shared" and requires.isolation == "context":
        raise SharedRequiresContextError(name, requires.name)


def _held(context: Any, middleware: object, asynchronous: bool) -> Generator[None, None, None]:
    """
    Hold a middleware's sync context manager entered, as a resource of the activation.

    Entered with the activation, it sits beneath every resource the activation enters later,
    so closing the activation exits it last, with the activation's error, as nested `with`
    statements would. Unlike a `with` statement, its exit can't swallow that error, and an
    `async def` `__enter__` or `__exit__` is refused, as {py:mod}`stratae.middleware` says.

    :param context: What the middleware returned. Its methods are looked up on its type, as
        `with` does, and it's refused if it has none.
    :param middleware: The middleware it came from, for error messages.
    :param asynchronous: Whether an async scope holds it, so a swallowed cancellation is
        checked against the task.
    :raises MiddlewareResultError: If `context` isn't a sync context manager.
    """
    kind = type_of(context)
    try:
        enter, leave = kind.__enter__, kind.__exit__
    except AttributeError:
        refuse_result(middleware, context, asynchronous)
    if (result := enter(context)).__class__ is CoroutineType:
        refuse_coroutine(result, middleware)
    try:
        yield
    except BaseException as error:
        suppressed = leave(context, type(error), error, error.__traceback__)
        if suppressed.__class__ is CoroutineType:
            refuse_coroutine(suppressed, middleware)
        if suppressed:
            refuse_swallow(error, middleware, task_cancelling() if asynchronous else True)
        raise
    if (result := leave(context, None, None, None)).__class__ is CoroutineType:
        refuse_coroutine(result, middleware)


async def _held_async(context: Any, kind: Any, middleware: object) -> AsyncGenerator[None]:
    """Hold a middleware's async context manager entered, as {py:func}`_held` does."""
    await kind.__aenter__(context)
    try:
        yield
    except BaseException as error:
        if await kind.__aexit__(context, type(error), error, error.__traceback__):
            refuse_swallow(error, middleware, task_cancelling())
        raise
    await kind.__aexit__(context, None, None, None)


class BaseScope[M]:
    """
    Shared state and validation behind Scope and AsyncScope.

    See {py:class}`Scope` and {py:class}`AsyncScope` for the concrete, usable classes. `M` is
    the kind of middleware the scope takes.
    """

    __slots__ = (
        "_lock",
        "_middleware",
        "_new_slots",
        "_parent_lock",
        "_plain",
        "_requires",
        "_slots",
        "_var",
        "name",
    )

    _requires: "BaseScope[Any] | None"
    _middleware: tuple[M, ...]
    _plain: bool
    """Whether activation is only setting the scope active: no parent to count, no middleware."""
    _asynchronous: ClassVar[bool]
    """Whether the scope can enter async context managers."""
    _slots: SlotStore
    _var: ScopeVar
    _new_slots: Callable[[], SlotStorage]
    _lock: "threading.Lock | None"
    _parent_lock: "threading.Lock | None"

    def __init__(
        self,
        name: str,
        isolation: IsolationType = "context",
        storage: StorageType = "dense",
        requires: "BaseScope[Any] | None" = None,
        threads: bool = False,
        middleware: tuple[M, ...] = (),
    ) -> None:
        """
        Create a scope.

        :param name: Identifier for the scope, e.g. `"request"`.
        :param isolation: `"shared"` for one cache across all threads and tasks, or
            `"context"` (default) for one per execution context.
        :param storage: `"dense"` (default) for positional slots, or `"sparse"` for lazily
            allocated ones.
        :param requires: A scope that must be active before this one activates. A shared
            scope can't require a context scope.
        :param threads: Compute each entry once even when threads share an activation of this
            context scope. Slows each entry's first computation.
        :param middleware: Callables each returning a context manager to enter around every
            activation, outermost first. Each is called with the scope. See the module docs.
        :raises LifecycleConfigurationError: If `isolation` or `storage` is invalid, a shared
            scope requires a context scope, or a shared scope takes `threads=True`.
        :raises MiddlewareNotTupleError: If `middleware` is a single callable or a string.
        :raises MiddlewareNotCallableError: If `middleware` holds something that isn't callable.
        :raises MiddlewareNotEnterableError: If a middleware class isn't a context manager the
            scope can enter.
        """
        _validate_types(name, isolation, storage)
        _validate_threads(name, isolation, threads)
        _validate_requires(name, isolation, requires)
        owner = f"{type(self).__name__} {name!r}"
        self._middleware = frozen(middleware, owner, "middleware")
        refuse_unenterable(self._middleware, owner, self._asynchronous)
        self._plain = requires is None and not self._middleware

        self.name: str = name
        self._requires = requires
        slots = SlotStore(
            name, shared=isolation == "shared", dense=storage == "dense", threads=threads
        )
        self._slots = slots
        # Copied out of the store for the activation hot paths.
        self._var = slots.var
        self._new_slots = slots.new
        self._lock = slots.lock
        self._parent_lock = requires._lock if requires is not None else None

    @property
    def isolation(self) -> IsolationType:
        """Cache isolation strategy for this scope."""
        return "shared" if self._slots.shared else "context"

    @property
    def storage(self) -> StorageType:
        """Slot storage strategy for this scope."""
        return "dense" if self._slots.dense else "sparse"

    @property
    def threads(self) -> bool:
        """Whether cached entries are computed once across threads sharing an activation."""
        return self._slots.threads

    @property
    def middleware(self) -> tuple[M, ...]:
        """The middleware entered around each activation, outermost first."""
        return self._middleware

    @property
    def requires(self) -> "BaseScope[Any] | None":
        """The scope that must be active before this one can activate, if any."""
        return self._requires

    def is_active(self) -> bool:
        """Whether this scope has a live activation in the calling context."""
        return bool(self._var.get(None))

    def _activate(self) -> SlotStorage:
        """
        Activate this scope, returning the new activation's storage.

        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        parent_lock = self._parent_lock
        if requires := self._requires:
            if parent_lock is not None:
                parent_lock.acquire()
            try:
                parent_slots = requires._var.get(None)
                if not parent_slots:
                    raise RequiredScopeInactiveError(self.name, requires.name)
                parent_slots[1] += 1  # the parent's slot 1: live dependents
            finally:
                if parent_lock is not None:
                    parent_lock.release()
        else:
            parent_slots = None
        slots = self._new_slots()
        slots[0] = []  # slot 0: entered resources
        slots[3] = parent_slots  # slot 3: parent's storage
        try:
            slots[2] = self._var.set(slots)  # slot 2: reset token
        except BaseException:
            if parent_slots is not None:
                # Only a shared scope's set raises, and a shared scope can only require
                # another shared scope, so there's always a parent lock here.
                with cast(threading.Lock, parent_lock):
                    parent_slots[1] -= 1  # the parent's slot 1: live dependents
            raise
        return slots

    def _end(self, expected: SlotStorage | None) -> SlotStorage:
        """
        Check the current activation can end, then reset the scope's activation holder.

        :param expected: The activation's storage, or `None` for the current one.
        :returns: The ended activation's storage, still to be emptied, closed, and released.
        :raises ScopeActivationError: If the activation can't be ended here.
        """
        var = self._var
        lock = self._lock
        if lock is not None:
            lock.acquire()
        try:
            slots = var.get(None)
            if expected is not None and slots is not expected:
                _raise_not_current(self.name, expected)
            if not slots:
                raise NothingToDeactivateError(self.name)
            if slots[1] > 0:  # slot 1: live dependents
                raise DependentScopeActiveError(self.name)
            try:
                var.reset(slots[2])  # slot 2: reset token
            except ValueError:
                raise WrongContextDeactivationError(self.name) from None
            return slots
        finally:
            if lock is not None:
                lock.release()


def _release(parent_slots: SlotStorage, parent_lock: "threading.Lock | None") -> None:
    """Stop counting an ended, closed activation against its parent's storage."""
    if parent_lock is None:
        parent_slots[1] -= 1  # the parent's slot 1: live dependents
    else:
        # Faster than `with` on this hot path.
        parent_lock.acquire()
        try:
            parent_slots[1] -= 1  # the parent's slot 1: live dependents
        finally:
            parent_lock.release()


def _raise_not_current(name: str, expected: SlotStorage) -> NoReturn:
    """Raise for a token whose activation isn't the scope's current one."""
    if not expected:
        raise AlreadyDeactivatedError(name)
    raise WrongContextDeactivationError(name)


class Activation:
    """
    Token for one activation of a sync scope.

    Returned by {py:meth}`Scope.activate`. Pass it to {py:meth}`Scope.deactivate`, or use it
    with `with`.
    """

    __slots__ = ("scope", "slots")

    slots: Any
    """The activation's live slot storage, declared `Any` rather than `SlotStorage`."""

    def __init__(self, scope: "Scope", slots: SlotStorage) -> None:
        """Construct the token for one activation of `scope`."""
        self.scope: Scope = scope
        self.slots = slots

    def __enter__(self) -> "Activation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate the scope this token belongs to."""
        return self.scope.deactivate(self, exc)


class AsyncActivation:
    """
    Token for one activation of an async scope.

    Returned by {py:meth}`AsyncScope.activate`. Pass it to {py:meth}`AsyncScope.deactivate`,
    or use it with `async with`.
    """

    __slots__ = ("scope", "slots")

    slots: Any
    """The activation's live slot storage, declared `Any` rather than `SlotStorage`."""

    def __init__(self, scope: "AsyncScope", slots: SlotStorage) -> None:
        """Construct the token for one activation of `scope`."""
        self.scope: AsyncScope = scope
        self.slots = slots

    async def __aenter__(self) -> "AsyncActivation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate the scope this token belongs to."""
        return await self.scope.deactivate(self, exc)


class Scope(BaseScope[ScopeMiddleware]):
    """A sync-flavored scope - activated with `with`, caches sync functions."""

    __slots__ = ()

    _asynchronous = False

    def __enter__(self) -> None:
        """
        Activate this scope for the duration of the `with` block.

        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        # Inlined _activate for a plain scope: `with scope:` is the hot path.
        if self._plain:
            slots = self._new_slots()
            slots[0] = []  # slot 0: entered resources
            slots[2] = self._var.set(slots)  # slot 2: reset token
        else:
            slots = self._activate()
            if self._middleware:
                self._enter_middleware(slots)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate this scope's current activation, closing its resources, then middleware."""
        # Inlined deactivate: `with scope:` is the hot path.
        slots = self._end(None)
        contexts, parent_slots = slots[0], slots[3]  # slots 0 and 3
        slots.clear()
        try:
            return close(contexts, exc) if contexts else False
        finally:
            if parent_slots is not None:
                _release(parent_slots, self._parent_lock)

    def _enter_middleware(self, slots: SlotStorage) -> None:
        """
        Enter the middleware around a new activation, as its first resources.

        One that raises refuses the activation: it ends with that error, exiting the
        middleware already entered, and the error propagates.
        """
        try:
            for middleware in self._middleware:
                enter_generator(slots, _held(middleware(self), middleware, False))
        except BaseException as refusal:
            self.deactivate(None, refusal)
            raise

    def activate(self) -> Activation:
        """
        Activate this scope, returning a token to pass to `deactivate()`.

        Prefer `with scope:` for a plain block; it's faster. The scope's middleware is entered
        before this returns.

        :returns: An {py:class}`Activation` token.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        :raises Exception: Whatever a middleware raises on entry, refusing the activation.
        """
        slots = self._activate()
        if self._middleware:
            self._enter_middleware(slots)
        return Activation(self, slots)

    def deactivate(
        self, activation: Activation | None = None, exc: BaseException | None = None
    ) -> bool:
        """
        Deactivate an activation of this scope, closing the resources entered within it.

        :param activation: The token from {py:meth}`Scope.activate`, or `None` for the current
            activation.
        :param exc: The exception ending the activation, passed to each resource's cleanup and
            then to the middleware's exits.
        :returns: Whether `exc` was suppressed.
        :raises ScopeActivationError: If the scope isn't active, `activation` isn't current, or
            a scope requiring this one is still active.
        """
        slots = self._end(None if activation is None else activation.slots)
        contexts, parent_slots = slots[0], slots[3]  # slots 0 and 3
        slots.clear()
        try:
            return close(contexts, exc) if contexts else False
        finally:
            if parent_slots is not None:
                _release(parent_slots, self._parent_lock)

    def cache(
        self,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> CacheDecorator:
        """
        Create a decorator that caches a function's result within this scope.

        Like `functools.lru_cache`, results are keyed by arguments. A generator function's
        generator is entered, its yielded value cached, and closed when the activation ends.

        ```{tip}
        Pass `ignore_params=True` when the result doesn't vary by argument within an
        activation. It uses a single, faster slot instead of a keyed dict.
        ```

        :param cache_key: Derives a hashable key from the arguments. Defaults to the
            arguments themselves.
        :param ignore_params: Cache one value per activation regardless of arguments.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises CacheKeyConflictError: If both `cache_key` and `ignore_params` are given.
        """
        if ignore_params and cache_key is not None:
            raise CacheKeyConflictError()
        return CacheDecorator(self._slots, cache_key, ignore_params)


class AsyncScope(BaseScope[AsyncScopeMiddleware]):
    """An async-flavored scope - activated with `async with`, caches sync and async functions."""

    __slots__ = ()

    _asynchronous = True

    async def __aenter__(self) -> None:
        """
        Activate this scope for the duration of the `async with` block.

        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        # Inlined _activate for a plain scope: `async with scope:` is the hot path.
        if self._plain:
            slots = self._new_slots()
            slots[0] = []  # slot 0: entered resources
            slots[2] = self._var.set(slots)  # slot 2: reset token
        else:
            slots = self._activate()
            if self._middleware:
                await self._enter_middleware(slots)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate this scope's current activation, closing its resources, then middleware."""
        # Inlined deactivate: `async with scope:` is the hot path; this saves a coroutine.
        slots = self._end(None)
        contexts, parent_slots = slots[0], slots[3]  # slots 0 and 3
        slots.clear()
        try:
            return await aclose(contexts, exc) if contexts else False
        finally:
            if parent_slots is not None:
                _release(parent_slots, self._parent_lock)

    async def _enter_middleware(self, slots: SlotStorage) -> None:
        """
        Enter the middleware around a new activation, as its first resources.

        Async context managers are awaited, sync ones entered directly. One that raises
        refuses the activation: it ends with that error, exiting the middleware already
        entered, and the error propagates.
        """
        try:
            for middleware in self._middleware:
                context, kind, is_async = manager(middleware, self)
                if is_async:
                    await enter_async_generator(slots, _held_async(context, kind, middleware))
                else:
                    enter_generator(slots, _held(context, middleware, True))
        except BaseException as refusal:
            await self.deactivate(None, refusal)
            raise

    async def activate(self) -> AsyncActivation:
        """
        Activate this scope, returning a token to pass to `deactivate()`.

        Prefer `async with scope:` for a plain block; it's faster. The scope's middleware is
        entered before this returns, so it's awaited, as `deactivate()` is.

        :returns: An {py:class}`AsyncActivation` token.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        :raises Exception: Whatever a middleware raises on entry, refusing the activation.
        """
        slots = self._activate()
        if self._middleware:
            await self._enter_middleware(slots)
        return AsyncActivation(self, slots)

    async def deactivate(
        self, activation: AsyncActivation | None = None, exc: BaseException | None = None
    ) -> bool:
        """
        Deactivate an activation of this scope, closing the resources entered within it.

        :param activation: The token from {py:meth}`AsyncScope.activate`, or `None` for the
            current activation.
        :param exc: The exception ending the activation, passed to each resource's cleanup and
            then to the middleware's exits.
        :returns: Whether `exc` was suppressed.
        :raises ScopeActivationError: If the scope isn't active, `activation` isn't current, or
            a scope requiring this one is still active.
        """
        slots = self._end(None if activation is None else activation.slots)
        contexts, parent_slots = slots[0], slots[3]  # slots 0 and 3
        slots.clear()
        try:
            return await aclose(contexts, exc) if contexts else False
        finally:
            if parent_slots is not None:
                _release(parent_slots, self._parent_lock)

    def cache(
        self,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> AsyncCacheDecorator:
        """
        Create a decorator that caches a function's result within this scope.

        Accepts sync and async functions. A generator function's generator, sync or async,
        is entered, its yielded value cached, and closed when the activation ends.

        :param cache_key: Derives a hashable key from the arguments. Defaults to the
            arguments themselves.
        :param ignore_params: Cache one value per activation regardless of arguments.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises CacheKeyConflictError: If both `cache_key` and `ignore_params` are given.
        """
        if ignore_params and cache_key is not None:
            raise CacheKeyConflictError()
        return AsyncCacheDecorator(self._slots, cache_key, ignore_params)
