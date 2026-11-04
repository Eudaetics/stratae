"""
Scope activation and per-scope function-result caching.

{py:class}`Scope` and {py:class}`AsyncScope` hold the cached results for one scope.
Activate one with `with scope:` (`async with` for {py:class}`AsyncScope`), or with
{py:meth}`Scope.activate` and {py:meth}`Scope.deactivate` when the two happen in different
callbacks. `@scope.cache()` caches a function's result for the active activation. A
{py:func}`resource <stratae.lifecycle.resource.resource>`-tagged function is entered when
cached, and its yielded value is cached in its place.

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

Hooks act on a scope's activations. `@scope.on_enter` registers a hook run as each
activation starts, once the scope is active. One that raises refuses the activation, as if
its block had raised. `@scope.on_exit` registers a hook run as each activation ends, after
every resource has exited. It gets the exception the activation ended with, or `None`. That
includes an error from a resource's cleanup, such as a failed commit. Each hook gets the
activation's token, an {py:class}`Activation` or {py:class}`AsyncActivation`. Its `state`
dict carries values from the start hooks to the exit hooks, such as a start time.

`scope.after_success` registers a callback for the active activation only. It runs if that
activation ends cleanly, after its resources have exited, so after a commit. Register it on
the scope whose resources do the work it follows. On an outer scope, it runs only when that
scope ends. Exit hooks and success callbacks only observe: one that raises is logged, and
the activation still ends as it did. A sync {py:class}`Scope` takes sync hooks. An
{py:class}`AsyncScope` takes sync or async ones.

Hooks run in the order they were registered. Hooks in different modules are registered in
the order the modules are imported, which is easy to change by accident. So register hooks
whose order matters in one place, by calling `on_enter` and `on_exit` directly. Each
activation runs the exit hooks that were registered when it started. A hook registered, or
removed with `scope.remove_hook`, while it runs takes effect from the next activation.

When a start hook refuses an activation, the start hooks after it don't run, but every exit
hook does. So an exit hook can't assume its start hook ran: check `activation.state` for
what it set up. An async exit hook or success callback that's cancelled stops the rest, and
the cancellation propagates, even though the activation's work already finished. Hand slow
work, such as a network call, to a task of its own.

````{example} Reporting an order only once its unit of work commits
```{code-block} python
from stratae.lifecycle import Scope, resource
from stratae.lifecycle.scope import Activation

work = Scope("work")

@work.cache()
@resource
def get_transaction():
    yield "transaction"
    print("committed")

@work.on_exit
def report(activation: Activation, error: BaseException | None) -> None:
    print("unit of work", "failed" if error else "succeeded")

def place_order(item: str) -> None:
    get_transaction()
    work.after_success(lambda: print(f"order placed: {item}"))
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
unit of work succeeded
unit of work failed
```
````

````{example} Timing each activation, with state shared from its start hook to its exit hook
```{code-block} python
from stratae.lifecycle import Scope
from stratae.lifecycle.scope import Activation

clock = iter([10.0, 10.25])  # stands in for time.monotonic
job = Scope("job")

@job.on_enter
def start_timer(activation: Activation) -> None:
    activation.state["started"] = next(clock)

@job.on_exit
def record_duration(activation: Activation, error: BaseException | None) -> None:
    started = activation.state.get("started")
    if started is not None:
        print(f"job took {next(clock) - started:.2f}s")

with job:
    pass
```
```{output}
job took 0.25s
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
from collections.abc import Callable, Hashable
from contextvars import ContextVar
from functools import partial
from types import TracebackType
from typing import Any, ClassVar, Literal, NoReturn, cast, get_args

from stratae._callables import is_async_callable
from stratae.lifecycle._decorators import AsyncCacheDecorator, CacheDecorator
from stratae.lifecycle._hooks import (
    EnterHook,
    ExitHook,
    HookRecord,
    SuccessCallback,
    run_enter,
    run_enter_async,
    run_exit,
    run_exit_async,
)
from stratae.lifecycle._slots import UNSET, ScopeVar, SharedVar, SlotDict, SlotStorage
from stratae.lifecycle._stack import aclose, close
from stratae.lifecycle.exceptions import (
    ActivationEndedError,
    AlreadyDeactivatedError,
    AsyncHookError,
    CacheKeyConflictError,
    DependentScopeActiveError,
    InvalidScopeOptionError,
    NoActivationError,
    NothingToDeactivateError,
    RequiredScopeInactiveError,
    SharedRequiresContextError,
    SharedScopeThreadsError,
    WrongContextDeactivationError,
)

_RESERVED_SLOTS = 5
"""
Slots 0-4 hold the entered resources, live-dependent count, reset token, parent's storage,
and the activation's hook record, if it has hooks or success callbacks - see
`stratae.lifecycle._stack` and `stratae.lifecycle._hooks`.
"""

IsolationType = Literal["shared", "context"]
StorageType = Literal["dense", "sparse"]


def _validate_types(name: str, isolation: IsolationType, storage: StorageType) -> None:
    if isolation not in frozenset(get_args(IsolationType)):
        raise InvalidScopeOptionError(name, "isolation")
    if storage not in frozenset(get_args(StorageType)):
        raise InvalidScopeOptionError(name, "storage")


def _validate_threads(name: str, isolation: IsolationType, threads: bool) -> None:
    if threads and isolation == "shared":
        raise SharedScopeThreadsError(name)


def _validate_requires(name: str, isolation: IsolationType, requires: "BaseScope | None") -> None:
    if requires and isolation == "shared" and requires.isolation == "context":
        raise SharedRequiresContextError(name, requires.name)


class BaseScope:
    """
    Shared state and validation behind Scope and AsyncScope.

    See {py:class}`Scope` and {py:class}`AsyncScope` for the concrete, usable classes.
    """

    __slots__ = (
        "_counter",
        "_enter_hooks",
        "_exit_hooks",
        "_free_slots",
        "_isolation",
        "_lock",
        "_new_slots",
        "_parent_lock",
        "_requires",
        "_slot_lock",
        "_storage",
        "_template",
        "_threads",
        "_var",
        "name",
    )

    _isolation: IsolationType
    _storage: StorageType
    _threads: bool
    _requires: "BaseScope | None"
    _var: ScopeVar
    _template: SlotStorage
    _new_slots: Callable[[], SlotStorage]
    _counter: int
    _free_slots: list[int]
    _slot_lock: threading.RLock
    _lock: "threading.Lock | None"
    _parent_lock: "threading.Lock | None"
    _enter_hooks: tuple[EnterHook[Any], ...]
    _exit_hooks: tuple[ExitHook[Any], ...]
    _asynchronous: ClassVar[bool] = False
    """Whether the scope can await its hooks."""
    _token: ClassVar[Callable[[Any, SlotStorage], Any]]
    """Builds the token for one activation: `Activation` or `AsyncActivation`."""

    def __init__(
        self,
        name: str,
        isolation: IsolationType = "context",
        storage: StorageType = "dense",
        requires: "BaseScope | None" = None,
        threads: bool = False,
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
        :raises LifecycleConfigurationError: If `isolation` or `storage` is invalid, a shared
            scope requires a context scope, or a shared scope takes `threads=True`.
        """
        _validate_types(name, isolation, storage)
        _validate_threads(name, isolation, threads)
        _validate_requires(name, isolation, requires)

        self.name = name
        self._isolation = isolation
        self._storage = storage
        self._threads = threads
        self._requires = requires
        if storage == "dense":
            self._template = [UNSET, 0, None, None, None]
            self._new_slots = self._template.copy
        else:
            self._template = SlotDict({1: 0, 2: None, 3: None, 4: None})
            # Built from a plain dict: faster than copying the SlotDict.
            self._new_slots = partial(SlotDict, {1: 0, 2: None, 3: None, 4: None})
        self._var = ContextVar(name) if isolation == "context" else SharedVar(name)
        self._counter = _RESERVED_SLOTS
        self._free_slots = []
        self._slot_lock = threading.RLock()
        self._lock = self._var.lock if isinstance(self._var, SharedVar) else None
        self._parent_lock = requires._lock if requires is not None else None
        self._enter_hooks = ()
        self._exit_hooks = ()

    @property
    def isolation(self) -> IsolationType:
        """Cache isolation strategy for this scope."""
        return self._isolation

    @property
    def storage(self) -> StorageType:
        """Slot storage strategy for this scope."""
        return self._storage

    @property
    def threads(self) -> bool:
        """Whether cached entries are computed once across threads sharing an activation."""
        return self._threads

    @property
    def requires(self) -> "BaseScope | None":
        """The scope that must be active before this one can activate, if any."""
        return self._requires

    def is_active(self) -> bool:
        """Whether this scope has a live activation in the calling context."""
        return bool(self._var.get(None))

    def is_shared(self) -> bool:
        """Whether this scope's activation holder is a lock-guarded SharedVar."""
        return isinstance(self._var, SharedVar)

    def activation_var(self) -> ScopeVar:
        """Return the scope's raw `ContextVar` or `SharedVar`."""
        return self._var

    def get_slots(self) -> SlotStorage:
        """
        Get this scope's live slot storage, extended with any slots allocated since activation.

        :returns: The scope's live slot storage.
        :raises ScopeInactiveError: If the scope isn't active in the calling context.
        """
        try:
            slots = self._var.get()
        except LookupError:
            raise NoActivationError(self.name) from None
        if not slots:
            raise ActivationEndedError(self.name)
        self._extend(slots)
        return slots

    def remove_hook(self, hook: Callable[..., object]) -> None:
        """
        Remove a start or exit hook, so later activations don't run it.

        An activation already running keeps the exit hooks it started with. A hook that isn't
        registered is ignored.

        :param hook: The hook to remove, compared by equality, so a bound method is found
            even through a new reference to it.
        """
        self._enter_hooks = tuple(item for item in self._enter_hooks if item != hook)
        self._exit_hooks = tuple(item for item in self._exit_hooks if item != hook)

    def after_success(self, callback: SuccessCallback) -> None:
        """
        Run a callback once the active activation of this scope ends cleanly.

        The callback belongs to the activation that's active now. It runs after every resource
        has exited, so after a commit, and before the activation's exit hooks. It doesn't run
        if the activation ends with an error. Success callbacks only observe: one that raises
        is logged, and the others still run.

        Register it on the scope whose resources do the work it follows. A callback on an
        outer scope, such as a request's, runs when that scope ends, which may be long after
        an inner scope committed.

        :param callback: Called with no arguments.
        :raises AsyncHookError: If `callback` is async and this is a sync {py:class}`Scope`.
        :raises ScopeInactiveError: If the scope isn't active in the calling context.
        """
        self._check_hook(callback)
        slots = self.get_slots()
        record: HookRecord[Any] | None = slots[4]
        if record is None:
            record = slots[4] = HookRecord(type(self)._token(self, slots), ())
        record.callbacks.append(callback)

    def _add_enter(self, hook: EnterHook[Any]) -> None:
        """Register a start hook, refusing an async one on a sync scope."""
        self._check_hook(hook)
        self._enter_hooks = (*self._enter_hooks, hook)

    def _add_exit(self, hook: ExitHook[Any]) -> None:
        """Register an exit hook, refusing an async one on a sync scope."""
        self._check_hook(hook)
        self._exit_hooks = (*self._exit_hooks, hook)

    def _check_hook(self, hook: Callable[..., Any]) -> None:
        """Raise AsyncHookError for an async hook on a sync scope, which can't await it."""
        if not self._asynchronous and is_async_callable(hook):
            raise AsyncHookError(self.name, hook)

    def _extend(self, slots: SlotStorage) -> None:
        """Extend live dense storage with any slots allocated since it was copied."""
        template = self._template
        if isinstance(template, list) and isinstance(slots, list) and len(slots) < len(template):
            slots.extend(template[len(slots) :])

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
                parent_slots[1] += 1
            finally:
                if parent_lock is not None:
                    parent_lock.release()
        else:
            parent_slots = None
        slots = self._new_slots()
        slots[0] = []
        slots[3] = parent_slots
        try:
            slots[2] = self._var.set(slots)
        except BaseException:
            if parent_slots is not None:
                # Only a shared scope's set raises, and a shared scope can only require
                # another shared scope, so there's always a parent lock here.
                with cast(threading.Lock, parent_lock):
                    parent_slots[1] -= 1
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
            if slots[1] > 0:
                raise DependentScopeActiveError(self.name)
            try:
                var.reset(slots[2])
            except ValueError:
                raise WrongContextDeactivationError(self.name) from None
            return slots
        finally:
            if lock is not None:
                lock.release()

    def allocate_slot(self) -> tuple[int, bool]:
        """
        Allocate a slot for a cached function. Internal to the cache decorators.

        :returns: The slot's index, and whether it was reused from the free pool.
        """
        with self._slot_lock:
            if free := self._free_slots:
                return free.pop(), True
            if isinstance(self._template, SlotDict):
                slot = self._counter
                self._counter = slot + 1
                return slot, False
            self._template.append(UNSET)
            if active := self._var.get(None):
                self._extend(active)
            return len(self._template) - 1, False

    def release_slot(self, slot: int) -> None:
        """
        Return a slot to the free pool once its owning cache wrapper is gone.

        :param slot: The slot index/key returned by {py:meth}`BaseScope.allocate_slot`.
        """
        with self._slot_lock:
            active = self._var.get(None)
            if isinstance(active, list):
                if slot < len(active):
                    active[slot] = UNSET
            elif active is not None:
                active.pop(slot, None)
            self._free_slots.append(slot)


def _release(parent_slots: SlotStorage, parent_lock: "threading.Lock | None") -> None:
    """Stop counting an ended, closed activation against its parent's storage."""
    if parent_lock is None:
        parent_slots[1] -= 1
    else:
        # Faster than `with` on this hot path.
        parent_lock.acquire()
        try:
            parent_slots[1] -= 1
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

    __slots__ = ("scope", "slots", "state")

    slots: Any
    """The activation's live slot storage, declared `Any` rather than `SlotStorage`."""

    def __init__(self, scope: "Scope", slots: SlotStorage) -> None:
        """Construct the token for one activation of `scope`."""
        self.scope = scope
        self.slots = slots
        self.state: dict[str, Any] = {}
        """Per-activation values the scope's hooks share, such as a start time."""

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

    __slots__ = ("scope", "slots", "state")

    slots: Any
    """The activation's live slot storage, declared `Any` rather than `SlotStorage`."""

    def __init__(self, scope: "AsyncScope", slots: SlotStorage) -> None:
        """Construct the token for one activation of `scope`."""
        self.scope = scope
        self.slots = slots
        self.state: dict[str, Any] = {}
        """Per-activation values the scope's hooks share, such as a start time."""

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


class Scope(BaseScope):
    """A sync-flavored scope - activated with `with`, caches sync functions."""

    __slots__ = ()

    _token = Activation

    def on_enter(self, hook: EnterHook[Activation]) -> EnterHook[Activation]:
        """
        Register a hook run as each activation of this scope starts, as a decorator.

        Start hooks run in the order they were registered, once the scope is active, so they
        can use its cached functions. Each gets the activation's {py:class}`Activation`, whose
        `state` it can share with the exit hooks. One that raises refuses the activation:
        it's closed with that error, as if its block had raised, and the error propagates.
        The start hooks after it don't run.

        :param hook: Called with the activation.
        :returns: `hook`, unchanged.
        :raises AsyncHookError: If `hook` is async.
        """
        self._add_enter(hook)
        return hook

    def on_exit(self, hook: ExitHook[Activation]) -> ExitHook[Activation]:
        """
        Register a hook run as each activation of this scope ends, as a decorator.

        Exit hooks run in the order they were registered, after every resource has exited
        and the scope is no longer active. Each gets the activation's
        {py:class}`Activation`, and the exception it ended with: the one raised in its block,
        or by a resource's cleanup, such as a failed commit. It gets `None` if the activation
        ended cleanly. Exit hooks only observe: one that raises is logged, and the activation
        still ends as it did.

        :param hook: Called with the activation and its exception, or `None`.
        :returns: `hook`, unchanged.
        :raises AsyncHookError: If `hook` is async.
        """
        self._add_exit(hook)
        return hook

    def __enter__(self) -> None:
        """
        Activate this scope for the duration of the `with` block.

        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        # Inlined _activate for a scope without a parent: `with scope:` is the hot path.
        if self._requires is None:
            slots = self._new_slots()
            slots[0] = []
            slots[2] = self._var.set(slots)
        else:
            slots = self._activate()
        if self._enter_hooks or self._exit_hooks:
            self._start(slots)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate this scope's current activation, closing its entered resources."""
        # Inlined deactivate: `with scope:` is the hot path.
        slots = self._end(None)
        if slots[4] is not None:
            return self._finish(slots, exc)
        contexts, parent_slots = slots[0], slots[3]
        slots.clear()
        try:
            return close(contexts, exc) if contexts else False
        finally:
            if parent_slots is not None:
                _release(parent_slots, self._parent_lock)

    def _start(self, slots: SlotStorage) -> Activation:
        """Record the activation's exit hooks, then run its start hooks."""
        activation = Activation(self, slots)
        slots[4] = HookRecord(activation, self._exit_hooks)
        try:
            run_enter(self._enter_hooks, activation)
        except BaseException as error:
            self._finish(self._end(slots), error)
            raise
        return activation

    def _finish(self, slots: SlotStorage, exc: BaseException | None) -> bool:
        """Close an ended activation, then run its success callbacks and exit hooks."""
        contexts, parent_slots, record = slots[0], slots[3], slots[4]
        slots.clear()
        try:
            try:
                suppressed = close(contexts, exc) if contexts else False
            finally:
                if parent_slots is not None:
                    _release(parent_slots, self._parent_lock)
        except BaseException as error:
            run_exit(self.name, record, error)
            raise
        run_exit(self.name, record, None if suppressed else exc)
        return suppressed

    def activate(self) -> Activation:
        """
        Activate this scope, returning a token to pass to `deactivate()`.

        Prefer `with scope:` for a plain block; it's faster.

        :returns: An {py:class}`Activation` token.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        slots = self._activate()
        if self._enter_hooks or self._exit_hooks:
            return self._start(slots)
        return Activation(self, slots)

    def deactivate(
        self, activation: Activation | None = None, exc: BaseException | None = None
    ) -> bool:
        """
        Deactivate an activation of this scope, closing the resources entered within it.

        :param activation: The token from {py:meth}`Scope.activate`, or `None` for the current
            activation.
        :param exc: The exception ending the activation, passed to each resource's cleanup.
        :returns: Whether `exc` was suppressed.
        :raises ScopeActivationError: If the scope isn't active, `activation` isn't current, or
            a scope requiring this one is still active.
        """
        return self._finish(self._end(None if activation is None else activation.slots), exc)

    def cache(
        self,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> CacheDecorator:
        """
        Create a decorator that caches a function's result within this scope.

        Like `functools.lru_cache`, results are keyed by arguments. A
        {py:func}`resource <stratae.lifecycle.resource.resource>`-tagged function is entered,
        its yielded value cached, and exited when the activation ends.

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
        return CacheDecorator(self, cache_key, ignore_params)


class AsyncScope(BaseScope):
    """An async-flavored scope - activated with `async with`, caches sync and async functions."""

    __slots__ = ()

    _asynchronous = True
    _token = AsyncActivation

    def on_enter(self, hook: EnterHook[AsyncActivation]) -> EnterHook[AsyncActivation]:
        """
        Register a sync or async hook run as each activation of this scope starts.

        Usable as a decorator. Start hooks run in the order they were registered, once the
        scope is active, so they can use its cached functions. Each gets the activation's
        {py:class}`AsyncActivation`, whose `state` it can share with the exit hooks. One that
        raises refuses the activation: it's closed with that error, as if its block had
        raised, and the error propagates. The start hooks after it don't run.

        :param hook: Called with the activation, and awaited if it's async.
        :returns: `hook`, unchanged.
        """
        self._add_enter(hook)
        return hook

    def on_exit(self, hook: ExitHook[AsyncActivation]) -> ExitHook[AsyncActivation]:
        """
        Register a sync or async hook run as each activation of this scope ends.

        Usable as a decorator. Exit hooks run in the order they were registered, after every
        resource has exited and the scope is no longer active. Each gets the activation's
        {py:class}`AsyncActivation`, and the exception it ended with: the one raised in its
        block, or by a resource's cleanup, such as a failed commit. It gets `None` if the
        activation ended cleanly. Exit hooks only observe: one that raises is logged, and the
        activation still ends as it did.

        :param hook: Called with the activation and its exception, or `None`, and awaited if
            it's async.
        :returns: `hook`, unchanged.
        """
        self._add_exit(hook)
        return hook

    async def __aenter__(self) -> None:
        """
        Activate this scope for the duration of the `async with` block.

        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        # Inlined _activate for a scope without a parent: `async with scope:` is the hot path.
        if self._requires is None:
            slots = self._new_slots()
            slots[0] = []
            slots[2] = self._var.set(slots)
        else:
            slots = self._activate()
        if self._enter_hooks or self._exit_hooks:
            await self._start(slots)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        """Deactivate this scope's current activation, closing its entered resources."""
        # Inlined deactivate: `async with scope:` is the hot path; this saves a coroutine.
        slots = self._end(None)
        if slots[4] is not None:
            return await self._finish(slots, exc)
        contexts, parent_slots = slots[0], slots[3]
        slots.clear()
        try:
            return await aclose(contexts, exc) if contexts else False
        finally:
            if parent_slots is not None:
                _release(parent_slots, self._parent_lock)

    async def _start(self, slots: SlotStorage) -> AsyncActivation:
        """Record the activation's exit hooks, then run its start hooks."""
        activation = AsyncActivation(self, slots)
        slots[4] = HookRecord(activation, self._exit_hooks)
        try:
            await run_enter_async(self._enter_hooks, activation)
        except BaseException as error:
            await self._finish(self._end(slots), error)
            raise
        return activation

    async def _finish(self, slots: SlotStorage, exc: BaseException | None) -> bool:
        """Close an ended activation, then run its success callbacks and exit hooks."""
        contexts, parent_slots, record = slots[0], slots[3], slots[4]
        slots.clear()
        try:
            try:
                suppressed = await aclose(contexts, exc) if contexts else False
            finally:
                if parent_slots is not None:
                    _release(parent_slots, self._parent_lock)
        except BaseException as error:
            await run_exit_async(self.name, record, error)
            raise
        await run_exit_async(self.name, record, None if suppressed else exc)
        return suppressed

    async def activate(self) -> AsyncActivation:
        """
        Activate this scope, returning a token to pass to `deactivate()`.

        Prefer `async with scope:` for a plain block; it's faster.

        :returns: An {py:class}`AsyncActivation` token.
        :raises ScopeActivationError: If `requires` is set and not currently active.
        """
        slots = self._activate()
        if self._enter_hooks or self._exit_hooks:
            return await self._start(slots)
        return AsyncActivation(self, slots)

    async def deactivate(
        self, activation: AsyncActivation | None = None, exc: BaseException | None = None
    ) -> bool:
        """
        Deactivate an activation of this scope, closing the resources entered within it.

        :param activation: The token from {py:meth}`AsyncScope.activate`, or `None` for the
            current activation.
        :param exc: The exception ending the activation, passed to each resource's cleanup.
        :returns: Whether `exc` was suppressed.
        :raises ScopeActivationError: If the scope isn't active, `activation` isn't current, or
            a scope requiring this one is still active.
        """
        return await self._finish(self._end(None if activation is None else activation.slots), exc)

    def cache(
        self,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> AsyncCacheDecorator:
        """
        Create a decorator that caches a function's result within this scope.

        Accepts sync and async functions. A
        {py:func}`resource <stratae.lifecycle.resource.resource>` or
        {py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged function
        is entered, its yielded value cached, and exited when the activation ends.

        :param cache_key: Derives a hashable key from the arguments. Defaults to the
            arguments themselves.
        :param ignore_params: Cache one value per activation regardless of arguments.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises CacheKeyConflictError: If both `cache_key` and `ignore_params` are given.
        """
        if ignore_params and cache_key is not None:
            raise CacheKeyConflictError()
        return AsyncCacheDecorator(self, cache_key, ignore_params)
