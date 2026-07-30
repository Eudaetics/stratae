"""
New scope classes - a parallel implementation, not yet wired into the rest of the package.

`BaseScope` holds the state a scope owns directly: the `ContextVar`/`SharedVar` for its
activation, the empty-slot template copied on activation, and the slot-allocation
bookkeeping `cache()` will use. `Scope` and `AsyncScope` are the concrete, usable
subclasses, differing only in which exit stack type they use.
"""

from contextvars import ContextVar
from types import TracebackType
from typing import Any, Callable, Hashable, get_args

from stratae.lifecycle._decorators2 import AsyncCacheDecorator, CacheDecorator
from stratae.lifecycle._scope import (
    UNSET,
    AsyncExitStack,
    ExitStack,
    ScopeVar,
    SharedToken,
    SharedVar,
    SlotDict,
    SlotStorage,
)
from stratae.lifecycle.exceptions import (
    LifecycleConfigurationError,
    ScopeActivationError,
    ScopeInactiveError,
)
from stratae.lifecycle.scope import IsolationType, StorageType


class SharedVar2(SharedVar):
    """
    A SharedVar that rejects re-entrant activation and stale-token deactivation.

    The base SharedVar always returns the same reused token from set(), and reset()
    unconditionally clears storage regardless of which token is passed - fine for the old
    Lifecycle, which never calls reset() on a SharedToken (it always calls clear()
    directly). This subclass mints a genuinely distinct token per activation and
    validates it in reset(), so activate()/deactivate() can detect re-entrant activation
    and stale/mismatched deactivation instead of silently corrupting a concurrent
    activation's state - and it lets deactivate() call reset(token) uniformly for both
    var flavors, without an isinstance check, the same way ContextVar already behaves.
    """

    __slots__ = ("_current_token",)

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._current_token: SharedToken | None = None

    def set(self, value: SlotStorage) -> SharedToken:
        """Activate the scope, raising if it's already active."""
        if self.storage is not UNSET:
            raise ScopeActivationError(
                f"Cannot activate shared scope {self.name!r}: already active."
            )
        token = SharedToken(self)
        self.storage = value
        self._current_token = token
        return token

    def reset(self, token: SharedToken) -> None:
        """Deactivate the scope, raising if token isn't the current activation."""
        if token is not self._current_token:
            raise ScopeActivationError(
                f"Cannot deactivate shared scope {self.name!r}: "
                "token is not the current activation."
            )
        self.storage = UNSET
        self._current_token = None


class BaseScope:
    """
    Shared state and validation behind Scope and AsyncScope.

    See {py:class}`Scope` and {py:class}`AsyncScope` for the concrete, usable classes.

    Storage defaults to dense. Below ~50 registered functions, dense wins outright
    regardless of touched count - allocating a dict already costs more than copying the
    whole list. Above that, it's the touched/registered ratio that decides: dense and
    sparse roughly break even around 1-4% touched (2% at 1,000 registered / 20 touched),
    sparse pulling ahead below it (~4x faster at 1,000 registered / 0 touched) and dense
    pulling ahead above it (~1.5x faster at 1,000 registered / 90 touched).
    """

    __slots__ = (
        "name",
        "_isolation",
        "_storage",
        "_requires",
        "_var",
        "_template",
        "_counter",
        "_free_slots",
        "_activation_cls",
        "_parent_sparse",
    )

    _exit_stack_cls: type[ExitStack] | type[AsyncExitStack]
    _dense_activation_cls: Callable[..., Any]
    _sparse_activation_cls: Callable[..., Any]
    _isolation: IsolationType
    _storage: StorageType
    _requires: "BaseScope | None"
    _var: ScopeVar
    _template: SlotStorage
    _counter: int
    _free_slots: list[int]
    _activation_cls: Callable[..., Any]
    _parent_sparse: bool

    def __init__(
        self,
        name: str,
        isolation: IsolationType = "context",
        storage: StorageType = "dense",
        requires: "BaseScope | None" = None,
    ) -> None:
        """
        Initialize the scope's identity, isolation, and storage, and build its state.

        :param name: Identifier for the scope (e.g. `"request"`, `"application"`). Must
            be a valid Python identifier.
        :param isolation: Cache isolation strategy for this scope, one of the
            {py:data}`IsolationType` values. `"shared"` uses a single cache visible to all
            concurrent tasks/threads while the scope is active, regardless of execution
            context - suitable for application-wide state such as database pools.
            `"context"` (the default) isolates the cache per execution context, backed by
            a `contextvars.ContextVar`, so concurrent contexts (e.g. concurrent requests)
            each see their own cache - suitable for request- or session-scoped state.
        :param storage: Slot storage strategy for this scope's cached values, one of the
            {py:data}`StorageType` values. `"dense"` (the default) indexes slots directly
            by position - the cheapest per-access cost, but every activation pays to
            copy/reset the full slot list, so it fits scopes with few registered
            functions or where most of them get used per activation. `"sparse"` allocates
            slots lazily and resets in O(touched) rather than O(registered) - the fit for
            scopes registering many functions where a given activation only touches a
            handful, e.g. a large API's per-resource caches.
        :param requires: The scope that must be active before this one can activate, or
            `None` for a scope with no such requirement. A `"shared"` scope may only
            require another `"shared"` scope, since a `"context"` scope's activity is
            per-execution-context and there's no single answer to "is it active" that
            would hold for every context concurrently sharing the requiring scope.
        :raises LifecycleConfigurationError: If `isolation`/`storage` is not one of their
            allowed values, or this scope is `"shared"` while `requires` is a
            `"context"`-isolated scope.

        """
        if isolation not in frozenset(get_args(IsolationType)):
            raise LifecycleConfigurationError(f"Invalid scope isolation given for {name}.")
        if storage not in frozenset(get_args(StorageType)):
            raise LifecycleConfigurationError(f"Invalid scope storage given for {name}.")
        if requires is not None and isolation == "shared" and requires.isolation == "context":
            raise LifecycleConfigurationError(
                f"Shared scope {name!r} cannot require context-isolated scope {requires.name!r}."
            )

        self.name = name
        self._isolation = isolation
        self._storage = storage
        self._requires = requires
        self._template = [UNSET, 0] if storage == "dense" else SlotDict()
        self._var = ContextVar(name) if isolation == "context" else SharedVar2(name)
        self._counter = 2
        self._free_slots = []
        self._parent_sparse = requires is not None and requires._storage == "sparse"
        self._activation_cls = (
            self._sparse_activation_cls if storage == "sparse" else self._dense_activation_cls
        )

    @property
    def isolation(self) -> IsolationType:
        """Cache isolation strategy for this scope - fixed at construction, never reassignable."""
        return self._isolation

    @property
    def storage(self) -> StorageType:
        """Slot storage strategy for this scope - fixed at construction, never reassignable."""
        return self._storage

    @property
    def requires(self) -> "BaseScope | None":
        """The scope that must be active before this one can activate, if any."""
        return self._requires

    def is_active(self) -> bool:
        """Whether this scope has a live activation in the calling context."""
        return self._var.get(UNSET) is not UNSET

    def is_shared(self) -> bool:
        """Whether this scope's activation holder is a SharedVar, needing lock-guarded access."""
        return isinstance(self._var, SharedVar)

    def activation_var(self) -> ScopeVar:
        """
        Return the scope's raw activation holder - a ContextVar or SharedVar.

        :returns: The scope's `ContextVar`/`SharedVar`.
        """
        return self._var

    def get_slots(self) -> SlotStorage:
        """
        Get this scope's live slot storage for the current activation.

        :returns: The scope's live slot storage.
        :raises ScopeInactiveError: If the scope has no active activation in the calling
            context.
        """
        try:
            return self._var.get()
        except LookupError:
            raise ScopeInactiveError(f"Scope {self.name!r} is not active.") from None

    def exit_stack_type(self) -> type[ExitStack] | type[AsyncExitStack]:
        """
        Return this scope's exit stack type, for codegen that lazily initializes exit stacks.

        :returns: `ExitStack` for a `Scope`, `AsyncExitStack` for an `AsyncScope`.
        """
        return self._exit_stack_cls

    def allocate_slot(self) -> int:
        """
        Allocate a dedicated slot for a cached function - a value directly, or a dict entry.

        Internal to the cache decorators; not meant to be called directly. Slots 0 and 1
        are reserved (the exit stack and the live-dependent count), so the first allocated
        slot is 2.

        :returns: The allocated slot's index/key, to be passed to
            {py:meth}`BaseScope.release_slot` once the owning wrapper is garbage collected.
        """
        if free := self._free_slots:
            return free.pop()
        if isinstance(self._template, SlotDict):
            slot = self._counter
            self._counter = slot + 1
            return slot
        self._template.append(UNSET)
        active = self._var.get(UNSET)
        if isinstance(active, list):
            active.append(UNSET)
        return len(self._template) - 1

    def release_slot(self, slot: int) -> None:
        """
        Return a slot to the free pool once its owning cache wrapper is gone.

        :param slot: The slot index/key returned by {py:meth}`BaseScope.allocate_slot`.
        """
        active = self._var.get(UNSET)
        if isinstance(active, list):
            active[slot] = UNSET
        elif active is not UNSET:
            active.pop(slot, None)
        self._free_slots.append(slot)


class Activation:
    """
    The token a dense `Scope.activate()` returns - use as `with`, or pass to `deactivate()`.

    Returned by {py:meth}`Scope.activate`, not constructed directly. See
    {py:class}`SparseActivation` for the sparse-storage counterpart, which differs only in
    how it reads the live-dependent count.
    """

    __slots__ = ("var", "token", "slots", "parent_slots")

    slots: Any
    """
    The activation's live slot storage, declared `Any` rather than `SlotStorage`.

    Every subclass reads slot 1 the way its own storage flavor reads cheapest - a list
    subscript here, a `dict.get` in {py:class}`SparseActivation` - and a union-typed
    attribute would force a narrowing call at each read to satisfy the checker. The
    storage flavor is fixed when the owning scope picks the activation class, so the
    narrowing would never actually be deciding anything at runtime.
    """

    def __init__(
        self,
        var: ScopeVar,
        token: Any,
        slots: SlotStorage,
        parent_slots: SlotStorage | None,
    ) -> None:
        self.var = var
        self.token = token
        self.slots = slots
        self.parent_slots = parent_slots

    def __enter__(self) -> "Activation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc: Exception | None,
        tb: TracebackType | None,
    ) -> None:
        """
        Deactivate the scope this token belongs to.

        The only copy of the sync deactivation logic - `Scope.deactivate` delegates here
        rather than inlining its own, so the guard lives in one place per storage flavor
        instead of being duplicated across both entry points. `parent_slots` was already
        resolved in `activate()` (legitimate there - it's a BaseScope method touching
        another BaseScope's state), so this never needs to reach across into the parent
        scope itself. `parent_slots is None` skips the `try`/`finally` entirely - it exists
        only to guarantee the decrement runs even if `stack.close()` raises, so it's pure
        overhead for a scope nothing requires.

        The three exception arguments are named rather than collected with `*exc_info`:
        `with` passes them positionally, so named parameters land straight in the frame's
        locals, where varargs would build a throwaway tuple on every deactivation. The
        scope's var is held directly rather than reached through `token.var`, which is a
        descriptor call on a real `contextvars.Token`, and the name for the error message
        comes off the var instead of costing a slot for a backref to the scope.
        """
        slots = self.slots
        if slots[1] > 0:
            raise ScopeActivationError(
                f"Cannot deactivate {self.var.name!r}: a scope requiring it is still active."
            )
        self.var.reset(self.token)
        parent_slots = self.parent_slots
        if parent_slots is None:
            stack = slots[0]
            if stack is not UNSET:
                stack.close()
        else:
            try:
                stack = slots[0]
                if stack is not UNSET:
                    stack.close()
            finally:
                parent_slots[1] -= 1


class SparseActivation(Activation):
    """
    The token a sparse `Scope.activate()` returns, reading slot 1 without materializing it.

    A sparse scope's template is an empty `SlotDict`, so an activation nothing depends on
    never allocates a hash table at all - the property that makes sparse storage worth
    choosing. Reserving slot 1 for the live-dependent count in the template would undo
    that, charging every activation of every sparse scope a table allocation and an insert
    for a counter most of them never use. So the count is written only when a requiring
    scope actually activates, and read here as `1 in slots and slots[1]` - a `__contains__`
    check does not route an absent key through `SlotDict.__missing__`, and it short-circuits
    before the subscript on the overwhelmingly common no-dependents path.

    Not `dict.get`. Measured on this benchmark, `get` is slower than both a bare subscript
    and a membership test followed by one, enough to swamp what avoiding `__missing__`
    saves. Slot 0 is left as a plain `slots[0]` for the same reason: it is present whenever
    the activation entered a resource, so a `get` there is pure loss.
    """

    __slots__ = ()

    def __exit__(
        self,
        exc_type: type[Exception] | None,
        exc: Exception | None,
        tb: TracebackType | None,
    ) -> None:
        """Deactivate the scope, treating an absent slot 1 as a count of zero."""
        slots = self.slots
        if 1 in slots and slots[1] > 0:
            raise ScopeActivationError(
                f"Cannot deactivate {self.var.name!r}: a scope requiring it is still active."
            )
        self.var.reset(self.token)
        parent_slots = self.parent_slots
        if parent_slots is None:
            stack = slots[0]
            if stack is not UNSET:
                stack.close()
        else:
            try:
                stack = slots[0]
                if stack is not UNSET:
                    stack.close()
            finally:
                parent_slots[1] -= 1


class AsyncActivation:
    """
    The token a dense `AsyncScope.activate()` returns - `async with`, or to `deactivate()`.

    Returned by {py:meth}`AsyncScope.activate`, not constructed directly. See
    {py:class}`AsyncSparseActivation` for the sparse-storage counterpart, which differs
    only in how it reads the live-dependent count.
    """

    __slots__ = ("var", "token", "slots", "parent_slots")

    slots: Any
    """
    The activation's live slot storage, declared `Any` rather than `SlotStorage`.

    See {py:attr}`Activation.slots` - same reasoning, same trade.
    """

    def __init__(
        self,
        var: ScopeVar,
        token: Any,
        slots: SlotStorage,
        parent_slots: SlotStorage | None,
    ) -> None:
        self.var = var
        self.token = token
        self.slots = slots
        self.parent_slots = parent_slots

    async def __aenter__(self) -> "AsyncActivation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc: Exception | None,
        tb: TracebackType | None,
    ) -> None:
        """
        Deactivate the scope this token belongs to.

        The only copy of the async deactivation logic - `AsyncScope.deactivate` delegates
        here rather than inlining its own, so the guard lives in one place per storage
        flavor instead of being duplicated across both entry points. `parent_slots` was
        already resolved in `activate()` (legitimate there - it's a BaseScope method
        touching another BaseScope's state), so this never needs to reach across into the
        parent scope itself. `parent_slots is None` skips the `try`/`finally` entirely - it
        exists only to guarantee the decrement runs even if `stack.aclose()` raises, so
        it's pure overhead for a scope nothing requires.

        The three exception arguments are named rather than collected with `*exc_info`:
        `async with` passes them positionally, so named parameters land straight in the
        frame's locals, where varargs would build a throwaway tuple on every deactivation.
        The scope's var is held directly rather than reached through `token.var`, which is
        a descriptor call on a real `contextvars.Token`, and the name for the error message
        comes off the var instead of costing a slot for a backref to the scope.
        """
        slots = self.slots
        if slots[1] > 0:
            raise ScopeActivationError(
                f"Cannot deactivate {self.var.name!r}: a scope requiring it is still active."
            )
        self.var.reset(self.token)
        parent_slots = self.parent_slots
        if parent_slots is None:
            stack = slots[0]
            if stack is not UNSET:
                await stack.aclose()
        else:
            try:
                stack = slots[0]
                if stack is not UNSET:
                    await stack.aclose()
            finally:
                parent_slots[1] -= 1


class AsyncSparseActivation(AsyncActivation):
    """
    The token a sparse `AsyncScope.activate()` returns, reading slot 1 without creating it.

    See {py:class}`SparseActivation` for why the live-dependent count is absent from a
    sparse scope's template and read with `dict.get` here.
    """

    __slots__ = ()

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc: Exception | None,
        tb: TracebackType | None,
    ) -> None:
        """Deactivate the scope, treating an absent slot 1 as a count of zero."""
        slots = self.slots
        if 1 in slots and slots[1] > 0:
            raise ScopeActivationError(
                f"Cannot deactivate {self.var.name!r}: a scope requiring it is still active."
            )
        self.var.reset(self.token)
        parent_slots = self.parent_slots
        if parent_slots is None:
            stack = slots[0]
            if stack is not UNSET:
                await stack.aclose()
        else:
            try:
                stack = slots[0]
                if stack is not UNSET:
                    await stack.aclose()
            finally:
                parent_slots[1] -= 1


class Scope(BaseScope):
    """A sync-flavored scope - activated with `with`, caches sync functions."""

    __slots__ = ()

    _exit_stack_cls = ExitStack
    _dense_activation_cls = Activation
    _sparse_activation_cls = SparseActivation

    def activate(self, *, force: bool = False) -> Activation:
        """
        Activate this scope, returning a token usable as `with` or passed to `deactivate()`.

        :param force: Skip the check that `requires` (if set) is currently active. The
            check exists to fail at the point of misuse rather than later, wherever the
            missing scope actually gets touched - `force` is for cases that legitimately
            don't want the full chain active, e.g. testing this scope's own behavior in
            isolation. It does not make the requirement disappear: code that reaches into
            `requires` while it's genuinely inactive still fails there, same as always.
            Dangerous: a force-activated activation is never counted as depending on
            `requires`, even if `requires` becomes active later during this activation's
            lifetime - `requires` can deactivate out from under it at any point, with no
            protection, for this activation's entire life, not just at the moment of
            activation.
        :returns: An `Activation` - enter it directly (`with scope.activate():`) or hold
            onto it and call {py:meth}`Scope.deactivate` manually, for split-callback
            lifecycles where activation and deactivation happen in different functions.
        :raises ScopeActivationError: If `requires` is set, not active, and `force` is
            not given.
        """
        parent_slots: Any = None
        requires = self._requires
        if requires is not None:
            parent_slots = requires._var.get(UNSET)
            if parent_slots is UNSET:
                parent_slots = None
                if not force:
                    raise ScopeActivationError(
                        f"Cannot activate {self.name!r}: required scope "
                        f"{requires.name!r} is not active."
                    )
        slots = self._template.copy()
        var = self._var
        token = var.set(slots)
        if parent_slots is not None:
            if self._parent_sparse and 1 not in parent_slots:
                parent_slots[1] = 1
            else:
                parent_slots[1] += 1
        return self._activation_cls(var, token, slots, parent_slots)

    def deactivate(self, activation: Activation) -> None:
        """
        Deactivate the scope activation identified by the given token.

        Delegates to the activation's own `__exit__` rather than keeping a second copy of
        the deactivation logic: the guard on slot 1 differs between dense and sparse
        storage, and duplicating it here would mean four copies to keep in step instead of
        two. The extra call costs a frame, which this split-callback path can afford in a
        way the `with` path could not.

        :param activation: The `Activation` returned by the matching
            {py:meth}`Scope.activate` call.
        :raises ScopeActivationError: If the scope is not currently active, or a scope
            requiring this one is still active.
        """
        activation.__exit__(None, None, None)

    def cache(
        self,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> CacheDecorator:
        """
        Create a decorator that caches a function's result within this scope.

        A {py:func}`resource <stratae.lifecycle.resource.resource>`-tagged function is
        entered automatically. Its yielded value is cached in place of the context manager
        itself, and exited when this scope's activation ends.

        :param cache_key: Callable deriving a hashable cache key from the decorated
            function's arguments. When omitted, the key is the arguments themselves.
            Mutually exclusive with `ignore_params`.
        :param ignore_params: Cache a single value per scope activation regardless of
            arguments, instead of keying by argument values. Mutually exclusive with
            `cache_key`.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises ValueError: If both `cache_key` and `ignore_params` are given.
        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return CacheDecorator(self, cache_key, ignore_params)


class AsyncScope(BaseScope):
    """An async-flavored scope - activated with `async with`, caches sync and async functions."""

    __slots__ = ()

    _exit_stack_cls = AsyncExitStack
    _dense_activation_cls = AsyncActivation
    _sparse_activation_cls = AsyncSparseActivation

    def activate(self, *, force: bool = False) -> AsyncActivation:
        """
        Activate this scope, returning a token usable as `async with` or passed to `deactivate()`.

        :param force: Skip the check that `requires` (if set) is currently active. The
            check exists to fail at the point of misuse rather than later, wherever the
            missing scope actually gets touched - `force` is for cases that legitimately
            don't want the full chain active, e.g. testing this scope's own behavior in
            isolation. It does not make the requirement disappear: code that reaches into
            `requires` while it's genuinely inactive still fails there, same as always.
            Dangerous: a force-activated activation is never counted as depending on
            `requires`, even if `requires` becomes active later during this activation's
            lifetime - `requires` can deactivate out from under it at any point, with no
            protection, for this activation's entire life, not just at the moment of
            activation.
        :returns: An `AsyncActivation` - enter it directly (`async with scope.activate():`)
            or hold onto it and call {py:meth}`AsyncScope.deactivate` manually, for
            split-callback lifecycles where activation and deactivation happen in
            different functions.
        :raises ScopeActivationError: If `requires` is set, not active, and `force` is
            not given.
        """
        parent_slots: Any = None
        requires = self._requires
        if requires is not None:
            parent_slots = requires._var.get(UNSET)
            if parent_slots is UNSET:
                parent_slots = None
                if not force:
                    raise ScopeActivationError(
                        f"Cannot activate {self.name!r}: required scope "
                        f"{requires.name!r} is not active."
                    )
        slots = self._template.copy()
        var = self._var
        token = var.set(slots)
        if parent_slots is not None:
            if self._parent_sparse and 1 not in parent_slots:
                parent_slots[1] = 1
            else:
                parent_slots[1] += 1
        return self._activation_cls(var, token, slots, parent_slots)

    async def deactivate(self, activation: AsyncActivation) -> None:
        """
        Deactivate the scope activation identified by the given token.

        Delegates to the activation's own `__aexit__` rather than keeping a second copy of
        the deactivation logic: the guard on slot 1 differs between dense and sparse
        storage, and duplicating it here would mean four copies to keep in step instead of
        two. The extra call costs a frame, which this split-callback path can afford in a
        way the `async with` path could not.

        :param activation: The `AsyncActivation` returned by the matching
            {py:meth}`AsyncScope.activate` call.
        :raises ScopeActivationError: If the scope is not currently active, or a scope
            requiring this one is still active.
        """
        await activation.__aexit__(None, None, None)

    def cache(
        self,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> AsyncCacheDecorator:
        """
        Create a decorator that caches a function's result within this scope.

        Accepts sync functions, async functions, and
        {py:func}`resource <stratae.lifecycle.resource.resource>`/
        {py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged
        context managers of either flavor. A tagged function is entered automatically. Its
        yielded value is cached in place of the context manager itself, and exited when
        this scope's activation ends.

        :param cache_key: Callable deriving a hashable cache key from the decorated
            function's arguments. When omitted, the key is the arguments themselves.
            Mutually exclusive with `ignore_params`.
        :param ignore_params: Cache a single value per scope activation regardless of
            arguments, instead of keying by argument values. Mutually exclusive with
            `cache_key`.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises ValueError: If both `cache_key` and `ignore_params` are given.
        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return AsyncCacheDecorator(self, cache_key, ignore_params)
