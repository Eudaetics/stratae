"""
New scope classes - a parallel implementation, not yet wired into the rest of the package.

`BaseScope` holds the state a scope owns directly: the `ContextVar`/`SharedVar` for its
activation, the empty-slot template copied on activation, and the slot-allocation
bookkeeping `cache()` will use. `Scope` and `AsyncScope` are the concrete, usable
subclasses, differing only in which exit stack type they use.
"""

from contextvars import ContextVar, Token
from typing import get_args

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
    )

    _exit_stack_cls: type[ExitStack] | type[AsyncExitStack]
    _isolation: IsolationType
    _storage: StorageType
    _requires: "BaseScope | None"
    _var: ScopeVar
    _template: SlotStorage
    _counter: int
    _free_slots: list[int]

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
        self._template = [UNSET, 0] if storage == "dense" else SlotDict({1: 0})
        self._var = ContextVar(name) if isolation == "context" else SharedVar(name)
        self._counter = 2
        self._free_slots = []

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
    The token `Scope.activate()` returns - use directly as `with`, or pass to `deactivate()`.

    Returned by {py:meth}`Scope.activate`, not constructed directly.
    """

    __slots__ = ("_scope", "token", "depends_on_active")

    def __init__(
        self,
        scope: "Scope",
        token: "Token[SlotStorage] | SharedToken",
        depends_on_active: bool,
    ) -> None:
        self._scope = scope
        self.token = token
        self.depends_on_active = depends_on_active

    def __enter__(self) -> "Activation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Deactivate the scope this token belongs to."""
        self._scope.deactivate(self)


class AsyncActivation:
    """
    The token `AsyncScope.activate()` returns - use as `async with`, or pass to `deactivate()`.

    Returned by {py:meth}`AsyncScope.activate`, not constructed directly.
    """

    __slots__ = ("_scope", "token", "depends_on_active")

    def __init__(
        self,
        scope: "AsyncScope",
        token: "Token[SlotStorage] | SharedToken",
        depends_on_active: bool,
    ) -> None:
        self._scope = scope
        self.token = token
        self.depends_on_active = depends_on_active

    async def __aenter__(self) -> "AsyncActivation":
        """Return self - the scope was already activated by `activate()`."""
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Deactivate the scope this token belongs to."""
        await self._scope.deactivate(self)


class Scope(BaseScope):
    """A sync-flavored scope - activated with `with`, caches sync functions."""

    __slots__ = ()

    _exit_stack_cls = ExitStack

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
        depends_on_active = False
        if self._requires is not None:
            if self._requires.is_active():
                self._requires._var.get()[1] += 1
                depends_on_active = True
            elif not force:
                raise ScopeActivationError(
                    f"Cannot activate {self.name!r}: required scope "
                    f"{self._requires.name!r} is not active."
                )
        token = self._var.set(self._template.copy())
        return Activation(self, token, depends_on_active)

    def deactivate(self, activation: Activation) -> None:
        """
        Deactivate the scope activation identified by the given token.

        :param activation: The `Activation` returned by the matching
            {py:meth}`Scope.activate` call.
        :raises ScopeActivationError: If the scope is not currently active, or a scope
            requiring this one is still active.
        """
        token = activation.token
        var = token.var
        try:
            slots = var.get()
        except LookupError:
            raise ScopeActivationError(
                f"Cannot deactivate {self.name!r}: scope is not active."
            ) from None
        if slots[1] > 0:
            raise ScopeActivationError(
                f"Cannot deactivate {self.name!r}: a scope requiring it is still active."
            )
        if isinstance(token, SharedToken):
            token.var.clear()
        else:
            token.var.reset(token)
        try:
            stack = slots[0]
            if stack is not UNSET:
                stack.close()
        finally:
            if activation.depends_on_active and self._requires is not None:
                self._requires._var.get()[1] -= 1


class AsyncScope(BaseScope):
    """An async-flavored scope - activated with `async with`, caches sync and async functions."""

    __slots__ = ()

    _exit_stack_cls = AsyncExitStack

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
        depends_on_active = False
        if self._requires is not None:
            if self._requires.is_active():
                self._requires._var.get()[1] += 1
                depends_on_active = True
            elif not force:
                raise ScopeActivationError(
                    f"Cannot activate {self.name!r}: required scope "
                    f"{self._requires.name!r} is not active."
                )
        token = self._var.set(self._template.copy())
        return AsyncActivation(self, token, depends_on_active)

    async def deactivate(self, activation: AsyncActivation) -> None:
        """
        Deactivate the scope activation identified by the given token.

        :param activation: The `AsyncActivation` returned by the matching
            {py:meth}`AsyncScope.activate` call.
        :raises ScopeActivationError: If the scope is not currently active, or a scope
            requiring this one is still active.
        """
        token = activation.token
        var = token.var
        try:
            slots = var.get()
        except LookupError:
            raise ScopeActivationError(
                f"Cannot deactivate {self.name!r}: scope is not active."
            ) from None
        if slots[1] > 0:
            raise ScopeActivationError(
                f"Cannot deactivate {self.name!r}: a scope requiring it is still active."
            )
        if isinstance(token, SharedToken):
            token.var.clear()
        else:
            token.var.reset(token)
        try:
            stack = slots[0]
            if stack is not UNSET:
                await stack.aclose()
        finally:
            if activation.depends_on_active and self._requires is not None:
                self._requires._var.get()[1] -= 1
