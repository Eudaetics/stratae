"""
`BaseLifecycle`, `Lifecycle`, and `AsyncLifecycle` - scope activation and caching.

`Lifecycle` manages sync functions and sync generators (via `resource`).
`AsyncLifecycle` additionally manages async functions, async generators
(via `async_resource`), and sync functions used from async code. Both
share the scope/slot mechanics implemented once on `BaseLifecycle`.

Construct one with a sequence of `Scope` declarations, then:

* `start(scope)` returns a context manager (``async with`` for
  `AsyncLifecycle`) that activates the scope for its duration.
* ``@lifecycle.cache(scope)`` decorates a function so its result is cached
  for the lifetime of that scope's active activation.

For example::

```python
from stratae.lifecycle import Lifecycle, Scope, resource

lifecycle = Lifecycle([Scope("application", "shared"), Scope("request", "context")])


@lifecycle.cache("application")
def get_database_connection() -> Connection:
    return create_connection()  # cached for the application scope


@lifecycle.cache("request")
@resource
def get_request_session():
    session = create_session()
    try:
        yield session  # cached for the request scope
    finally:
        session.close()


with lifecycle.start("application"):
    with lifecycle.start("request"):
        connection = get_database_connection()
        session = get_request_session()
```
"""

from contextvars import Token
from typing import Callable, Hashable, Sequence

from stratae.lifecycle._context import AsyncLifecycleContext, LifecycleContext
from stratae.lifecycle._decorators import AsyncCacheDecorator, CacheDecorator
from stratae.lifecycle._scope import (
    UNSET,
    AsyncExitStack,
    ExitStack,
    SharedToken,
    SlotDict,
    SlotStorage,
    build_lifecycle_state,
)
from stratae.lifecycle._validation import validate_config
from stratae.lifecycle.exceptions import (
    ScopeActivationError,
    ScopeInactiveError,
    ScopeNotFoundError,
)
from stratae.lifecycle.scope import Scope


class BaseLifecycle[
    ContextT: (LifecycleContext, AsyncLifecycleContext),
    StackT: (ExitStack, AsyncExitStack),
]:
    """
    Shared behavior for sync and async lifecycles.

    Generic over the context manager and exit stack types a concrete subclass uses, so
    `start()` and `get_exit_stack()` stay typed to the subclass's sync/async flavor while
    every other scope/slot mechanic lives here once. `Lifecycle` and `AsyncLifecycle` only
    need to pin `_context_cls`/`_exit_stack_cls` and add `pop()`/`cache()`, which differ in
    ways (async-ness, decorator class) generics can't paper over.

    Type Parameters:
        ContextT: The scope context manager type `start()` returns -
            `LifecycleContext` or `AsyncLifecycleContext`.
        StackT: The exit stack type `get_exit_stack()` returns - `ExitStack`
            or `AsyncExitStack`.
    """

    __slots__ = (
        "_scopes",
        "_templates",
        "_vars",
        "_contexts",
        "_counters",
        "_free_slots",
    )

    _context_cls: type[ContextT]
    _exit_stack_cls: type[StackT]

    def __init__(self, scopes: Sequence[Scope]) -> None:
        """
        Initialize the lifecycle manager with its scopes.

        Args:
            scopes: The scopes this manager activates and caches within.
                Each scope's name must be unique.

        Raises:
            LifecycleConfigurationError: If `scopes` is empty, or two
                scopes share the same name.

        """
        validate_config(scopes)
        self._scopes: dict[str, Scope] = {scope.name: scope for scope in scopes}
        state = build_lifecycle_state(scopes, self._context_cls)
        self._templates = state.templates
        self._vars = state.scope_vars
        self._contexts = state.contexts
        self._counters = state.counters
        self._free_slots = state.free_slots

    def push(self, scope: str) -> Token[SlotStorage] | SharedToken:
        """
        Push a new lifecycle scope activation, returning the token pop() takes.

        A lower-level alternative to `start()`, for callers managing
        activation lifetime outside a ``with`` block.

        Args:
            scope: Name of the scope to activate.

        Returns:
            A token identifying this activation; pass it to `pop()` to
            deactivate the scope again.

        Raises:
            ScopeNotFoundError: If `scope` was not declared on this
                lifecycle.

        """
        try:
            return self._vars[scope].set(self._templates[scope].copy())
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

    def start(self, scope: str) -> ContextT:
        """
        Get a scope context by name for use as a context manager.

        Shared scopes return the same reusable context instance on every call - their
        activations don't nest, so sharing one per scope skips an allocation per
        activation. Context-isolated scopes get a fresh context with the scope's
        ContextVar and slot template pre-resolved, so its enter/exit run with zero dict
        lookups. The dict lookups double as scope validation.

        Args:
            scope: Name of the scope to activate.

        Returns:
            A context manager (``async with`` for `AsyncLifecycle`) that
            activates the scope on entry and deactivates it on exit,
            closing any exit stack that activation created.

        Raises:
            ScopeNotFoundError: If `scope` was not declared on this
                lifecycle.

        """
        ctx = self._contexts.get(scope)
        if ctx is not None:
            return ctx
        try:
            return self._context_cls(self._vars[scope], self._templates[scope])
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

    def is_empty(self) -> bool:
        """
        Check if there are no active scopes, introspection only.

        Returns:
            `True` if no scope declared on this lifecycle currently has an
            active activation in the calling context.

        """
        return all(var.get(UNSET) is UNSET for var in self._vars.values())

    def active_scopes(self) -> Sequence[str]:
        """
        Get a list of active scopes, in declaration order.

        Returns:
            The names of scopes with an active activation in the calling
            context.

        """
        return [name for name in self._scopes if self._is_active(name)]

    def _is_active(self, scope: str) -> bool:
        """Whether the scope has a live activation in the calling context."""
        return self._vars[scope].get(UNSET) is not UNSET

    def _scope_error(self, scope: str) -> ScopeNotFoundError | ScopeInactiveError:
        """Build the exception for a failed scope lookup - only ever called off the hot path."""
        if scope not in self._scopes:
            return ScopeNotFoundError(f"Unknown scope: {scope}")
        return ScopeInactiveError(f"Scope '{scope}' is not active.")

    def get_exit_stack(self, scope: str) -> StackT:
        """
        Get the exit stack for the specified lifecycle scope, creating it on first use.

        Args:
            scope: Name of the scope whose exit stack to fetch.

        Returns:
            The scope's `ExitStack`/`AsyncExitStack`, closed automatically
            when the scope's current activation deactivates.

        Raises:
            ScopeNotFoundError: If `scope` was not declared on this
                lifecycle.
            ScopeInactiveError: If `scope` has no active activation in the
                calling context.

        """
        slots = self.get_slots(scope)
        stack = slots[0]
        if stack is UNSET:
            stack = slots[0] = self._exit_stack_cls()
        return stack

    def allocate_slot(self, scope: str) -> int:
        """
        Allocate a dedicated slot for a cached function - a value directly, or a dict.

        Every scope draws from its free-slot stack first, if it isn't empty (see
        release_slot), before minting a new index. Dense-backed scopes that do mint a new
        index grow their template by one slot and index by position, and also grow the
        currently visible activation's copy in place if one exists - the shared copy, or
        the calling context's; copies live in other execution contexts are unreachable -
        so a slot allocated mid-activation is visible without waiting for the next
        push(). Sparse-backed
        scopes hand out the next int key from a per-scope counter instead -
        SlotDict.__missing__ means the key needn't exist anywhere until first written.

        Args:
            scope: Name of the scope to allocate a slot in.

        Returns:
            The allocated slot's index/key, to be passed to `release_slot`
            once the owning wrapper is garbage collected.

        Raises:
            ScopeNotFoundError: If `scope` was not declared on this
                lifecycle.

        """
        try:
            template = self._templates[scope]
        except KeyError:
            raise ScopeNotFoundError(f"Unknown scope: {scope}") from None

        if free := self._free_slots.get(scope):
            return free.pop()

        if isinstance(template, SlotDict):
            slot = self._counters[scope]
            self._counters[scope] = slot + 1
            return slot

        template.append(UNSET)
        active = self._vars[scope].get(UNSET)
        if isinstance(active, list):
            active.append(UNSET)
        return len(template) - 1

    def release_slot(self, scope: str, slot: int) -> None:
        """
        Return a scope's slot to the free pool once its owning wrapper is gone.

        Args:
            scope: Name of the scope the slot was allocated in.
            slot: The slot index/key returned by `allocate_slot`.

        """
        active = self._vars[scope].get(UNSET)
        if isinstance(active, list):
            active[slot] = UNSET
        elif active is not UNSET:
            active.pop(slot, None)
        self._free_slots[scope].append(slot)

    def get_slots(self, scope: str) -> SlotStorage:
        """
        Get the scope's slot list - slot 0 is reserved for the exit stack (see __init__).

        An unknown scope's KeyError is a LookupError too, so one except distinguishes
        the two failures through _scope_error.

        Args:
            scope: Name of the scope whose slot storage to fetch.

        Returns:
            The scope's live slot storage for the current activation.

        Raises:
            ScopeNotFoundError: If `scope` was not declared on this
                lifecycle.
            ScopeInactiveError: If `scope` has no active activation in the
                calling context.

        """
        try:
            return self._vars[scope].get()
        except LookupError:
            raise self._scope_error(scope) from None

    def exit_stack_type(self) -> type[StackT]:
        """
        Return the exit stack type for codegen lazily initiating exit stacks.

        Returns:
            `ExitStack` for a `Lifecycle`, `AsyncExitStack` for an
            `AsyncLifecycle`.

        """
        return self._exit_stack_cls


class Lifecycle(BaseLifecycle[LifecycleContext, ExitStack]):
    """
    Manager for caching function results within sync scope activations.

    Register scopes at construction, activate one with `start(scope)` (or
    the lower-level `push`/`pop`), and decorate functions with
    ``@lifecycle.cache(scope)`` to cache their results for that
    activation's lifetime. See the module docstring for a full example.
    """

    __slots__ = ()

    _context_cls = LifecycleContext
    _exit_stack_cls = ExitStack

    def pop(self, token: Token[SlotStorage] | SharedToken) -> None:
        """
        Pop the lifecycle scope activation identified by the token push() returned.

        Under LIFO push/pop discipline, get() always returns the token's own activation;
        a reused or cross-context ContextVar token surfaces contextvars' own error from
        reset(), while a shared scope's token simply clears its var.

        Args:
            token: The token returned by the matching `push()` call.

        Raises:
            ScopeActivationError: If the scope identified by `token` is not
                currently active.

        """
        var = token.var
        try:
            slots = var.get()
        except LookupError:
            raise ScopeActivationError(f"Cannot pop {var.name}: scope is not active.") from None
        if isinstance(token, SharedToken):
            token.var.clear()
        else:
            token.var.reset(token)
        stack = slots[0]
        if stack is not UNSET:
            stack.close()

    def cache(
        self,
        scope: str,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> CacheDecorator:
        """
        Create a decorator to set the lifecycle scope for caching function results.

        A `resource`-tagged function is entered automatically, with its
        yielded value cached in place of the context manager itself, exited
        when the owning scope's activation ends.

        Args:
            scope: Name of the scope the decorated function's result is
                cached in.
            cache_key: Callable deriving a hashable cache key from the
                decorated function's arguments. When omitted, the key is
                the arguments themselves. Mutually exclusive with
                `ignore_params`.
            ignore_params: Cache a single value per scope activation
                regardless of arguments, instead of keying by argument
                values. Mutually exclusive with `cache_key`.

        Returns:
            A decorator, applied to the function whose result should be
            cached.

        Raises:
            ValueError: If both `cache_key` and `ignore_params` are given.

        For example::

        ```python
        @lifecycle.cache("application")
        def get_database_connection() -> Connection:
            return create_connection()
        ```

        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return CacheDecorator(scope, self, cache_key, ignore_params)


class AsyncLifecycle(BaseLifecycle[AsyncLifecycleContext, AsyncExitStack]):
    """
    Manager for caching function results within async scope activations.

    Register scopes at construction, activate one with
    ``async with lifecycle.start(scope):`` (or the lower-level
    `push`/`pop`), and decorate functions with ``@lifecycle.cache(scope)``
    to cache their results for that activation's lifetime. Accepts sync
    functions, async functions, and `resource`/`async_resource`-tagged
    context managers of either flavor.
    """

    __slots__ = ()

    _context_cls = AsyncLifecycleContext
    _exit_stack_cls = AsyncExitStack

    async def pop(self, token: Token[SlotStorage] | SharedToken) -> None:
        """
        Asynchronously pop the lifecycle scope activation identified by the token.

        Under LIFO push/pop discipline, get() always returns the token's own activation;
        a reused or cross-context ContextVar token surfaces contextvars' own error from
        reset(), while a shared scope's token simply clears its var. Only awaits when the
        activation actually created an exit stack.

        Args:
            token: The token returned by the matching `push()` call.

        Raises:
            ScopeActivationError: If the scope identified by `token` is not
                currently active.

        """
        var = token.var
        try:
            slots = var.get()
        except LookupError:
            raise ScopeActivationError(f"Cannot pop {var.name}: scope is not active.") from None
        if isinstance(token, SharedToken):
            token.var.clear()
        else:
            token.var.reset(token)
        stack = slots[0]
        if stack is not UNSET:
            await stack.aclose()

    def cache(
        self,
        scope: str,
        *,
        cache_key: Callable[..., Hashable] | None = None,
        ignore_params: bool = False,
    ) -> AsyncCacheDecorator:
        """
        Create a decorator to set the lifecycle scope for caching function results.

        Accepts sync functions, async functions, and
        `resource`/`async_resource`-tagged context managers of either
        flavor. A tagged function is entered automatically, with its
        yielded value cached in place of the context manager itself,
        exited when the owning scope's activation ends.

        Args:
            scope: Name of the scope the decorated function's result is
                cached in.
            cache_key: Callable deriving a hashable cache key from the
                decorated function's arguments. When omitted, the key is
                the arguments themselves. Mutually exclusive with
                `ignore_params`.
            ignore_params: Cache a single value per scope activation
                regardless of arguments, instead of keying by argument
                values. Mutually exclusive with `cache_key`.

        Returns:
            A decorator, applied to the function whose result should be
            cached.

        Raises:
            ValueError: If both `cache_key` and `ignore_params` are given.

        Example:
            .. code-block:: python

                @lifecycle.cache("request")
                @async_resource
                async def get_session():
                    session = await Session.open()
                    try:
                        yield session
                    finally:
                        await session.close()

        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return AsyncCacheDecorator(scope, self, cache_key, ignore_params)
