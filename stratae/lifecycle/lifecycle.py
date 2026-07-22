"""
Scope activation and per-scope function-result caching.

{py:class}`BaseLifecycle` implements the scope and slot mechanics shared by
{py:class}`Lifecycle` (sync) and {py:class}`AsyncLifecycle` (async), each constructed from
a sequence of {py:class}`Scope <stratae.lifecycle.scope.Scope>` declarations.
{py:func}`BaseLifecycle.start` returns a context manager (``async with`` for
{py:class}`AsyncLifecycle`) that activates a scope for its duration. ``@lifecycle.cache(scope)``
(via {py:func}`Lifecycle.cache` or {py:func}`AsyncLifecycle.cache`) decorates a function so
its result is cached for the lifetime of that scope's active activation. A
{py:func}`resource <stratae.lifecycle.resource.resource>`/
{py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged function is
entered automatically when cached. Its yielded value is cached in place of the context
manager itself.

```{rubric} Example:
```
```{code-block} python
:caption: Reuse one database connection app-wide, and one session per incoming request

import sqlite3
from stratae.lifecycle import Lifecycle, Scope, resource

lifecycle = Lifecycle([Scope("application", "shared"), Scope("request", "context")])

@lifecycle.cache("application")
def get_database_connection() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")  # cached for the application scope

class RequestSession:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.closed = False

    def close(self):
        self.closed = True

@lifecycle.cache("request")
@resource
def get_session():
    session = RequestSession(user_id=42)
    try:
        yield session  # cached for the request scope
    finally:
        session.close()

with lifecycle.start("application"):
    with lifecycle.start("request"):
        connection = get_database_connection()
        session = get_session()
        assert not session.closed

    with lifecycle.start("request"):
        connection_again = get_database_connection()
        session_again = get_session()

assert connection is connection_again  # one connection, reused across every request
assert session is not session_again  # each request gets its own session
assert session.closed  # closed automatically once its "request" activation ended
```

```{note}
Cache keying behaves like `functools.lru_cache`: unless `ignore_params` is set, a
function that takes parameters gets one cached value per distinct set of
arguments (or per `cache_key` result), not one cached value for the whole scope
activation. Calling it again with different arguments computes and caches a
separate value rather than reusing the first one. A function that takes no
parameters has only one possible argument set, so it always uses the same fast
slot path as `ignore_params=True`.
```

```{tip}
If you know a function's result won't actually vary within a scope activation,
even though it still accepts parameters, pass `ignore_params=True`. That caches
the value directly in the function's slot instead of a keyed dict, trading
per-argument caching for a single, faster slot lookup.
```

See {py:class}`BaseLifecycle`, {py:class}`Lifecycle`, and {py:class}`AsyncLifecycle` for
additional examples.
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
    Shared scope and slot mechanics behind Lifecycle and AsyncLifecycle.

    Generic over `ContextT` (the scope context manager type {py:func}`BaseLifecycle.start`
    returns: `LifecycleContext` or `AsyncLifecycleContext`) and `StackT` (the exit stack
    type {py:func}`BaseLifecycle.get_exit_stack` returns: `ExitStack` or `AsyncExitStack`).
    Those type parameters keep `start()` and `get_exit_stack()` typed to a concrete
    subclass's sync/async flavor, while every other scope/slot mechanic lives here once.
    {py:class}`Lifecycle` and {py:class}`AsyncLifecycle` only need to pin
    `_context_cls`/`_exit_stack_cls` and add `pop()`/`cache()`. Those differ in ways
    (async-ness, decorator class) generics can't paper over.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: Lifecycle and AsyncLifecycle share push/pop/start/active_scopes via BaseLifecycle

    from stratae.lifecycle import Lifecycle, Scope

    lifecycle = Lifecycle([Scope("request")])

    with lifecycle.start("request"):
        assert lifecycle.active_scopes() == ["request"]

    assert lifecycle.is_empty()
    ```

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

        :param scopes: The scopes this manager activates and caches within. Each scope's
            name must be unique.
        :raises LifecycleConfigurationError: If `scopes` is empty, or two scopes share
            the same name.

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

        A lower-level alternative to {py:func}`BaseLifecycle.start`, for callers managing
        activation lifetime outside a ``with`` block.

        :param scope: Name of the scope to activate.
        :returns: A token identifying this activation; pass it to `pop()` to deactivate
            the scope again.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: Activate and deactivate a scope manually, outside a with block

        from stratae.lifecycle import Lifecycle, Scope

        lifecycle = Lifecycle([Scope("request")])

        token = lifecycle.push("request")
        assert lifecycle.active_scopes() == ["request"]

        lifecycle.pop(token)
        assert lifecycle.is_empty()
        ```

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

        :param scope: Name of the scope to activate.
        :returns: A context manager (``async with`` for {py:class}`AsyncLifecycle`) that
            activates the scope on entry and deactivates it on exit, closing any exit
            stack that activation created.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: Activating a scope makes its cached functions callable for the block's duration

        from stratae.lifecycle import Lifecycle, Scope

        lifecycle = Lifecycle([Scope("request")])

        with lifecycle.start("request"):
            assert lifecycle.active_scopes() == ["request"]

        assert lifecycle.is_empty()
        ```

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
        Report whether this lifecycle currently has no active scope activations.

        Introspection only.

        :returns: `True` if no scope declared on this lifecycle currently has an active
            activation in the calling context.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: A freshly constructed lifecycle has no active scopes

        from stratae.lifecycle import Lifecycle, Scope

        lifecycle = Lifecycle([Scope("request")])
        assert lifecycle.is_empty()

        with lifecycle.start("request"):
            assert not lifecycle.is_empty()
        ```

        """
        return all(var.get(UNSET) is UNSET for var in self._vars.values())

    def active_scopes(self) -> Sequence[str]:
        """
        Get the names of currently active scopes, in declaration order.

        :returns: The names of scopes with an active activation in the calling context.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: Nested scope activations are reported in declaration order, not activation order

        from stratae.lifecycle import Lifecycle, Scope

        lifecycle = Lifecycle([Scope("application", "shared"), Scope("request")])

        with lifecycle.start("request"):
            with lifecycle.start("application"):
                assert lifecycle.active_scopes() == ["application", "request"]
        ```

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

        :param scope: Name of the scope whose exit stack to fetch.
        :returns: The scope's `ExitStack`/`AsyncExitStack`, closed automatically when the
            scope's current activation deactivates.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.
        :raises ScopeInactiveError: If `scope` has no active activation in the calling
            context.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: The exit stack is created lazily and reused within one activation

        from stratae.lifecycle import Lifecycle, Scope

        lifecycle = Lifecycle([Scope("request")])

        with lifecycle.start("request"):
            stack = lifecycle.get_exit_stack("request")
            assert lifecycle.get_exit_stack("request") is stack
        ```

        """
        slots = self.get_slots(scope)
        stack = slots[0]
        if stack is UNSET:
            stack = slots[0] = self._exit_stack_cls()
        return stack

    def allocate_slot(self, scope: str) -> int:
        """
        Allocate a dedicated slot for a cached function - a value directly, or a dict entry.

        Every scope draws from its free-slot stack first, if it isn't empty (see
        {py:func}`BaseLifecycle.release_slot`), before minting a new index.

        Dense-backed scopes that do mint a new index grow their template by one slot and
        index by position. They also grow the currently visible activation's copy in
        place, if one exists - the shared copy, or the calling context's (copies living in
        other execution contexts are unreachable). That's why a slot allocated
        mid-activation is visible without waiting for the next
        {py:func}`BaseLifecycle.push`.

        Sparse-backed scopes hand out the next int key from a per-scope counter instead.
        `SlotDict.__missing__` means the key needn't exist anywhere until first written.

        :param scope: Name of the scope to allocate a slot in.
        :returns: The allocated slot's index/key, to be passed to
            {py:func}`BaseLifecycle.release_slot` once the owning wrapper is garbage
            collected.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: A released slot is handed back out before a new one is minted

        from stratae.lifecycle import Lifecycle, Scope

        lifecycle = Lifecycle([Scope("request", "context")])

        first = lifecycle.allocate_slot("request")
        lifecycle.release_slot("request", first)
        second = lifecycle.allocate_slot("request")

        assert second == first  # reused from the free pool
        ```

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

        :param scope: Name of the scope the slot was allocated in.
        :param slot: The slot index/key returned by {py:func}`BaseLifecycle.allocate_slot`.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: Released slots are reused most-recently-released first

        from stratae.lifecycle import Lifecycle, Scope

        lifecycle = Lifecycle([Scope("request", "context")])
        first = lifecycle.allocate_slot("request")
        second = lifecycle.allocate_slot("request")

        lifecycle.release_slot("request", first)
        lifecycle.release_slot("request", second)

        assert lifecycle.allocate_slot("request") == second
        assert lifecycle.allocate_slot("request") == first
        ```

        """
        active = self._vars[scope].get(UNSET)
        if isinstance(active, list):
            active[slot] = UNSET
        elif active is not UNSET:
            active.pop(slot, None)
        self._free_slots[scope].append(slot)

    def get_slots(self, scope: str) -> SlotStorage:
        """
        Get the scope's slot storage - slot 0 is reserved for the exit stack.

        An unknown scope's `KeyError` is a `LookupError` too. One `except` distinguishes
        the two failures through `_scope_error`.

        :param scope: Name of the scope whose slot storage to fetch.
        :returns: The scope's live slot storage for the current activation.
        :raises ScopeNotFoundError: If `scope` was not declared on this lifecycle.
        :raises ScopeInactiveError: If `scope` has no active activation in the calling
            context.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: An inactive scope's slots raise, an active scope's slots are readable and writable

        import pytest
        from stratae.lifecycle import Lifecycle, Scope
        from stratae.lifecycle.exceptions import ScopeInactiveError

        lifecycle = Lifecycle([Scope("request", "context")])
        slot = lifecycle.allocate_slot("request")

        with pytest.raises(ScopeInactiveError):
            lifecycle.get_slots("request")

        with lifecycle.start("request"):
            lifecycle.get_slots("request")[slot] = "cached value"
            assert lifecycle.get_slots("request")[slot] == "cached value"
        ```

        """
        try:
            return self._vars[scope].get()
        except LookupError:
            raise self._scope_error(scope) from None

    def exit_stack_type(self) -> type[StackT]:
        """
        Return the exit stack type, for codegen that lazily initializes exit stacks.

        :returns: `ExitStack` for a {py:class}`Lifecycle`, `AsyncExitStack` for an
            {py:class}`AsyncLifecycle`.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: A sync Lifecycle reports ExitStack as its exit stack type

        from stratae.lifecycle import Lifecycle, Scope
        from stratae.lifecycle._scope import ExitStack

        lifecycle = Lifecycle([Scope("request", "context")])
        assert lifecycle.exit_stack_type() is ExitStack
        ```

        """
        return self._exit_stack_cls


class Lifecycle(BaseLifecycle[LifecycleContext, ExitStack]):
    """
    Manager for caching function results within sync scope activations.

    Register scopes at construction, activate one with {py:func}`BaseLifecycle.start` (or
    the lower-level {py:func}`BaseLifecycle.push`/{py:func}`Lifecycle.pop`), and decorate
    functions with ``@lifecycle.cache(scope)`` to cache their results for that activation's
    lifetime.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: A request ID is generated once per request and reused by every caller within it

    from uuid import uuid4
    from stratae.lifecycle import Lifecycle, Scope

    lifecycle = Lifecycle([Scope("request", "context")])

    @lifecycle.cache("request")
    def get_request_id() -> str:
        return str(uuid4())

    with lifecycle.start("request"):
        logged_id = get_request_id()
        response_id = get_request_id()

    with lifecycle.start("request"):
        next_request_id = get_request_id()

    assert logged_id == response_id  # same ID throughout one request
    assert logged_id != next_request_id  # a new ID for the next request
    ```

    See the module docstring for a further example.
    """

    __slots__ = ()

    _context_cls = LifecycleContext
    _exit_stack_cls = ExitStack

    def pop(self, token: Token[SlotStorage] | SharedToken) -> None:
        """
        Pop the lifecycle scope activation identified by the token returned from push.

        Under LIFO push/pop discipline, `get()` always returns the token's own activation.
        A reused or cross-context `ContextVar` token surfaces `contextvars`' own error from
        `reset()`. A shared scope's token simply clears its var.

        :param token: The token returned by the matching {py:func}`BaseLifecycle.push` call.
        :raises ScopeActivationError: If the scope identified by `token` is not currently
            active.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: Popping the same token twice raises, since the activation is already gone

        import pytest
        from stratae.lifecycle import Lifecycle, Scope
        from stratae.lifecycle.exceptions import ScopeActivationError

        lifecycle = Lifecycle([Scope("request", "context")])
        token = lifecycle.push("request")

        lifecycle.pop(token)

        with pytest.raises(ScopeActivationError):
            lifecycle.pop(token)
        ```

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
        Create a decorator that caches a function's result within the named scope.

        A {py:func}`resource <stratae.lifecycle.resource.resource>`-tagged function is
        entered automatically. Its yielded value is cached in place of the context manager
        itself, and exited when the owning scope's activation ends.

        :param scope: Name of the scope the decorated function's result is cached in.
        :param cache_key: Callable deriving a hashable cache key from the decorated
            function's arguments. When omitted, the key is the arguments themselves.
            Mutually exclusive with `ignore_params`.
        :param ignore_params: Cache a single value per scope activation regardless of
            arguments, instead of keying by argument values. Mutually exclusive with
            `cache_key`.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises ValueError: If both `cache_key` and `ignore_params` are given.

        See the module docstring for cache-keying semantics and when to use
        `ignore_params`.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: Cache per distinct argument value, or collapse to one value with ignore_params

        from stratae.lifecycle import Lifecycle, Scope

        lifecycle = Lifecycle([Scope("request", "context")])

        @lifecycle.cache("request")
        def get_user(user_id: int) -> object:
            return object()

        @lifecycle.cache("request", ignore_params=True)
        def get_current_time() -> object:
            return object()

        with lifecycle.start("request"):
            assert get_user(1) is get_user(1)  # same value, same argument
            assert get_user(1) is not get_user(2)  # different arguments, different value
            assert get_current_time() is get_current_time()  # cached once, args ignored
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
    {py:func}`BaseLifecycle.push`/{py:func}`AsyncLifecycle.pop`), and decorate functions
    with ``@lifecycle.cache(scope)`` to cache their results for that activation's lifetime.
    Accepts sync functions, async functions, and
    {py:func}`resource <stratae.lifecycle.resource.resource>`/
    {py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged context
    managers of either flavor.

    ```{rubric} Example:
    ```
    ```{code-block} python
    :caption: A request ID is generated once per request and reused by every caller within it

    import asyncio
    from uuid import uuid4
    from stratae.lifecycle import AsyncLifecycle, Scope

    lifecycle = AsyncLifecycle([Scope("request", "context")])

    @lifecycle.cache("request")
    async def get_request_id() -> str:
        return str(uuid4())

    async def main():
        async with lifecycle.start("request"):
            logged_id = await get_request_id()
            response_id = await get_request_id()

        async with lifecycle.start("request"):
            next_request_id = await get_request_id()

        assert logged_id == response_id  # same ID throughout one request
        assert logged_id != next_request_id  # a new ID for the next request

    asyncio.run(main())
    ```
    """

    __slots__ = ()

    _context_cls = AsyncLifecycleContext
    _exit_stack_cls = AsyncExitStack

    async def pop(self, token: Token[SlotStorage] | SharedToken) -> None:
        """
        Asynchronously pop the scope activation identified by the token returned from push.

        Under LIFO push/pop discipline, `get()` always returns the token's own activation.
        A reused or cross-context `ContextVar` token surfaces `contextvars`' own error from
        `reset()`. A shared scope's token simply clears its var. This only awaits when the
        activation actually created an exit stack.

        :param token: The token returned by the matching {py:func}`BaseLifecycle.push` call.
        :raises ScopeActivationError: If the scope identified by `token` is not currently
            active.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: Popping the same token twice raises, since the activation is already gone

        import asyncio
        import pytest
        from stratae.lifecycle import AsyncLifecycle, Scope
        from stratae.lifecycle.exceptions import ScopeActivationError

        lifecycle = AsyncLifecycle([Scope("request", "context")])

        async def main():
            token = lifecycle.push("request")
            await lifecycle.pop(token)

            with pytest.raises(ScopeActivationError):
                await lifecycle.pop(token)

        asyncio.run(main())
        ```

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
        Create a decorator that caches a function's result within the named scope.

        Accepts sync functions, async functions, and
        {py:func}`resource <stratae.lifecycle.resource.resource>`/
        {py:func}`async_resource <stratae.lifecycle.resource.async_resource>`-tagged
        context managers of either flavor. A tagged function is entered automatically. Its
        yielded value is cached in place of the context manager itself, and exited when the
        owning scope's activation ends.

        :param scope: Name of the scope the decorated function's result is cached in.
        :param cache_key: Callable deriving a hashable cache key from the decorated
            function's arguments. When omitted, the key is the arguments themselves.
            Mutually exclusive with `ignore_params`.
        :param ignore_params: Cache a single value per scope activation regardless of
            arguments, instead of keying by argument values. Mutually exclusive with
            `cache_key`.
        :returns: A decorator, applied to the function whose result should be cached.
        :raises ValueError: If both `cache_key` and `ignore_params` are given.

        See the module docstring for cache-keying semantics and when to use
        `ignore_params`.

        ```{rubric} Example:
        ```
        ```{code-block} python
        :caption: An async_resource-tagged function's yielded value is cached and closed on exit

        import asyncio
        from stratae.lifecycle import AsyncLifecycle, Scope, async_resource

        lifecycle = AsyncLifecycle([Scope("request", "context")])

        class RemoteClient:
            connected = False

            async def connect(self):
                self.connected = True

            async def disconnect(self):
                self.connected = False

        @lifecycle.cache("request")
        @async_resource
        async def get_client():
            client = RemoteClient()
            await client.connect()
            try:
                yield client
            finally:
                await client.disconnect()

        async def main():
            async with lifecycle.start("request"):
                client = await get_client()
                assert client.connected

            assert not client.connected

        asyncio.run(main())
        ```

        """
        if ignore_params and cache_key is not None:
            raise ValueError("Cannot use both ignore_params and cache_key together.")
        return AsyncCacheDecorator(scope, self, cache_key, ignore_params)
