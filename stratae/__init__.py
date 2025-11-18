"""
Stratae: Composable tools for building applications in Python.

Stratae provides lightweight, high-performance tools for dependency injection,
lifecycle management, and context variables. Built on Python's native features,
it works anywhere: APIs, CLIs, workers, and tests.

Quick example:
    >>> from stratae.depends import Depends, inject
    >>> from stratae.lifecycle import Lifecycle
    >>>
    >>> lifecycle = Lifecycle(['application', 'request'])
    >>>
    >>> @lifecycle.cache('application')
    >>> def get_database():
    ...     return Database(url="postgresql://...")
    >>>
    >>> @inject
    >>> def create_user(name: str, db = Depends(get_database)):
    ...     return db.users.create(name=name)
    >>>
    >>> with lifecycle.start('application'):
    ...     with lifecycle.start('request'):
    ...         user = create_user("Alice")

Modules:
    cache: Caching protocols and implementations
    depends: Dependency injection and resolution
    lifecycle: Scope-based caching and resource management
    context: Context variables with nested scopes
"""
