# Stratae

**Composable tools for dependency injection in Python. Fast, simple, and framework-agnostic.**

Stratae is a toolkit designed of small, focused components that layer together to create more powerful systems. It's built to complement Python's 3.12+ features, leveraging decorators, contexts, and functions to create a system that works anywhere.

Write your business logic once, use it everywhere: APIs, CLIs, workers, and tests.

```bash
pip install stratae
```

## Quick Start

```python
from stratae.depends import Depends, inject
from stratae.lifecycle import Lifecycle

lifecycle = Lifecycle(["application"])

type Database = dict[str, list[dict[str, str]]]

# Simple database connection (just a dict for demo)
@lifecycle.cache('application')
def get_database() -> Database:
    return {"users": []}

@inject
def create_user(name: str, db: Database = Depends(get_database)):
    user = {"name": name}
    db["users"].append(user)
    return user

# Use anywhere: APIs, CLIs, workers, tests
with lifecycle.start('application'):
    user = create_user("Alice")
    print(f"Created user: {user['name']}")
```

## Why Stratae?

**Simple by design.** Stratae doesn't impose architectural patterns or force you into a framework. It provides focused tools that solve specific problems: dependency injection, lifecycle management, and context variables. Use what you need, ignore what you don't.

**Zero lock-in.** Your business logic is just functions with decorators. Remove Stratae anytime by replacing `Depends()` with actual values. No container to untangle, no framework to escape.

**Built for performance.** Stratae keeps overhead minimal through straightforward design:

- Analyze dependencies once at decoration time
- No runtime introspection or provider lookups
- Direct function calls with minimal indirection

The result is a system that minimizes overhead and stays out of your way. If you're using a system that has dependency injection, we encourage you to test it yourself. Change one small piece to use Stratae and see if it works for you.

## Features

### Dependency Injection

Dependency injection in Stratae uses familiar decorator syntax that works with any callable. Use this to send values, objects, or anything into a function.

```python
from stratae.depends import Depends, inject

def get_config():
    return {"env": "dev", "mode": "strict"}

# Decorate the function with inject to resolve dependencies
@inject
# Use Depends(...) to mark parameters for injection
def endpoint(config: dict[str, str] = Depends(get_config)):
    print(f"Environment: {config['env']}, Mode: {config['mode']}")

endpoint()
# Environment: dev, Mode: strict
```

### Lifecycle Management

Use lifecycle management when you want to cache objects or guarantee resource cleanup for context managers. With managed resources, everything is cleaned up automatically at the end of a lifecycle scope.

```python
# Set up the lifecycle with your application scopes
lifecycle = Lifecycle(['application', 'request'])

# Lifecycle will cache the yielded value and return it for all calls within a request
@lifecycle.cache('request')
# Mark get_session as a contextmanager that will be auto-entered to get the session
@managed
def get_session():
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

# Set up your lifecycle boundaries
with lifecycle.start('application'):
    with lifecycle.start('request'):
        # Session is created at first call and cached automatically
        # All get_session calls in this request will return the same session
        db = get_session()
        db.users.create_user('John')
    with lifecycle.start('request'):
        # New request, new session
        db = get_session()
```

### Context Variables

To enable decoupled systems, Stratae uses context variables for setting values that need to be shared among components. This is particularly useful for setting values at runtime that are needed deep in dependency chains. Change values at runtime, or even whole behavior, without needing to thread parameters or manipulate overrides.

```python
from stratae.context import Context

lifecycle = Lifecycle('request')
user_id = Context[int]("user_id")

@lifecycle.cache('request')
@inject
def get_current_user(uid: int = Depends(user_id)) -> User:
    return fetch_user(uid)

@inject
def create_post(
    content: str,
    user: User = Depends(get_current_user),
) -> Post:
    return Post(author=user, content=content)

with lifecycle.start('request'), user_id.use(123):
    post = create_post("Hello world!")
```

### Async Support

Stratae is fully async compatible. Injection natively works with sync or async functions. Lifecycle offers versions for sync and async handling of resources.

```python
from stratae.depends import Depends, inject
from stratae.lifecycle import AsyncLifecycle

lifecycle = AsyncLifecycle(['application', 'request'])

@lifecycle.cache('application')
async def get_database() -> Database:
    return await Database(url="postgresql://...")

@inject
async def create_user(
    name: str,
    db: Database = Depends(get_database),
) -> User:
    return await db.users.create(name=name)

# Use anywhere: APIs, CLIs, workers, tests
async with lifecycle.start('application'):
    async with lifecycle.start('request'):
        user = await create_user("Alice")
```

### Easy Testing

With no complex configuration, testing functions decorated with Stratae is easy. The function signature isn't changed, just pass in the mocks you need.

```python
@inject
def create_user(name: str, db = Depends(get_db)):
    db.user.create(name=name)

# Use normally
create_user('Steve')

# Test
create_user('Jason', db=MockDB())
```

### Framework Agnostic

Stratae doesn't have a complex framework to configure or objects to pass around. Write your business logic once with injection, then simply call those functions anywhere.

```python
# Business logic - framework-independent
@inject
async def create_user(
    name: str,
    db: Database = Depends(get_database),
) -> User:
    return await db.users.create(name=name)

# FastAPI
@app.post("/users")
async def api_create(name: str):
    return await create_user(name)

# CLI
@click.command()
def cli_create(name: str):
    asyncio.run(create_user(name))

# Tests
async def test_create():
    user = await create_user("Alice", db=mock_db)
```

### Simple Integrations

The design of Stratae means integrating with other tools or frameworks is typically easy. For FastAPI, an ASGI middleware that starts the request lifecycle is enough to add Stratae's lifecycle management.

```python
from fastapi import FastAPI
from stratae.integrations import RequestLifecycleMiddleware
from stratae.lifecycle import AsyncLifecycle, managed


app = FastAPI()
lifecycle = AsyncLifecycle(['request'])

# Add the middleware that starts a lifecycle request
app.add_middleware(RequestLifecycleMiddleware, lifecycle, 'request')

# Everything that needs the session will get the same session
@lifecycle.cache('request')
@managed
async def get_session():
    session = AsyncSession()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

@inject
async def create_user(
    name: str,
    # Using Stratae Depends
    db: Session = Depends(get_session)
):
    await db.users.create(name=name)

# Every request will get a new session
@app.post('/users')
async def post_user(name: str):
    await create_user(name)
```

## Documentation

More detailed documentation will be published soon.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

Before contributing, please:

1. Check for open issues or open a new issue to start a discussion
2. Fork the repository on GitHub
3. Install development dependencies with `pip install -e ".[dev]"`
4. Run pre-commit hooks with `pre-commit install`
5. Make your changes following the project's coding style
6. Write tests that cover your changes
7. Update documentation if needed
8. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
