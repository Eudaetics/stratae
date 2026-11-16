# Contributing to Stratae

Thank you for considering contributing to Stratae! This document provides guidelines and instructions for contributing to this project.

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](code_of_conduct.md).

## How Can I Contribute?

### Reporting Bugs

Before submitting a bug report:

- Check the [issue tracker](https://github.com/Eudaetics/stratae/issues) to see if the bug has already been reported
- Make sure you're using the latest version of the package

When submitting a bug report:

- Use a clear and descriptive title
- Describe the exact steps to reproduce the problem
- Describe the behavior you observed and what behavior you expected
- Include details about your environment (OS, Python version, package version)
- Include any relevant logs or error messages

### Suggesting Enhancements

Enhancement suggestions are welcome! When submitting an enhancement suggestion:

- Use a clear and descriptive title
- Provide a detailed description of the proposed functionality
- Explain why this enhancement would be useful to Stratae users
- Include any relevant examples or use cases

### Code Contributions

#### Setting Up Your Development Environment

1. Fork the repository on GitHub
2. Clone your fork locally:

   ```bash
   git clone https://github.com/your-username/stratae.git
   cd stratae
   ```

3. Create a branch for your changes:

   ```bash
   git checkout -b your-branch-name
   ```

4. Install the package with its development dependencies and every extra, as CI does:

   ```bash
   uv sync --all-extras --all-groups
   ```

5. Install pre-commit hooks:

   ```bash
   uv run pre-commit install
   ```

#### Making Changes

1. Make your changes to the code
2. Write or update tests for your changes
3. Ensure your code follows the project's style guidelines:
   - Code is formatted with Ruff
   - Passes all Ruff linting checks
   - Type hints are used throughout, and pass Pyright in strict mode
4. Run every check CI runs. It formats and lints, type-checks, checks the public API's type completeness, runs the tests at 100% coverage, and checks complexity:

   ```bash
   just check
   ```

5. If you changed the RabbitMQ integration, run its tests against a live broker. They read `RABBITMQ_URL`, by default `amqp://guest:guest@localhost/`:

   ```bash
   just test-rabbitmq
   ```

6. If you changed documentation or a docstring's example, run the examples and build the docs:

   ```bash
   just test-docs
   just docs
   ```

#### Submitting a Pull Request

1. Commit your changes:

   ```bash
   git add .
   git commit -m "Your detailed commit message"
   ```

2. Push to your fork:

   ```bash
   git push origin your-branch-name
   ```

3. Submit a pull request through the GitHub website

When submitting a pull request:

- Use a clear and descriptive title
- Reference any related issues
- Describe what changes you've made and why
- Include any breaking changes
- Mention any necessary migration steps

#### Pull Request Review Process

1. Maintainers will review your pull request
2. Automated checks must pass (tests, linting, etc.)
3. You may need to make additional changes based on feedback
4. Once approved, your pull request will be merged

## Style Guidelines

### Code Style

- **Python**: Follow PEP 8 guidelines with the modifications specified in our setup
- **Formatting**: We use Ruff for code formatting
- **Linting**: We use Ruff for linting
- **Type Hints**: Use type hints throughout the codebase. Pyright runs in strict mode, and the package has no type or coverage suppressions
- **Docstrings**: Write them in MyST Markdown, with `:param:`, `:returns:` and `:raises:` fields and runnable examples, as in `stratae/checks.py`

### Git Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters
- Reference issues and pull requests after the first line

## Project Structure

```text
stratae/                # The package
├── backoff.py          # Backoff values for retries and reconnects
├── callables.py        # Telling what calling a callable gives
├── checks.py           # Guard checks
├── codegen/            # Building generated wrappers
├── context.py          # Context variables
├── depends/            # Dependency injection
├── events/             # Events, buses, the emit pipeline and the outbox
├── integrations/       # FastAPI, Starlette, ASGI, RabbitMQ and OpenTelemetry
├── lifecycle/          # Scopes, cached resources and scope middleware
└── middleware.py       # The (call, next) middleware chain
tests/
├── unit/               # Tests for each module
├── integration/        # Tests against frameworks and a live broker
└── e2e/                # End-to-end tests
docs/                   # Sphinx documentation and tutorials
benchmarks/             # Benchmarks for performance changes
```

## Development Workflow

### Creating a New Feature

1. Create a new branch from `main`
2. Implement the feature with tests
3. Submit a pull request

### Fixing a Bug

1. Create a new branch from `main`
2. Write a test that reproduces the bug
3. Fix the bug
4. Submit a pull request

## Questions or Need Help?

If you have questions about contributing, please:

- Open a discussion on GitHub
- Contact the maintainers at [social@eudaetics.com]

Thank you for contributing to Stratae!
