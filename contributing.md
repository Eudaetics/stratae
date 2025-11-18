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

4. Install the package in development mode with development dependencies:

   ```bash
   pip install -e ".[dev]"
   ```

5. Install pre-commit hooks:

   ```bash
   pre-commit install
   ```

#### Making Changes

1. Make your changes to the code
2. Write or update tests for your changes
3. Ensure your code follows the project's style guidelines:
   - Code is formatted with Ruff
   - Passes all Ruff linting checks
   - Type hints are used appropriately and checked with Pyright
4. Run the tests to make sure your changes don't break existing functionality:

   ```bash
   pytest
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
- **Type Hints**: Use type hints throughout the codebase
- **Docstrings**: Use [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)

### Git Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters
- Reference issues and pull requests after the first line

## Project Structure

```stratae/       # Main package
├── __init__.py              # Package exports
├── submodule1/              # Organize code into modules
│   └── ...                  # Module implementation
├── submodule2/
│   └── ...
└── ...
tests/                      # Test directory
├── __init__.py
├── unit/
    └── test_submodule1.py  # Tests for submodule1
└── ...
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
