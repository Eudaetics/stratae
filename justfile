format:
    uv run ruff format .

lint:
    uv run ruff check --fix .

typecheck:
    uv run pyright

test:
    uv run pytest

test-rabbitmq:
    uv run pytest -m rabbitmq --no-cov

test-lowest:
    uv run --isolated --resolution lowest-direct --all-extras pytest --no-cov

radon-cc:
    uv run radon cc stratae -n B

radon-mi:
    uv run radon mi stratae -n B

no-cover-check:
    ! grep -rn --include='*.py' 'pragma: no cover' stratae

type-ignore-check:
    ! grep -rn --include='*.py' 'type: ignore' stratae

pyright-ignore-check:
    ! grep -rn --include='*.py' 'pyright: ignore' stratae

check: format lint typecheck test no-cover-check type-ignore-check pyright-ignore-check radon-cc radon-mi
