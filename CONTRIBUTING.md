# Contributing

We treat this repo as "Open Source" within Redis: anyone who clears the bar below is welcome to contribute.

## Local setup

```bash
git clone git@github.com:redis-performance/redisbench-admin.git
cd redisbench-admin

# Initialise submodules
git submodule update --init --recursive

# Install Poetry (if not already installed)
pip install poetry

# Install all dependencies (including dev extras) from the lock file
poetry install

# Alternatively, install dev dependencies directly
pip install -r dev_requirements.txt
```

The package is declared in `pyproject.toml` and managed with [Poetry](https://python-poetry.org/).  
Python 3.10 or later is required (CI tests 3.11–3.14).

## Branch naming

```
<type>/<short-description>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Example: `feat/add-pipeline-mode`

## Coding standards

- Keep changes focused; one logical change per PR.
- Follow the conventions already present in the codebase (formatting, naming, error handling).
- No dead code, no commented-out blocks.
- Code is formatted with `black` and linted with `flake8`. Run compliance checks before opening a PR:

```bash
tox -e compliance        # check formatting and linting
tox -e format            # auto-fix formatting with black
```

## Submitting changes

1. Fork or create a branch from `master` following the branch naming convention above.
2. Make your changes with clear, atomic commits. Commit messages should explain the *why* and the *how*; the title must be concise and match the style of existing commits.
3. Open a pull request against `master` with a descriptive title and summary.
4. Address review comments promptly; force-push to the same branch to update the PR.

## Testing

- All new behaviour must be covered by tests.
- Existing tests must pass: run the test suite locally before opening a PR.
- Coverage should not decrease.

The integration suite uses Docker-backed sidecars (Redis Stack for `RTS_PORT`). Run tests via `tox`:

```bash
# Full suite (compliance + integration tests)
tox

# Compliance only (black + flake8)
tox -e compliance

# Integration tests only
tox -e integration-tests

# Run a specific test file
tox -- tests/test_defaults_purpose_built_env.py

# Run a specific test file with verbose logging
tox -- -vv --log-cli-level=INFO tests/test_run.py

# Keep the Redis-Stack Docker container alive between runs (faster iteration)
tox --docker-dont-stop=rts_datasink -- -vv --log-cli-level=INFO tests/test_defaults_purpose_built_env.py
```

You can also run pytest directly (skips the Docker sidecar setup — some tests will be skipped or fail):

```bash
poetry run pytest --cov=redisbench_admin --cov-report=term-missing -ra
```

Or use the Makefile shortcuts:

```bash
make compliance          # run black + flake8 checks
make format              # auto-fix formatting
make test                # run pytest with coverage
make all                 # compliance + test
```

## Review process

- At least one maintainer approval is required before merge.
- CI must be green (the `Run Tests using tox` workflow runs on every PR against `master`).
- Maintainers may request changes or close PRs that don't meet the bar — this is normal and not personal.
