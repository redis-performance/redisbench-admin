# Release Process

1. Bump the version using `poetry version <patch|minor|major>`
2. Create a pull request and wait for it to be merged.
   - Branch protection on `master` requires an approving review. GitHub
     forbids approving your own PR, so a version-bump PR authored by the
     releaser must either be reviewed by someone else or merged with
     `gh pr merge --admin` (admin override).
3. Create a new release on GitHub with `gh release create v$(poetry version) --generate-notes`
4. Wait for GitHub Actions to publish the package to PyPI

# Testing

Use `tox` — the integration suite requires a docker-backed Redis-Stack
sidecar (exposes `RTS_PORT`). Bare `poetry run pytest` silently skips or
fails the RTS-dependent tests.

- Full suite: `tox`
- Compliance (black + flake8): `tox -e compliance`
- Format in place: `tox -e format`
- Scoped run: `tox -- tests/test_foo.py -v`
- Persist the docker RTS container across runs:
  `tox --docker-dont-stop=rts_datasink -- -vv --log-cli-level=INFO tests/test_foo.py`

# Git Commits

- commit messages strictly based on staged changes
- explain the "why" and the "how" rather than the "what". If not evident, ask
- first line (title) must be concise and follow previous commits format
- details provided in list points ("- ...")
- text should be ready to be copied and pasted, in a single block
- suggest a branch name, in the format used on the repo
- validate documentation is up to date
