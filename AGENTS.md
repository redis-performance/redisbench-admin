# Release Process

1. Bump the version using `poetry version <patch|minor|major>`
2. Create a pull request and wait for it to be merged
3. Create a new release on GitHub with `gh release create v$(poetry version) --generate-notes`
4. Wait for GitHub Actions to publish the package to PyPI

# Git Commits

- commit messages strictly based on staged changes
- explain the "why" and the "how" rather than the "what". If not evident, ask
- first line (title) must be concise and follow previous commits format
- details provided in list points ("- ...")
- text should be ready to be copied and pasted, in a single block
- suggest a branch name, in the format used on the repo
- validate documentation is up to date
