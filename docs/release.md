# Release Process

This project uses an automated release script (`scripts/release.py`) to manage
version bumps, changelog generation, and roadmap updates.

## How to Cut a Release

1. Main branch is protected. Create a release branch off `main`:

```bash
git switch -c release/v<VERSION>

```

2. Run a dry-run check to preview changes:
```bash
python scripts/release.py <VERSION>
# Example: python scripts/release.py 0.4.0

```

3. Execute the release script:
```bash
python scripts/release.py <VERSION> --execute

```

### What the Release Script Automates

* Bumps `version` in `Cargo.toml`
* Bumps `version` in `pyproject.toml`
* Runs `git-cliff` to regenerate `CHANGELOG.md`
* Replaces `[unreleased]` with `[<VERSION>] - YYYY-MM-DD` in `docs/roadmap.md`
* Creates a local `chore(release): bump version to <VERSION>` Git commit

4. Push the release commit and tags:
```bash
git push -u origin release/vx.x.x

```

5. Do Pull Request and Merge on github

6. Switch to `main` branch

```bash
git switch main && git pull

```

7. Tag the release commit and push the tag:

```bash
git tag v<VERSION>
git push origin v<VERSION>

```
