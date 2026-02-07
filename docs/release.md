# Release Guide

This repo uses `VERSION` as the source of truth for Rust and Python versions.

## Local release flow

1. Ensure your tree is clean.
2. Run release checks:

```bash
just release-check
```

Checks include:
- clean git tree
- Rust fmt + clippy (`-D warnings`) + Rust tests
- Python tests from `.venv`
- release binary build and binary version match (`ragmail-rs version == VERSION`)

3. Build host artifacts:

```bash
just release-artifacts
```

Optional local smoke validation (artifact naming/checksum/formula path):

```bash
just release-smoke
```

Optional local CI-equivalent dry run (release publish + tap update using a temporary local tap repo):

```bash
just release-ci-dry-run
```

4. Tag release:

```bash
just release-tag
```

Or run all in order:

```bash
just release
```

## Version bump

```bash
just bump-patch
```

This command refuses to run on a dirty tree, increments patch in `VERSION`, and syncs:
- `python/pyproject.toml`
- `python/lib/ragmail/__init__.py`
- `rust/Cargo.toml` workspace version

## CI release packaging

Pushing tag `vX.Y.Z` triggers `.github/workflows/release.yml`.

Generated assets:
- macOS tarballs (`amd64`, `arm64`)
- Linux tarballs (`amd64`, `arm64`)
- Linux `.deb` packages (`amd64`, `arm64`)
- `SHA256SUMS`
- Homebrew formula (`ragmail-rs.rb`)

Linux `arm64` builds are cross-compiled on `ubuntu-latest` with `gcc-aarch64-linux-gnu`, so release packaging does not require a dedicated ARM GitHub runner.

## Homebrew tap automation

To auto-publish formula updates on release:
- Set Actions variable `HOMEBREW_TAP_REPO` to `<owner>/<tap-repo>`.
- Set Actions secret `HOMEBREW_TAP_TOKEN` with push rights to that repo.

The workflow writes `Formula/ragmail-rs.rb` in the tap repo.
