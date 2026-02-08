# Release Guide

This repo uses `VERSION` as the source of truth for Rust and Python versions.

## Build a runnable distribution (local)

Primary command:

```bash
just release <os/arch>
```

Examples:

```bash
# Build for current machine
just release host

# Explicit target spelling
just release macos/arm64
just release macos/amd64
just release linux/arm64
just release linux/amd64
```

Behavior:
- Builds `ragmail` (Rust) and `ragmail-py` (PyInstaller bridge).
- Runs a best-effort local runtime smoke probe before packaging (disable with `RAGMAIL_RELEASE_RUNTIME_SMOKE=0`).
- Writes artifacts to `releases/` by default.
- Produces `SHA256SUMS`.
- Produces Linux `.deb` on Linux hosts when `dpkg-deb` is available.

Important:
- `ragmail-py` must be built on the target OS/arch.
- Cross-platform requests from a non-matching host fail with a clear remediation message.

## Maintainer release flow (checks + tag)

1. Ensure your tree is clean.
2. Run release checks:

```bash
just release-check
```

Checks include:
- clean git tree
- Rust fmt + clippy (`-D warnings`) + Rust tests
- Python tests from `.venv`
- release binary build and binary version match (`ragmail version == VERSION`)
- PyInstaller bridge build and version match (`ragmail-py --version` matches `VERSION`)

3. Build artifacts:

```bash
just release host
```

4. Tag release:

```bash
just release-tag
```

Or run all three in order:

```bash
just release-cut host
```

Optional local smoke validation (artifact naming/checksum/formula path):

```bash
just release-smoke
```

Optional local CI-equivalent dry run (release publish + tap update using a temporary local tap repo):

```bash
just release-ci-dry-run
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
- Homebrew formula (`ragmail.rb`)

Each release tarball includes:
- `ragmail` (Rust entrypoint CLI)
- `ragmail-py` (standalone Python bridge built by PyInstaller)

Linux `arm64` builds run on native ARM runners (`ubuntu-24.04-arm`) so both binaries are host-native.

## Homebrew tap automation

To auto-publish formula updates on release:
- Set Actions variable `HOMEBREW_TAP_REPO` to `<owner>/<tap-repo>`.
- Set Actions secret `HOMEBREW_TAP_TOKEN` with push rights to that repo.

The workflow writes `Formula/ragmail.rb` in the tap repo.
