# ai-state.md (ragmail)

Purpose: compact AI handoff. Update whenever CLI contracts, stage ownership, workspace schema, or release tooling changes.

## Runtime ownership
- Rust owns harness/orchestration and MBOX-heavy stages:
  - `split`
  - `preprocess` (clean + index emission in one pass)
- Python only for:
  - `model` warmup
  - `vectorize`
  - `ingest`
  - query/API/LLM commands (forwarded from Rust passthrough)

## Stage contract
- Canonical default stage order: `split,preprocess,vectorize,ingest`
- Optional warmup stage: `model` (runs only when explicitly selected)
- Aliases accepted:
  - `download` -> `model`
  - `clean` -> `preprocess`
  - `index` -> `preprocess` (pipeline stage alias only)
- Invariant: `split/mbox_index.jsonl` is produced during `preprocess`; no standalone index stage in `ragmail pipeline`.
- Runtime UX invariant:
  - Rust `pipeline` owns live terminal UI (header + staged live area + spinner + durations + summary).
  - `split` + `preprocess` now emit in-loop progress updates (not only per-file completion), so large single-file runs visibly advance.
  - `model` progress text (`downloaded_bytes`/`cache_bytes`) is shown only when the optional `model` stage is selected.
  - `split`, `preprocess`, `vectorize`, and `ingest` use explicit `starting` status + startup text before first measurable progress callback.
  - Vectorize emits startup heartbeat progress while embedding provider initialization is in-flight.

## CLI boundary details
- Rust pipeline command entrypoint in `rust/ragmail-cli/src/main.rs`.
- Rust CLI internals are now split across cohesive modules:
  - `rust/ragmail-cli/src/display.rs` (live stage UI + pipeline header/summary rendering)
  - `rust/ragmail-cli/src/file_ops.rs` (stage parsing, checkpoint/file collection, counters, index-part merge)
  - `rust/ragmail-cli/src/logging.rs` (pipeline/stage log writing)
  - `rust/ragmail-cli/src/python_bridge.rs` (bridge command resolution, streaming JSON protocol, retry policy)
  - `rust/ragmail-cli/src/util.rs` (shared helpers)
  - `rust/ragmail-cli/src/tests.rs` (CLI unit/integration-style tests)
- Rust bridge execution order:
  - `RAGMAIL_PY_BRIDGE_BIN` override if set
  - sibling `ragmail-py` next to `ragmail`
  - repo `.venv/bin/ragmail-py` / `python/.venv/bin/ragmail-py`
  - fallback `python -m ragmail.cli`
- Rust forwards unknown subcommands to Python bridge (`query`, `stats`, `dedupe`, `serve`, etc.).
- Query command contract:
  - primary command is `ragmail query`
  - legacy `ragmail search` alias remains hidden for compatibility
  - RAG is enabled by default (`--rag` / `--no-rag`)
- Python bridge streaming protocol (for Rust UI):
  - progress lines: JSON with `event="progress"` and stage-specific counters.
  - ingest compaction lines: JSON with `event="compaction"`.
  - final line: JSON result object with `status="ok"` + stage output fields.
  - startup progress lines may include `startup_text` (displayed by Rust stage UI).
- Boolean bridge flags: Click-style booleans must be passed as flags (`--resume`/`--no-resume`) and never as extra positional values (`--resume true|false`).
- Rust bridge runner now streams child stdout/stderr incrementally, parses event JSON live, updates stage UI, and logs bridge lines to `logs/<stage>.log`.

## Workspace layout (stable)
`workspaces/<name>/`
- `inputs/`
- `split/` (`YYYY-MM.mbox`, `mbox_index.jsonl`)
- `clean/` (`*.clean.jsonl`)
- `spam/` (`*.spam.jsonl`)
- `reports/` (`*.summary`)
- `embeddings/` (`*.embed.db`)
- `db/email_search.lancedb`
- `logs/`
- `.checkpoints/` (`split-rs`, `preprocess-rs`, vectorize/ingest checkpoints)
- `workspace.json`, `state.json`

## Build/dev contracts
- `just bootstrap`:
  - bootstraps Python env (`just.d/scripts/bootstrap-python.sh`)
  - builds Rust workspace
  - links `.venv/bin/ragmail` -> `rust/target/debug/ragmail`
- Python bridge script in dev env: `.venv/bin/ragmail-py`

## Release contracts
- Version source of truth: root `VERSION`
- Release artifacts default output dir: `releases/`
- Local artifact build entrypoint: `just release <platform>` (`platform` default `host`)
- Supported local platform tokens:
  - `host`
  - `macos/amd64`, `macos/arm64`
  - `linux/amd64`, `linux/arm64`
- Maintainer cut command: `just release-cut <platform>` (`release-check` + build + `release-tag`)
- Tarballs include both binaries:
  - `ragmail`
  - `ragmail-py`
- Artifact build script runs a best-effort local runtime smoke probe (`ragmail version` + `ragmail query --help`) before packaging.
- Cross-platform local requests are rejected (PyInstaller bridge must be built on target OS/arch); use matching host or CI matrix.
- Linux package name/file pattern: `ragmail_<version>_<arch>.deb`
- Homebrew formula filename/class:
  - `ragmail.rb`
  - `class Ragmail < Formula`
- CI release workflow builds per-platform Rust + PyInstaller bridge binaries.

## Key files touched by harness migration
- Rust CLI entry + modules:
  - `rust/ragmail-cli/src/main.rs`
  - `rust/ragmail-cli/src/display.rs`
  - `rust/ragmail-cli/src/file_ops.rs`
  - `rust/ragmail-cli/src/logging.rs`
  - `rust/ragmail-cli/src/python_bridge.rs`
  - `rust/ragmail-cli/src/util.rs`
- Workspace refresh/state: `rust/ragmail-core/src/workspace/mod.rs`
- Rust stage/shared modules:
  - `rust/ragmail-core/src/stage.rs`
  - `rust/ragmail-clean/src/pipeline.rs`
  - `rust/ragmail-clean/src/header.rs`
  - `rust/ragmail-clean/src/mime.rs`
  - `rust/ragmail-clean/src/text.rs`
  - `rust/ragmail-clean/src/codec.rs`
  - `rust/ragmail-mbox/src/split.rs`
  - `rust/ragmail-mbox/src/stream.rs`
  - `rust/ragmail-index/src/build.rs`
  - `rust/ragmail-index/src/record.rs`
  - `rust/ragmail-index/src/query.rs`
- Rust crate bin name/version: `rust/ragmail-cli/Cargo.toml`, `rust/Cargo.toml`
- Python bridge commands: `python/lib/ragmail/cli.py`
- Python Rust binary resolution compatibility: `python/lib/ragmail/pipeline.py`
- Bootstrap/release scripts:
  - `just.d/scripts/bootstrap-python.sh`
  - `just.d/scripts/link-dev-cli.sh`
  - `just.d/scripts/build-python-bridge.sh`
  - `just.d/scripts/build-release-artifacts.sh`
  - `just.d/scripts/package-deb.sh`
  - `just.d/scripts/generate-homebrew-formula.sh`
  - `just.d/scripts/release-publish-assets.sh`
  - `just.d/scripts/publish-homebrew-tap.sh`
  - `just.d/scripts/release-check.sh`
  - `just.d/scripts/release-ci-dry-run.sh`

## Current verification snapshot
- `just lint` -> pass
- `just test-all` -> pass (`118 passed, 6 skipped` Python; all Rust tests green)
- `./just.d/scripts/release-ci-dry-run.sh` -> pass
- `just release host` -> pass (artifact + `SHA256SUMS`; passthrough smoke can warn under restricted sandboxes)
- Additional post-UX checks:
  - `cargo test -p ragmail-cli` -> pass
  - `cargo clippy -p ragmail-cli -- -D warnings` -> pass
  - `.venv/bin/python -m pytest python/tests/test_python_bridge_contracts.py python/tests/test_rust_pipeline_bridge.py` -> pass
