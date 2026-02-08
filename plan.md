# Rust-First Harness Plan

## Status
- Date: 2026-02-07
- Branch: `rust-migration`
- Overall state: `M0`-`M3` complete, `M4`-`M5` in progress.
- Current focus:
  - Finish release/distribution hardening around `ragmail` + `ragmail-py`.
  - Finalize Rust pipeline UX parity (live stage display + streamed Python progress + logs).
  - Track remaining robustness items in `todo.md`.
- Maintenance note: keep `/Users/mo/.codex/worktrees/edd5/ragmail/todo.md` updated with prioritized risk/robustness items while milestones progress.

## Milestones

### M0 - Contract Reset and Plan Baseline
- Define new harness contract: Rust is the only public CLI entrypoint.
- Confirm Python boundary: only model warmup, vectorize, ingest, search/API helpers.
- Recreate active planning/tracking docs (`plan.md`, `todo.md`) with current status.

### M1 - Rust Entrypoint and Stage Orchestration
- Rename Rust binary to `ragmail`.
- Make Rust pipeline stage model canonical (`model,split,preprocess,vectorize,ingest`).
- Remove standalone `index` pipeline stage and produce `mbox_index.jsonl` during `preprocess`.
- Add Rust passthrough commands for Python-backed features (`search`, `stats`, `dedupe`, `serve`, etc.).

### M2 - Python Bridge Packaging Boundary
- Add dedicated Python bridge command surface for Rust invocation (`py model|vectorize|ingest`).
- Add PyInstaller build flow for bridge executable (`ragmail-py`).
- Make Rust bridge resolution prefer packaged `ragmail-py`, with source-dev fallback.

### M3 - Developer Build UX
- Keep `just bootstrap` as one-step local setup.
- Ensure `.venv/bin/ragmail` resolves to Rust binary after bootstrap.
- Keep Python env bootstrap robust (`uv` preferred, `venv`+`pip` fallback).

### M4 - Versioned Release Artifacts
- Emit versioned artifacts under `releases/`.
- Bundle Rust CLI + Python bridge binary for dependency-free execution.
- Update Linux package/homebrew tooling to distribute the new binary names.
- Preserve strict release checks: clean git tree, lint/tests pass, version-tag consistency.

### M5 - Docs, Skills, AI State, and Hardening
- Update README + docs to Rust-first harness and packaging model.
- Update AGENTS/skill docs where behavior changed.
- Update `ai-state.md` for stage/runtime/layout invariants.
- Expand tests for new CLI/bridge/release contracts.

## Progress Log

### 2026-02-07
- Replaced stale root planning files with a new Rust-entrypoint migration plan.
- Established milestone sequence (`M0`..`M5`) and explicit tracking rules.
- Began implementing `M1` (Rust binary + stage orchestration updates).
- Completed Rust entrypoint migration:
  - Rust CLI renamed to `ragmail`.
  - Pipeline stage contract normalized to `model,split,preprocess,vectorize,ingest`.
  - Preprocess now emits `mbox_index.jsonl` in one pass.
  - Added Rust passthrough handling for Python-owned CLI commands.
- Completed Python bridge boundary updates:
  - Added `py model` bridge command.
  - Added bridge resolution preference for packaged/sibling `ragmail-py`.
  - Added PyInstaller build script and entrypoint (`build-python-bridge.sh`).
- Completed dev workflow hardening:
  - `just bootstrap` now links `.venv/bin/ragmail` to Rust debug binary.
  - Bootstrap fallback made resilient in sandbox/offline-like environments.
- Advanced release tooling migration:
  - Artifacts default to `releases/`.
  - Tarballs include `ragmail` + `ragmail-py`.
  - Debian/homebrew naming moved from `ragmail-rs` to `ragmail`.
  - Release CI workflow updated for per-platform Rust + bridge builds.
- Updated AI/docs/tests to new contracts:
  - `ai-state.md` refreshed for current architecture.
  - `AGENTS.md`, `docs/DESIGN.md`, `docs/developers.md`, `docs/release.md` updated.
  - Rust and Python suites passing after migration updates.
- Added Rust pipeline UX parity work (in progress):
  - Rust `pipeline` now prints Python-style header + live stage area with colors/spinners/progress text.
  - Rust now renders `model` stage as downloaded/cache bytes instead of `0/1`.
  - Python bridge commands emit streaming JSON events (`event=progress|compaction`) before final result JSON.
  - Rust bridge execution now streams child stdout/stderr, parses event JSON incrementally, updates UI live, and logs event lines to stage logs.
  - Added bridge contract tests for streamed progress/compaction events.
  - Validation run: `cargo test -p ragmail-cli`, `cargo clippy -p ragmail-cli -- -D warnings`, `pytest python/tests/test_python_bridge_contracts.py`, `pytest python/tests/test_rust_pipeline_bridge.py`.
- Continued UX fixes for stalled-looking stages:
  - Added in-loop Rust progress callbacks for `split` and `preprocess` so large single files update continuously.
  - Added `model` elapsed heartbeat in progress events when download byte deltas stay flat due to cache hits.
  - Verified live increments with a larger synthetic MBOX run and re-ran Rust/Python test suites + clippy.
- Closed additional pipeline UX/bridge correctness gaps:
  - `model` stage now displays status `downloading` while active (not `running`).
  - Added explicit `starting` startup display text for `split` and `preprocess` before first measurable progress event.
  - Fixed Rust -> Python Click boolean forwarding for bridge stages to use flags (`--resume`/`--no-resume`) instead of invalid extra args (`--resume true|false`).
  - Added regression assertion in Rust CLI tests to prevent boolean-flag regressions.
  - Validation run:
    - `cargo test -p ragmail-cli`
    - `cargo clippy -p ragmail-cli -- -D warnings`
    - `.venv/bin/python -m pytest python/tests/test_python_bridge_contracts.py python/tests/test_rust_pipeline_bridge.py`
- Improved vectorize/ingest startup visibility:
  - `vectorize` and `ingest` now enter `starting` status before `running`.
  - Rust stage UI now consumes `startup_text` from bridge progress events for these stages.
  - Python bridge now emits explicit startup progress events for `py vectorize` and `py ingest`.
  - Vectorize runtime now emits heartbeat progress during embedding-model load and first-batch buildup so long initialization does not appear hung.
  - Validation run:
    - `cargo test -p ragmail-cli`
    - `cargo clippy -p ragmail-cli -- -D warnings`
    - `.venv/bin/python -m pytest python/tests/test_python_bridge_contracts.py python/tests/test_rust_pipeline_bridge.py`
