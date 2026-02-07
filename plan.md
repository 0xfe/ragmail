# ragmail Rust-First Migration Plan

## Status
- Date: 2026-02-06
- Branch: `rust-migration`
- State: `M1`-`M6` complete; `M7` and `M8` in progress.
- Current focus:
  - Validate distribution pipeline end-to-end (`M7`).
  - Expand corpus-scale robustness verification (`M8`).
- Maintenance note: keep `/Users/mo/.codex/worktrees/edd5/ragmail/todo.md` updated with improvements, risks, and follow-up opportunities.

## Milestones

### M0 - Design & Contracts (complete)
- Freeze workspace/stage contracts and schema invariants.
- Define Rust/Python boundary: Rust for MBOX-heavy stages, Python for embeddings/LLM/search.

### M1 - Rust Workspace Scaffold (complete)
- Rust workspace crates and version plumbing.
- `just`-based build/lint/test/release scaffolding.

### M2 - Rust Split/Index Core (complete)
- Streaming split with checkpointed resume.
- Rust index build with per-part checkpointing + merge.
- Robust interruption/resume and idempotency coverage.

### M3 - Rust Clean Stage (complete)
- Rust cleaner with summary output and tested contract behavior.
- Historical fixture coverage for edge cases.

### M4 - Rust Pipeline Orchestrator (complete)
- Stage state/log/duration parity.
- Refresh/archive semantics and failure-state recording.

### M5 - Python Boundary Hardening (complete)
- Stable Python bridge commands for vectorize/ingest.
- Retry/backoff and failure classification coverage.

### M6 - Build/Release/Versioning (complete)
- Root `VERSION` source of truth.
- One-command patch bump and strict release checks.
- Annotated version tagging and release artifact scripts.

### M7 - Distribution (in progress)
- Tag-driven release artifacts for macOS and Linux.
- Linux arm64 cross-compile path.
- Homebrew formula and tap publish automation.
- Remaining: real-tag and real-credential validation.

### M8 - Docs/AI State/Hardening (in progress)
- Rust-only docs/skills/tests alignment.
- Corpus-scale robustness and benchmark floor calibration.

## Architecture (Current)
- Rust-backed stages only: `split`, `preprocess` (clean + index in one pass).
- Python stages: `vectorize`, `ingest`, `search`, API/LLM.
- Legacy Python split/clean/index code paths removed.
- Legacy stage-selection flags removed from CLI.

## Next Steps
1. Validate CI release workflow on real release tags.
2. Validate Homebrew tap automation with real credentials.
3. Validate Linux arm64 artifacts on real arm64 hosts.
4. Raise benchmark floor from smoke level after baseline collection.

## Progress Log

### 2026-02-06
- Fixed split-stage file descriptor exhaustion on large, multi-year mailboxes by capping open monthly writers and evicting+flushing least-recently-used handles in `ragmail-mbox`.
- Hardened split checkpoint semantics by flushing active writers before checkpoint writes to avoid checkpoint-ahead-of-disk data-loss windows on interruption.
- Added regression coverage for low writer limits across many month partitions.
- Refactored pipeline stage model to `model -> split -> preprocess -> vectorize -> ingest`.
- Removed standalone pipeline `index` stage; `preprocess` now writes per-file index parts and merges to `split/mbox_index.jsonl`.
- Added preprocess parallelism control (`--preprocess-workers`, `EMAIL_SEARCH_PREPROCESS_WORKERS`) and defaulted to safe single-worker behavior.
- Fixed Rust clean panic on long blank-line runs (`u8` overflow), and reduced per-line allocations in body normalization.
- Updated Python and Rust tests for preprocess semantics; validated full Rust/Python suites and rustfmt/clippy gates.
- Updated docs/skills references from `index`/`clean` stage language to `preprocess`.
- Removed legacy Python split/clean modules and fallback branches.
- Removed legacy CLI toggles for Rust stage selection.
- Simplified Python index helper to read-only lookup functions.
- Updated tests to Rust-only contracts and robustness checks.
- Updated README/skills/design docs and AI state for the Rust-only stage model.
- Reworked benchmark tooling and CI benchmark gate to Rust-only throughput floor.
- Moved Python project under `python/`:
  - `lib/` -> `python/lib/`
  - `tests/` -> `python/tests/`
  - `pytest.ini` -> `python/pytest.ini`
  - `pyproject.toml` -> `python/pyproject.toml`
  - `uv.lock` -> `python/uv.lock`
  - `requirements.txt` -> `python/requirements.txt`
  - Updated pytest config and `uv`/CI/just tooling to use the `python/` project layout.
- Fixed Python-to-Rust repo-root resolution after project relocation by resolving root via `rust/Cargo.toml` discovery in `python/lib/ragmail/pipeline.py`.
- Updated ragmail skill DB reference docs to `python/lib/ragmail/*` paths.
- Updated Rust/Python fixture references to `python/tests/fixtures` after test relocation.
- Rewrote user-facing docs set for the Rust-first architecture:
  - Simplified `README.md` with clear user flow sections.
  - Added `docs/pipeline.md` deep dive with stage behavior and practical command recipes.
  - Reworked `docs/DESIGN.md` for high-level technical architecture.
  - Added `docs/developers.md` with build/test/debug and iteration guidance.
- Fixed bootstrap UX and prebuild behavior:
  - `just bootstrap` now uses a shared root `.venv` and builds the Rust workspace.
  - Added robust Python bootstrap fallback script (`just.d/scripts/bootstrap-python.sh`).
  - Python pipeline now prefers prebuilt `rust/target/debug/ragmail-rs` before `cargo run`.
- Validated relocation with full test gates:
  - `./just.d/scripts/test-python.sh` -> 118 passed, 6 skipped
  - `cargo test --manifest-path rust/Cargo.toml --workspace` -> all passed
  - `just lint` (`cargo fmt --check` + `cargo clippy -D warnings`) -> passed
