# ai-state.md (ragmail)

Purpose: dense AI-only brief. Keep updated when CLI/stages/schema/workspace layout or build/release flows change.

Snapshot (2026-02-06)
- Active branch: `rust-migration`.
- Rust toolchain target: `1.93.0` (`rustfmt`, `clippy`, `cargo` required).
- Python project root: `python/` (`python/pyproject.toml`, `python/uv.lock`, `python/lib/ragmail`).
- Python tests root: `python/tests` (configured by `python/pytest.ini`).
- Pipeline direction is now single-path for MBOX heavy stages:
  - Rust-backed only: `split`, `preprocess` (includes index generation).
  - Python-only: `vectorize`, `ingest`, `search`, API/LLM interfaces.
- Legacy Python split/clean/index implementations removed from active path.
- Legacy CLI flags removed: `--rust-split-index`, `--rust-clean`.

## Current architecture

### Python entrypoints
- CLI: `python/lib/ragmail/cli.py`.
  - `pipeline` command orchestrates stages.
  - `py vectorize` and `py ingest` are bridge contract commands used by Rust orchestration.
- Pipeline orchestrator: `python/lib/ragmail/pipeline.py`.
  - Always calls Rust helpers for split/preprocess.
  - Resolves repo root by searching parent directories for `rust/Cargo.toml`.
  - Keeps Python vectorize/ingest options and behavior.
- Index read helpers: `python/lib/ragmail/mbox_index.py` (`find_in_index`, `read_message_bytes`).

### Rust crates
- Workspace: `rust/Cargo.toml`
- CLI binary: `rust/ragmail-cli` (`ragmail-rs`)
- Core/workspace contracts: `rust/ragmail-core`
- MBOX stream/split: `rust/ragmail-mbox`
- Index: `rust/ragmail-index`
- Clean: `rust/ragmail-clean`

## Workspace layout
`workspaces/<name>/`
- `inputs/`
- `split/` (`YYYY-MM.mbox`, `mbox_index.jsonl`)
- `clean/` (`*.clean.jsonl`)
- `spam/` (`*.spam.jsonl`)
- `reports/` (`*.mbox.summary`)
- `embeddings/` (`*.embed.db`)
- `db/email_search.lancedb`
- `logs/`
- `.checkpoints/`
- `workspace.json`, `state.json`

## Stage contract
Default stage order: `model,split,preprocess,vectorize,ingest`
- `split`: Rust split command with checkpointed resume (`.checkpoints/split-rs`).
  - Writer handles are bounded (LRU eviction + flush) to avoid OS `ulimit -n` failures on many-month datasets.
  - Writers are flushed before checkpoint writes for stronger resume durability.
- `preprocess`: Rust clean outputs written per split mbox, with index part outputs in `.checkpoints/preprocess-rs/index-parts`, merged into `split/mbox_index.jsonl`.
- `vectorize`: Python embeddings.
- `ingest`: Python LanceDB ingest.

Invariant: `mbox_index.jsonl` is generated during `preprocess` (no standalone index stage in `ragmail pipeline`).

## Removed legacy code paths
- Deleted old Python split/clean packages:
  - `python/lib/ragmail/split/`
  - `python/lib/ragmail/clean/`
- Removed Python index builder/writer from active code path.
- Removed CLI options and resume command emission for legacy rust toggles.

## Skills/docs alignment
- Skill docs now state `mbox_index.jsonl` is created during `preprocess`.
- Database skill reference now points to stage-based `ragmail pipeline` flow only.
- `docs/DESIGN.md` rewritten to Rust-first current architecture.

## Bench tooling
- `just.d/scripts/benchmark_pipeline.py` now benchmarks Rust pipeline throughput only.
- `just.d/scripts/benchmark_threshold.py` enforces `--min-msg-per-s` floor.
- CI benchmark smoke uses:
  - `UV_PROJECT_ENVIRONMENT=$PWD/.venv uv run --project python python just.d/scripts/benchmark_threshold.py --messages 2000 --iterations 1 --min-msg-per-s 1 --build-rust-bin`

## Build/test/release quick commands
- Bootstrap:
  - `just bootstrap` (shared root `.venv` + Rust workspace build)
- Rust gates:
  - `cargo fmt --manifest-path rust/Cargo.toml --all -- --check`
  - `cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets -- -D warnings`
  - `cargo test --manifest-path rust/Cargo.toml --workspace`
- Python tests:
  - `.venv/bin/python -m pytest -c python/pytest.ini -q`
- Release gates:
  - `just release-check`
  - `just release`

Bootstrap notes:
- Python bootstrap script: `just.d/scripts/bootstrap-python.sh`.
- `just bootstrap` attempts `uv` first, then falls back to `python -m venv + pip` when needed.
- Pipeline Rust bridge prefers prebuilt `rust/target/debug/ragmail-rs` if present.

## Key tests for new split/preprocess path
- `python/tests/test_rust_pipeline_bridge.py`
- `python/tests/test_index_parity.py` (now Rust-only contract/resume robustness)
- `python/tests/test_clean_parity.py` (Rust clean contract)
- `python/tests/test_clean_historical_parity.py` (historical fixture on Rust clean)

## Latest verification snapshot
- Python full suite: `./just.d/scripts/test-python.sh` => `118 passed, 6 skipped` (2026-02-06).
- Rust full suite: `cargo test --manifest-path rust/Cargo.toml --workspace` => all tests passed (2026-02-06).
- Rust lint gate: `just lint` (`cargo fmt --check` + `cargo clippy -D warnings`) => passed (2026-02-06).
