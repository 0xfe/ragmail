# ragmail Migration TODO

Track improvements, risks, and follow-up work. Keep this file prioritized by risk/robustness.

## Priority Queue (Risk/Robustness Order)
- [ ] P1: Validate `.github/workflows/release.yml` end-to-end on a real `vX.Y.Z` tag (artifacts/checksums/formula).
- [ ] P1: Validate Linux `arm64` release binaries on real arm64 hosts.
- [ ] P1: Validate Homebrew tap publish automation with real tap credentials.
- [ ] P2: Benchmark and tune `--preprocess-workers` defaults on large corpora (avoid IO thrash on low-IO hosts).
- [ ] P2: Expand historical-corpus clean/index verification beyond fixtures (multi-year real-world sample).
- [ ] P2: Tighten benchmark floor to a meaningful hardware-normalized threshold after collecting baseline runs.

## Completed Improvements (Latest)
- [x] Fixed split-stage `Too many open files` failures by bounding open month writers with LRU eviction+flush.
- [x] Flushed split writers before checkpoint writes to improve resume durability after interruption.
- [x] Added low-writer-limit split regression test for many-month corpora.
- [x] Removed legacy Python split/clean modules (`python/lib/ragmail/split`, `python/lib/ragmail/clean`).
- [x] Removed legacy pipeline toggle flags (`--rust-split-index`, `--rust-clean`) from CLI.
- [x] Made Python pipeline orchestration Rust-only for `split/preprocess`.
- [x] Simplified Python index helper module to read-only index lookups.
- [x] Rewrote parity tests to Rust-only contract/robustness coverage (`test_clean_*`, `test_index_parity`, `test_rust_pipeline_bridge`).
- [x] Updated README/skills/design docs to describe only the Rust-first stage model.
- [x] Reworked benchmark tooling to Rust-only throughput + throughput floor gate (`--min-msg-per-s`).
- [x] Moved Python project layout under `python/` and updated just/CI/scripts to use `uv --project python`.
- [x] Moved pytest suite under `python/tests/` and updated pytest config/CI/docs path references.
- [x] Moved `pytest.ini` under `python/` and updated runners to pass `-c python/pytest.ini`.
- [x] Updated Rust and Python test fixtures/paths to `python/tests/fixtures` after relocation.
- [x] Rewrote docs set for current architecture: simplified `README.md`, added `docs/pipeline.md`, refreshed `docs/DESIGN.md`, added `docs/developers.md`.
- [x] Fixed bootstrap-to-run flow: shared root `.venv`, Rust prebuild in `just bootstrap`, and automatic use of prebuilt `ragmail-rs`.
- [x] Fixed Python pipeline repo-root detection after `python/` move by resolving root from `rust/Cargo.toml`.
- [x] Renamed pipeline stages to `model,split,preprocess,vectorize,ingest`; removed standalone `index` stage from `ragmail pipeline`.
- [x] Made preprocess emit `split/mbox_index.jsonl` inline (clean + index in one pass).
- [x] Added preprocess concurrency control (`--preprocess-workers`, `EMAIL_SEARCH_PREPROCESS_WORKERS`) for large-corpus speedups.
- [x] Fixed Rust clean overflow panic on large blank-line runs and reduced body-normalization allocations.
- [x] Updated skill/docs/test references from `index`/`clean` stage names to `preprocess`.
- [x] Ran full suite gates after migration:
  - Python: `./just.d/scripts/test-python.sh` (118 passed, 6 skipped)
  - Rust: `cargo test --manifest-path rust/Cargo.toml --workspace` (all passed)
  - Lint: `just lint` (`cargo fmt --check` + `cargo clippy -D warnings`)

## Current Risks
- Benchmark floor currently uses a permissive smoke threshold and should be calibrated with real baseline data.
- Distribution automation remains partially unvalidated without real tag/tap/arm64 host runs.

## Future Opportunities
- Move pipeline orchestration entirely into Rust CLI and keep Python CLI as thin shim.
- Add golden JSONL contract fixtures for clean/index outputs to lock schema behavior across releases.
- Add nightly corpus-scale benchmark + regression trend reporting.
