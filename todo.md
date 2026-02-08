# Prioritized TODO (Risk/Robustness First)

1. `P0` Verify dependency-free runtime on clean hosts for each release artifact (macOS amd64/arm64, Linux amd64/arm64), including Python bridge execution from Rust.
2. `P0` Ensure Rust pipeline resume/idempotency for large real-world MBOX files after the entrypoint and preprocess changes.
3. `P0` Validate packaged Python bridge reliability for long vectorize/ingest runs (memory growth, retries, error surfacing).
4. `P0` Validate Ctrl-C behavior end-to-end for running Python bridge stages (model/vectorize/ingest), including subprocess shutdown and checkpoint consistency.
5. `P1` Harden release workflow for multi-arch builds (especially Linux arm64 packaging and checksums/formula consistency).
6. `P1` Add CLI integration tests for Rust passthrough commands (`search`, `stats`, `dedupe`, `serve`) through the packaged bridge path.
7. `P1` Add explicit preflight diagnostics when bridge binaries are missing/mismatched version.
8. `P1` Add TTY detection for pipeline live UI; fall back to non-live log mode to avoid ANSI cursor noise in CI/non-interactive shells.
9. `P2` Validate model-stage byte reporting on uncached downloads (cache miss path) and ensure display remains accurate on slow links.
10. `P2` Add benchmark baselines and regression gates for preprocess throughput and memory.
11. `P2` Review optional parallelism knobs for preprocess and split under constrained I/O environments.
12. `P3` Add signed release artifacts and provenance metadata (SBOM/attestation) once base release flow is stable.

## Notes Collected So Far
- Users need `just bootstrap` to produce a ready `ragmail` command in the activated `.venv`.
- `ragmail` is now the canonical Rust CLI name; keep `ragmail-rs` compatibility only where explicitly needed.
- Stage naming drift between Rust and Python harnesses is a recurring source of confusion; Rust canonical stage names must be authoritative.
- Rust pipeline now has a Python-style live stage area; non-interactive output needs dedicated fallback formatting.
- Python bridge contract now includes streamed JSON events (`event=progress|compaction`) plus final JSON result.
- `split` and `preprocess` now emit in-loop progress updates for large single-file runs; keep watching for regressions under very large corpora.
- Rust bridge boolean flags for Click-style options now use `--resume`/`--no-resume`; never pass boolean strings as extra args.
- `vectorize` and `ingest` now surface explicit `starting` status and startup-text progress before first processed batch.
- Vectorize emits startup heartbeat events while embedding model initialization blocks, reducing perceived hangs.
