# Design

This document explains the current ragmail architecture.
It is written for technical users and developers.

## Goals

- Keep MBOX-heavy work fast and resumable.
- Keep interfaces stable between stages.
- Keep workspace outputs inspectable and recoverable.
- Keep Python for embedding, ingest, search, and LLM integration.

## Architecture Overview

ragmail has two runtimes:
- Rust runtime for orchestration and MBOX-heavy stages.
- Python runtime only for model warmup, vectorization, ingest, and search/API layers.

Primary entrypoint:
- `ragmail pipeline ... --workspace <name>`

The Rust CLI is the public harness entrypoint.
Rust calls Python bridge commands only for Python-owned stages.

## Stage Ownership

| Stage | Owner | Why |
|---|---|---|
| `model` | Python (via Rust bridge) | dependency/cache prep logic |
| `split` | Rust | streaming + checkpointed MBOX partitioning |
| `preprocess` | Rust | parse/clean/filter and emit `mbox_index.jsonl` in one pass |
| `vectorize` | Python (via Rust bridge) | embedding models + Python ML ecosystem |
| `ingest` | Python (via Rust bridge) | LanceDB ingest, compaction, FTS operations |

## Data Flow

1. Input MBOX files are linked into workspace `inputs/`.
2. Rust `split` emits month-partitioned MBOX files.
3. Rust `preprocess` emits structured `clean/*.clean.jsonl` and `spam/*.spam.jsonl`.
4. Rust `preprocess` also emits `split/mbox_index.jsonl` for raw-byte lookups.
5. Python `vectorize` emits embedding stores (`embeddings/*.embed.db`).
6. Python `ingest` writes normalized records into LanceDB.

## Workspace Model

Workspace root:
- `workspaces/<name>/`

Important subpaths:
- `split/`: month files plus `mbox_index.jsonl`
- `clean/`: cleaned JSONL
- `spam/`: filtered rows
- `embeddings/`: embedding DB files
- `db/email_search.lancedb`: final retrieval DB
- `.checkpoints/`: resumable stage state
- `logs/`: per-stage logs
- `state.json`: stage status and details

## Stage State Contract

Each stage tracks status in `state.json`:
- `pending`
- `running`
- `done`
- `failed`
- `interrupted`

This supports:
- Safe resume after interruption.
- Deterministic skip of completed stages.
- Observable failure details for debugging.

## Key Contracts and Invariants

- MBOX handling is streaming.
- Stages are resumable and idempotent where practical.
- `mbox_index.jsonl` format remains stable for message lookup.
- Clean JSONL schema remains stable for vectorize/ingest/search.
- Stage outputs are isolated by workspace.
- `ragmail` binary is always the entrypoint in source and release builds.

## Rust-Python Boundary

Rust to Python boundary:
- Rust orchestrator can call Python bridge commands:
  - `ragmail py model`
  - `ragmail py vectorize`
  - `ragmail py ingest`
- Rust accepts external subcommands and forwards them to Python for query/API workflows.
  - Preferred path: `ragmail-py ...` when available.
  - Fallback path: `python -m ragmail.search_cli ...` in source/dev environments.

This boundary keeps heavy MBOX processing in Rust while preserving Python ML integrations.

## Search and RAG Path

After ingest, retrieval uses LanceDB tables:
- `emails`
- `email_chunks`

Search can run fully local.
LLM-assisted answer generation uses an OpenAI-compatible backend.
You can route this backend to hosted or local servers.

## Performance Model

Largest wins come from:
- Rust streaming stages (`split`, `preprocess`).
- Running `vectorize` on GPUs.
- Stage-only reruns using `--stages` and `--refresh`.

## Failure and Recovery Model

- Checkpoints reduce restart cost.
- `--resume` continues partial work.
- `--refresh` archives outputs and reruns selected stages.
- Logs and state files are stored per workspace.

## Compatibility Notes

- Rust toolchain target is 1.93.0+.
- `VERSION` is the cross-runtime version source of truth.
- Release artifacts include macOS and Linux targets.

## Related Docs

- Pipeline deep dive: [`pipeline.md`](pipeline.md)
- Developer guide: [`developers.md`](developers.md)
- Release flow: [`release.md`](release.md)
