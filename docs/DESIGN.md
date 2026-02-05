# ragmail Design (M0)

## Goal
Unify `ragmail` and `ragmail` into a single program named `ragmail` with one CLI, one workspace model, and a single root venv. Preserve behavior while improving cohesion, validation, and testability.

## Current State Inventory

### Entry Points
- `ragmail/ragmail.py`: Clean Gmail MBOX into JSONL for RAG.
  - Outputs:
    - `<name>.clean.jsonl`
    - `<name>.spam.jsonl`
    - `<name>.mbox.summary`
  - Supports `--resume` checkpointing.
- `ragmail/split_mbox.py`: Split MBOX by year.
  - Supports `--resume`, `--refresh`, `--years`, and `--verify`.
- `ragmail/sample-mbox.py`: Create small samples for testing.
  - Supports `--distributed` and `--emails-per-file`.
- `ragmail/lib/ragmail/cli.py`: Click-based CLI.
  - Commands: `ingest`, `search`, `stats`, `serve`.
  - Ingest accepts JSONL or MBOX and uses checkpointing.

### Shared Libraries
- `ragmail/lib/terminal.py`: terminal UI helpers.
- `ragmail/lib/checkpoint.py`: generic checkpoint support.
- `ragmail` contains its own config, ingestion, and storage layers.

## Data Formats

### Clean JSONL (from `ragmail`)
Each line is a JSON object with these top-level keys:
- `headers` (dict):
  - `from`: `{name, email}` or string
  - `to`, `cc`, `bcc`: list of `{name, email}` or strings
  - `reply_to`: `{name, email}`
  - `subject`: string
  - `date`: ISO 8601 string (e.g. `2015-01-23T15:02:04-08:00`)
  - `message_id`, `in_reply_to`: string
  - `references`: list of message IDs
  - `thread_id`: string (Gmail thread id)
  - `list_id`: string
- `tags`: list of Gmail labels
- `content`: list of blocks `{type: "text", body: "..."}`
- `attachments` (optional): list of `{filename, content_type, size}`

Example (abbrev):
```json
{"headers":{"from":{"name":"Brian","email":"notifications@github.com"},"subject":"...","date":"2015-01-23T15:02:04-08:00"},"tags":["Archived"],"content":[{"type":"text","body":"..."}]}
```

### Spam JSONL (from `ragmail`)
Minimal JSON for filtered email summaries:
- `from`, `subject`, `date`, `reason`

### Summary File
`<name>.mbox.summary` captures counts, labels, senders, and size reduction.

## Ingestion Expectations (ragmail)
- `JsonEmailParser` accepts the above JSONL format.
- Addresses may be dicts or strings.
- `headers.references` may be list or string.
- `content` is expected to contain at least one `type="text"` block.
- Date parsing accepts ISO 8601 and RFC-2822-ish strings.

## Target Architecture (Proposed)

### Package Layout
Single top-level package `ragmail/`:
- `ragmail/cli.py`: unified CLI router
- `ragmail/clean/`: cleaning pipeline
- `ragmail/split/`: mbox split
- `ragmail/ingest/`: JSONL + DB ingestion
- `ragmail/search/`: search + RAG
- `ragmail/workspace/`: workspace config and path resolution
- `ragmail/common/`: progress, checkpointing, shared utilities

Legacy entrypoints remain as thin shims until full migration completes.

### Workspace Model
Workspace directory per dataset:
```
workspaces/<name>/
  inputs/
  clean/
  spam/
  db/
  logs/
  .checkpoints/
  reports/
  cache/
  split/
  workspace.json
  state.json
```
Workspace config file (JSON or YAML) defines paths, defaults, and metadata.

`workspace.json` contains the workspace name, root, created timestamp, and path map.
`state.json` tracks pipeline stage status and timestamps for resume support.
`split/` contains monthly MBOX chunks (`YYYY-MM.mbox`) plus `mbox_index.jsonl`.

### CLI Contract (Draft)
```
ragmail pipeline <mbox> [--workspace <name>] [--stages split,index,clean,vectorize,ingest] [--resume] [--refresh] [--no-repair-embeddings] [--compact-every <N>]
ragmail search <query> [--db <path>] [--limit N] [--rag]
ragmail stats [--db <path>]
ragmail serve [--db <path>] [--host] [--port]
```

## Search + Query Planning

- Hybrid search combines vector similarity with full-text search (FTS).
- FTS indexes subject, body, sender/recipient fields, and labels.
- A structured query planner converts natural language into:
  - vector query text
  - FTS query text
  - metadata filters (year/month/date range/attachments)
- The planner can be LLM-assisted (`--plan` / `--rag`) and always returns JSON.

## Validation + Ignore Lists (M4)
- JSONL validation runs before ingest by default, with `--strict` and `--max-errors` modes (via pipeline ingest stage).
- Validation errors are logged as structured JSONL (default `errors.jsonl` in checkpoint/logs).
- Ignore lists are JSON-defined rule sets applied post-cleaning (`ragmail ignore apply`), emitting filtered and ignored JSONL outputs with rule metadata.

## Baseline Verification (M0)
- Created `private/sample-3years.mbox` from 2015/2024/2026 (10 emails each).
- Ran pipeline split/clean (index built during clean)/vectorize/ingest in a workspace:
  - Clean: 29, Spam: 1, Errors: 0.
  - Summary file: `private/sample-3years.mbox.summary`.
- Ingested into `/tmp/ragmail-test.lancedb`: total 29 emails.
- Search query `"styling modifiers"` returned expected results.
- `ragmail stats` returned per-year counts and top senders.

### Notes / Risks
- Console summary printed `Total processed: 0` while summary file shows 30 processed. Likely a progress/stat display bug.
- HuggingFace model cache writes failed in this environment (`Operation not permitted`); ingestion still completed. Consider redirecting cache in future (e.g., `HF_HOME` into workspace).
- `stats` command uses a hardcoded embedding dimension (384). Default model dimension is 768. Verify this in refactor.

Tracked in: `docs/RISKS.md`
