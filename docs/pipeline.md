# Pipeline Deep Dive

This doc explains each pipeline stage.
It also shows when to run stages independently.

The main entrypoint is:
- `ragmail pipeline ... --workspace <name>`

Related post-ingest commands (`query`, `stats`, `message`, `serve`) are Python passthrough commands.
They work from `ragmail`, even though they are not listed in `ragmail --help`.

Default stage order:
1. `model`
2. `split`
3. `preprocess`
4. `vectorize`
5. `ingest`

## Stage Summary

| Stage | Runtime | Purpose | Primary outputs |
|---|---|---|---|
| `model` | Python | Warm model/cache dependencies | cache directories |
| `split` | Rust | Split MBOX by month with checkpoints | `split/YYYY-MM.mbox` |
| `preprocess` | Rust | Parse + clean + filter noisy mail and build byte-offset index | `clean/*.clean.jsonl`, `spam/*.spam.jsonl`, `reports/*.summary`, `split/mbox_index.jsonl` |
| `vectorize` | Python | Build embedding vectors from clean JSONL | `embeddings/*.embed.db` |
| `ingest` | Python | Write records into LanceDB + FTS | `db/email_search.lancedb` |

## How Each Stage Works

## `model`
This stage prepares dependencies and cache state.
It makes later stages more predictable.

Use it alone when you want to pre-warm a machine.

```bash
# Warm dependencies only
ragmail pipeline --workspace my-mail --stages model
```

## `split`
This stage streams MBOX and writes month-partitioned files.
It is resumable and checkpointed.

Use it alone when:
- You want to inspect raw monthly partitions.
- You want to parallelize later work by month.

```bash
# Split only
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --stages split

# Split with year filter
ragmail pipeline private/all-mail.mbox --workspace my-mail --stages split --years 2024 --years 2025
```

## `preprocess`
This stage parses messages, normalizes headers, extracts useful body text, and filters low-value bulk noise.
It writes clean and spam outputs plus summaries, and produces `split/mbox_index.jsonl` in the same pass.

Use it alone when:
- You changed cleaning logic and want to regenerate outputs.
- You want to inspect cleaning quality before embeddings.
- You need to rebuild `mbox_index.jsonl` without re-running split.

```bash
# Preprocess only
ragmail pipeline --workspace my-mail --stages preprocess

# Re-run preprocess from scratch and archive previous outputs
ragmail pipeline --workspace my-mail --stages preprocess --refresh
```

## `vectorize`
This stage reads clean JSONL and creates embedding stores.
It is the best candidate for GPU acceleration.

Use it alone when:
- You process clean JSONL on a different machine.
- You want to tune embedding throughput without re-cleaning.

```bash
# Vectorize from workspace clean outputs
ragmail pipeline --workspace my-mail --stages vectorize

# Vectorize from an external clean directory
ragmail pipeline --workspace my-mail --stages vectorize --clean-dir /data/clean
```

## `ingest`
This stage writes email and chunk rows into LanceDB.
It also manages FTS index creation and compaction.

Use it alone when:
- Embeddings already exist.
- You need to rebuild DB state from clean/embedding outputs.

```bash
# Ingest from workspace embeddings
ragmail pipeline --workspace my-mail --stages ingest

# Ingest from external clean directory (with matching embeddings)
ragmail pipeline --workspace my-mail --stages ingest --clean-dir /data/clean
```

## Stage Composition Patterns

## Pattern A: Full local run
```bash
# One command for complete pipeline
ragmail pipeline private/gmail-2015.mbox --workspace my-mail
```

## Pattern B: Preprocess local, vectorize remote GPU
```bash
# Local preprocessing
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --stages split,preprocess

# Remote vectorization (example)
ragmail pipeline --workspace my-mail --base-dir /data/ragmail/workspaces --stages vectorize --clean-dir /data/ragmail/clean

# Local ingest after embeddings return
ragmail pipeline --workspace my-mail --stages ingest
```

## Pattern C: Rebuild DB only
```bash
# Re-ingest existing clean/embedding outputs
ragmail pipeline --workspace my-mail --stages ingest --refresh
```

## Key Parameters

- `--stages`: run a subset of stages.
- `--resume/--no-resume`: continue from checkpoints or force fresh execution behavior.
- `--refresh`: archive stage outputs and rerun selected stages.
- `--base-dir`: change workspace root.
- `--years`: filter split/preprocess inputs to specific years.
- `--clean-dir`: use external clean JSONL for vectorize/ingest.
- `--embeddings-dir`: override embeddings input/output directory.
- `--db-path`: override target LanceDB path for ingest.

Ingest and embedding tuning:
- `--embedding-batch-size`
- `--ingest-batch-size`
- `--chunk-size`
- `--chunk-overlap`
- `--checkpoint-interval`
- `--skip-exists-check`
- `--compact-every`
- `--no-repair-embeddings`

## Resume and Refresh Behavior

Resume behavior:
- Stages already marked done are skipped.
- Interrupted stages use checkpoints when available.

Refresh behavior:
- Selected stage outputs are archived under `workspaces/<name>/old/<timestamp>/`.
- Selected stage checkpoints are cleared.
- Pipeline reruns selected stages from clean state.

## Useful Validation Commands

```bash
# Quick dataset sanity checks
ragmail stats --workspace my-mail

# Search after ingest
ragmail query --workspace my-mail "invoice" --limit 20

# Fetch full raw message via index
ragmail message --workspace my-mail --email-id <email_id>
```

## Troubleshooting

- If `ingest` fails due to missing embeddings, run `vectorize` first.
- If raw message lookup fails, rerun `preprocess`.
- If you changed cleaner logic, rerun `preprocess` and downstream stages.
- For detailed diagnostics, check `workspaces/<name>/logs/` and `state.json`.
