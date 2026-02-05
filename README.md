# RAGMail

RAGmail is an agent skill that searches and analyzes your email data.

It lets you ask questions like:

- "How much did the house painting I did last summer cost?"
- "How many times did Bob email me in Feb 2026?"
- "Who was Michael's teacher in the 9th grade?"

## How do I use it?

RAGMail can work with any MBOX file, and includes special support for Gmail.

You can start by visiting [Google Takeout](https://takeout.google.com/) and downloading your Gmail data.

Then run `ragmail pipeline` to split, clean, vectorize, and then ingest your emails into a LanceDB vector database. It
takes care of a lot of tedious work around spam removal, stripping signatures and attachments, dealing with multipart messages,
building embedding vectors, deduplication, etc.

```bash
# Create a new workspace under workspaces/foo
ragmail pipeline private/all-my-emails.mbox --workspace foo
```

Note, this can take a very long time (>15h on my macbook pro for 15GB of email.) You can make this a lot faster (<1h) on an L4 GPU VM. See instructions below on how to do that.

To analyze your emails, start your favorite AI agent (e.g., Claude Code or Codex) and use the `ragmail` skill.

```
$ codex
> load the $ragmail skill in the workspace foo
...
Using the ragmail skill for workspace mo. What do you want to find in that workspace
(query, sender, date range, or topic)?
...
> how many times did Bob email me in Feb 2026?
...
```

## Quickstart

```bash

# Setup env (once)
uv venv
uv sync

# Activate environment
source .venv/bin/activate

# Run the full pipeline
ragmail pipeline private/gmail-2015.mbox --workspace test-sample

# Search
ragmail search "meeting tomorrow" --workspace test-sample
```

## Full Remote-GPU Workflow

This uses stage controls to move work across machines, and only ships the clean JSONL + embeddings.

1. Local machine: split + clean only
```bash
ragmail pipeline ~/path/to/mbox --workspace foo --stages split,clean
```

2. Local machine: package clean JSONL and copy to remote
```bash
tar -czf foo-clean.tar.gz workspaces/foo/clean
rsync -av foo-clean.tar.gz your-vm:/data/ragmail/
```

3. Remote machine: unpack and vectorize
```bash
tar -xzf /data/ragmail/foo-clean.tar.gz -C /data/ragmail/
ragmail pipeline \
  --workspace foo \
  --base-dir /data/ragmail/workspaces \
  --stages vectorize \
  --clean-dir /data/ragmail/clean
```

4. Remote machine: package embeddings and copy back
```bash
tar -czf foo-embeddings.tar.gz -C /data/ragmail embeddings
rsync -av your-vm:/data/ragmail/foo-embeddings.tar.gz ./
```

5. Local machine: unpack embeddings and ingest
```bash
tar -xzf foo-embeddings.tar.gz -C workspaces/foo
ragmail pipeline --workspace foo --stages ingest
```

## About Workspaces

Workspaces are directories under `workspaces/` that contain all the state for the different stages of a pipeline. If you're
ingesting multiple mailboxes and want them in different databases, you must use different workspaces.

```bash
ragmail workspace init my-workspace
ragmail pipeline private/gmail-2014.mbox private/gmail-2015.mbox --workspace my-workspace

# Search within workspace without specifying --db
ragmail search "meeting tomorrow" --workspace my-workspace
```

### Pipeline Stages

You can run specific stages only:
```bash
ragmail pipeline private/gmail-2015.mbox --workspace my-workspace --stages split,clean
ragmail pipeline --workspace my-workspace --stages vectorize
ragmail pipeline --workspace my-workspace --stages ingest
```

You can also point vectorize/ingest at a directory of clean JSONL files:
```bash
ragmail pipeline --workspace my-workspace --stages vectorize --clean-dir /data/clean
ragmail pipeline --workspace my-workspace --stages ingest --clean-dir /data/clean
```

Stages:
- `download` (dependency warmup)
- `split`
- `index` (MBOX byte-offset index; built during `clean`)
- `clean`
- `vectorize`
- `ingest`

Note: ingest expects embeddings produced by the `vectorize` stage. If none exist, run `ragmail pipeline --stages vectorize` first.

Split output files are monthly: `workspaces/<name>/split/YYYY-MM.mbox` (processed oldest to newest).
Ingest stores `mbox_file`, `mbox_offset`, and `mbox_length` in LanceDB for fast raw message lookup.

Cache note:
- Default model cache: `./.ragmail-cache`
- Override with `--cache-dir /path/to/cache`

## Ingest Tuning (Speed)

You can tune ingestion from the CLI:

- `--ingest-batch-size`: how many emails to buffer before writing to the DB. Larger = fewer writes.
- `--embedding-batch-size`: batch size sent to the embedding model. Larger = faster if you have GPU/CPU memory.
  This controls how many texts the model embeds per call, while `--ingest-batch-size` controls how many emails are flushed per DB write.
- `--chunk-size` / `--chunk-overlap`: controls body chunking. Fewer chunks = faster ingestion.
- `--skip-exists-check`: skips per-email existence lookups. This is auto-enabled for new or empty databases.
- `--checkpoint-interval`: checkpoint interval in seconds (default: 120). This allows session resumption.
- `--refresh`: rerun selected stages from scratch. Existing outputs are archived to `workspaces/<name>/old/YYMMDDHHMMSS/` and checkpoints are cleared.
- `--no-repair-embeddings`: disable automatic repair of missing embeddings during ingest (enabled by default).
- `--compact-every`: run compaction every N ingested emails (default: 20000). Set to `0` to disable periodic compaction; a final compaction still runs after ingest.

Example:
```bash
ragmail pipeline --workspace test-sample --stages ingest \
  --ingest-batch-size 500 \
  --embedding-batch-size 128 \
  --chunk-size 2000 \
  --chunk-overlap 0
```
These flags also work with `ragmail pipeline`.

## Search + Query Planning

- Hybrid search uses vector similarity plus full-text search (subject, body, sender, recipients, labels).
- FTS index is created during ingest and auto-rebuilt if corruption is detected.

Examples:
```bash
# Pure search
ragmail search "who is Amy Smith" --workspace 2026

# Structured planning with LLM (JSON-only plan)
ragmail search --plan "how many emails from Anthropic in January 2026" --workspace 2026

# RAG answers also enable the planner by default
ragmail search --rag "summarize my discussions with Sarah about the budget" --workspace 2026
```

## Maintenance

```bash
# Check for duplicate IDs (emails + chunks)
ragmail stats --dupes --workspace my-workspace

# Remove duplicates (rebuilds FTS index after emails)
ragmail dedupe --workspace my-workspace
```

## Agent Skills (Codex/Claude)

Agent skills are a first-class, supported way to query the LanceDB workspaces. This repo includes the `ragmail` skill under `.agents/skills/ragmail` with a scripted query helper for fast, repeatable answers. Keep the skill updated if schemas, indexing, or workspace layout change.

Examples to ask an agent:
- "How many times did Bob email me in Feb 2026?"
- "How much did the house painting I did last summer cost?"
- "Who is Arkin's teacher?"

Usage notes:
- Run from repo root and specify a workspace name or db path.
- Convert relative dates to explicit ranges before querying.
- If your agent expects skills in a different directory, configure it to load `.agents/skills`.

## Ignore Lists

```bash
# Create an ignore list template
ragmail ignore init /tmp/ignore.json

# Apply ignore list rules
ragmail ignore apply test-sample.clean.jsonl --ignore-list /tmp/ignore.json
```

## Packaging

Create a portable source tarball (no data/caches/workspaces):

```bash
make package
```

If you want to include local uncommitted changes, use:
```bash
make package-dev
```
## Docs

See `docs/` for detailed documentation.
