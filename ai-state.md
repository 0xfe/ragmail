# ai-state.md (ragmail)

Purpose: compact AI-only brief. Keep updated when CLI/stages/schema/workspace layout changes.

Repo entry
- CLI: `ragmail` -> `lib/ragmail/cli.py` (commands: `pipeline`, `search`, `stats`, `dedupe`, `serve`, `message`, `workspace`, `ignore`).
- Pipeline: `lib/ragmail/pipeline.py` (stages: download, split, index, clean, vectorize, ingest).
- Workspace layout: `lib/ragmail/workspace.py`.

Setup / tests
- Env: `uv venv && source .venv/bin/activate && uv sync`.
- Tests: `uv run pytest`.
 - If uv cache permission errors occur, rerun with `UV_CACHE_DIR=.uv-cache`.

Workspace layout (default under `workspaces/<name>/`)
- `inputs/` (symlinked inputs)
- `split/` (monthly `YYYY-MM.mbox` + `mbox_index.jsonl`)
- `clean/` (per-month `YYYY-MM.clean.jsonl`)
- `spam/` (per-month spam JSONL)
- `reports/` (per-month `.mbox.summary`)
- `embeddings/` (SQLite `.embed.db` per clean file)
- `db/` (`email_search.lancedb`)
- `logs/`, `.checkpoints/`, `cache/`
- `workspace.json`, `state.json`

Data flow / stages
- split: `lib/ragmail/split/splitter.py` streams MBOX, writes `split/YYYY-MM.mbox`, oldest->newest ordering by filename.
- clean: `lib/ragmail/clean/cleaner.py` -> clean/spam JSONL + summary; writes index records if `index_writer` set.
- index: `lib/ragmail/mbox_index.py` builds byte-offset index (`mbox_index.jsonl`).
  - Index is built during `clean` if clean runs; index stage exists for standalone rebuilds.
- vectorize: `lib/ragmail/vectorize/run.py` creates `.embed.db` per clean file under `embeddings/`.
- ingest: `lib/ragmail/ingest/run.py` + `lib/ragmail/ingest/pipeline.py` -> LanceDB in `db/`.
  - Default: repair missing embeddings during ingest; if *no* embeddings exist, fail.
  - Disable repair with `--no-repair-embeddings`.

Index + raw message lookup
- `split/mbox_index.jsonl` records `{email_id, message_id, message_id_lower, mbox_file, offset, length}`.
- `ragmail message --workspace <name> --message-id|--email-id` reads raw bytes via index.
- `mbox_file`, `mbox_offset`, `mbox_length` are stored in LanceDB during ingest for quick lookup.

Schema highlights (LanceDB)
- `emails` (flat) includes: `email_id`, `message_id`, `subject`, `from_*`, `to/cc`, `date`, `body_plain`, `has_attachment`, `attachment_names`, `attachment_types`, `labels_str`, `thread_id`, `in_reply_to`, `year`, `month`, `mbox_file/offset/length`, `subject_vector`.
- `email_chunks` includes chunk metadata + `chunk_text`, `body_vector`, plus same attachment + mbox fields.

Attachments
- Metadata only during clean/ingest. Raw attachments are NOT extracted by default.
- Opt-in: use `.agents/skills/ragmail/scripts/ragmail_attachments.py` to extract via index.
- Always warn: attachment extraction is slow (scans large MBOX files); require explicit user ask.

Search
- `lib/ragmail/search_cli.py` uses hybrid search (vector + FTS) + optional planner / RAG.
- FTS index built during ingest; auto-rebuild if corruption detected.

Config defaults (`lib/ragmail/config.py`)
- Embedding model: `nomic-ai/nomic-embed-text-v1.5`, dim 768, batch 32.
- Chunk size/overlap: 1200 / 200.
- Cache: `RAGMAIL_CACHE_DIR` or `./.ragmail-cache` (sets HF env vars in `workspace.apply_env`).

Docs + skills
- Main docs: `README.md`, `docs/`.
- Skill: `.agents/skills/ragmail/SKILL.md` (keep in sync with schema + workspace layout).

Common pitfalls
- Index missing: run `ragmail pipeline --stages clean --workspace <name>` (or `--stages index` for index-only).
- Large files: must stream; never load whole MBOX in memory.
- Resume: checkpoints live in `.checkpoints/`; clean + index support resume.
