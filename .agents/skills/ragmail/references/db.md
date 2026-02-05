# ragmail LanceDB reference

## Workspace layout

- Workspace root: `workspaces/<name>/`
- Database path: `workspaces/<name>/db/email_search.lancedb`
- `workspace.json` contains `paths.db` if customized.
- Other workspace outputs:
  - `clean/` cleaned JSONL
  - `spam/` filtered spam/newsletters
  - `split/` split mbox chunks (`YYYY-MM.mbox`) + `mbox_index.jsonl`
  - `reports/` summaries
  - `logs/` pipeline logs

## Tables

### `emails`

Core fields (flattened):
- `email_id` (string)
- `message_id` (string | null)
- `subject` (string)
- `from_address` (string)
- `from_name` (string)
- `to_addresses_str` (string, comma-separated)
- `cc_addresses_str` (string, comma-separated)
- `date` (timestamp[us])
- `body_plain` (string, truncated to 10k chars)
- `has_attachment` (bool)
- `attachment_names` (list[string])
- `attachment_types` (list[string])
- `labels_str` (string, comma-separated)
- `in_reply_to` (string | null)
- `thread_id` (string | null)
- `year` (int | null)
- `month` (int | null)
- `mbox_file` (string | null)
- `mbox_offset` (int | null)
- `mbox_length` (int | null)
- `subject_vector` (vector)

### `email_chunks`

Chunked body search:
- `chunk_id`, `email_id`, `chunk_index`, `chunk_text`
- Same sender/recipient/date/labels fields as `emails`
- `body_vector` (vector)

## Full-text search (FTS)

FTS index is created on `emails` for:
- `subject`
- `body_plain`
- `from_name`
- `from_address`
- `to_addresses_str`
- `cc_addresses_str`
- `labels_str`

For `email_chunks`, use:
- `subject`
- `chunk_text`
- `from_name`
- `from_address`
- `to_addresses_str`
- `cc_addresses_str`
- `labels_str`

FTS index is created during ingest and auto-rebuilt if corrupted.

## Date filtering

Preferred filters:
- `year = YYYY`
- `month = M`
- `date >= 'YYYY-MM-DDTHH:MM:SS' AND date <= 'YYYY-MM-DDTHH:MM:SS'`

## Ingestion pipeline overview

Pipeline stages (see `lib/ragmail/pipeline.py` and `lib/ragmail/ingest/run.py`):
- `ragmail sample` (optional) creates a distributed sample mbox
- `ragmail clean` parses/cleans mbox into JSONL (`*.clean.jsonl`)
- `ragmail ingest` embeds and writes records into LanceDB (`email_search.lancedb`)
- `ragmail pipeline` orchestrates the full flow into a workspace

The database schema is defined in `lib/ragmail/storage/schema.py` and the FTS setup in `lib/ragmail/storage/repository.py`.

## Raw data location

- Raw Gmail exports (gitignored): `private/gmail-*.mbox`
