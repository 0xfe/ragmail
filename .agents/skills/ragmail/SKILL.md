---
name: ragmail
description: Query ragmail LanceDB workspaces for email search, counts, date/topic filters, and amount extraction. Use for answering questions about who/what/when from email data.
metadata:
  short-description: Query ragmail email databases
---

# ragmail skill

Use this skill when the user asks questions about email content in ragmail workspaces (who said what, counts by sender/date, topic summaries, costs mentioned, etc.). Default to running small Python scripts that query LanceDB directly; use the bundled CLI script only as a convenience or for quick reference.

## Quick start

1. Identify the target workspace (e.g., `2026`) or the full LanceDB path.
2. Ensure Python is run from the repo venv created with `uv`.
3. Prefer a short ad‑hoc Python script that connects to LanceDB and runs the query.
4. If the question uses relative dates ("last summer", "yesterday"), convert to absolute dates before querying.

### Python environment (required)

Use `uv` to create and manage the repo venv:

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Locate the database

- Workspace root: `workspaces/<name>/`
- Database: `workspaces/<name>/db/email_search.lancedb`
- If `workspaces/<name>/workspace.json` exists, prefer `paths.db`.

## Direct query recipe (preferred)

Use Python + `lancedb` to query the tables directly.

```python
import lancedb
from pathlib import Path

db_path = Path("workspaces/2026/db/email_search.lancedb")
db = lancedb.connect(str(db_path))
emails = db.open_table("emails")

# FTS search + filters
rows = (
    emails.search("Arkin teacher", query_type="fts")
    .where("year = 2026 AND month = 1", prefilter=True)
    .limit(50)
    .to_list()
)
```

For deeper body matches, use `email_chunks` and search `chunk_text`.

## Direct Python Queries (recommended)

Use `uv run python - <<'PY'` to run a short script inline. This enables more complex filtering and joins than the CLI wrapper.

### Example: FTS + filter + custom projection
```bash
uv run python - <<'PY'
import lancedb

db = lancedb.connect("workspaces/2026/db/email_search.lancedb")
emails = db.open_table("emails")

rows = (
    emails.search("school trip", query_type="fts")
    .where("year = 2025 AND month = 9", prefilter=True)
    .limit(20)
    .to_list()
)

for r in rows:
    print(r["date"], r["from_address"], r["subject"])
PY
```

### Example: Pull full emails after chunk hits
```bash
uv run python - <<'PY'
import lancedb

db = lancedb.connect("workspaces/2026/db/email_search.lancedb")
chunks = db.open_table("email_chunks")
emails = db.open_table("emails")

hits = (
    chunks.search("refund", query_type="fts")
    .where("year = 2024", prefilter=True)
    .limit(5)
    .to_list()
)

email_ids = {h["email_id"] for h in hits}
full = (
    emails.search()
    .where("email_id IN (" + ",".join([f\"'{i}'\" for i in email_ids]) + ")", prefilter=True)
    .to_list()
)

for r in full:
    print(r["subject"])
    print(r["body_plain"][:500])
    print("----")
PY
```

### Example: Aggregate counts by sender
```bash
uv run python - <<'PY'
import lancedb
from collections import Counter

db = lancedb.connect("workspaces/2026/db/email_search.lancedb")
emails = db.open_table("emails")

rows = (
    emails.search("invoice", query_type="fts")
    .where("year = 2024", prefilter=True)
    .limit(2000)
    .to_list()
)

counts = Counter(r["from_address"] for r in rows)
for addr, cnt in counts.most_common(10):
    print(cnt, addr)
PY
```

## Optional CLI Script (reference / quick use)

Use this when you need fast counts, snippets, or amount extraction without writing a Python snippet.

```bash
python .agents/skills/ragmail/scripts/ragmail_query.py search --workspace 2026 --query "Arkin teacher"
```

## Commands

Run from repo root:

```bash
python .agents/skills/ragmail/scripts/ragmail_query.py search --workspace 2026 --query "Arkin teacher" --limit 50
```

### Search

```bash
python .agents/skills/ragmail/scripts/ragmail_query.py search \
  --workspace 2026 \
  --query "Arkin teacher" \
  --limit 50 \
  --fields date,from_name,from_address,subject,email_id,snippet
```

### Count

```bash
python .agents/skills/ragmail/scripts/ragmail_query.py count \
  --workspace 2026 \
  --from-like "bob" \
  --year 2026 \
  --month 2
```

### Sum amounts (costs)

```bash
python .agents/skills/ragmail/scripts/ragmail_query.py sum \
  --workspace 2026 \
  --query "house painting" \
  --start 2025-06-01 \
  --end 2025-08-31
```

## Filters (all commands)

- `--year` / `--month` for quick month scoping
- `--start` / `--end` for date ranges (YYYY-MM-DD or ISO datetime)
- `--from-address` for exact sender match
- `--from-like`, `--to-like`, `--subject-like`, `--labels-like` for string filters
- `--table` can be `emails` (default) or `email_chunks`

## Output notes

- `search` emits key fields and a snippet.
- `count` returns `count=` and warns if it hits `--max-scan`.
- `sum` extracts currency-like amounts from matching emails and reports totals plus examples.

## Find And Display Full Email

Use a two-step flow: locate the matching chunk to get `email_id`, then fetch the full email body from the `emails` table.

1. Find the matching chunk and capture `email_id`:
```bash
python .agents/skills/ragmail/scripts/ragmail_query.py search \
  --workspace 2026 \
  --table email_chunks \
  --query "Born 24 March 2010" \
  --limit 5 \
  --fields date,from_name,from_address,subject,email_id,chunk_text
```

2. Fetch the full email by `email_id` (direct LanceDB query):
```bash
uv run python - <<'PY'
import lancedb
from pathlib import Path

db = lancedb.connect("workspaces/2026/db/email_search.lancedb")
emails = db.open_table("emails")

email_id = "bf509cfe2e3ca574"
rows = (
    emails.search()
    .where(f"email_id = '{email_id}'", prefilter=True)
    .limit(1)
    .to_list()
)
print(rows[0]["body_plain"] if rows else "not found")
PY
```

Alternate: Use `ragmail_query.py` with `--email-id` to fetch the full body directly.
```bash
python .agents/skills/ragmail/scripts/ragmail_query.py search \
  --workspace 2026 \
  --email-id bf509cfe2e3ca574 \
  --limit 1 \
  --fields date,from_name,from_address,subject,email_id,body_plain
```

Tip: If the query uniquely identifies the email, you can also ask `ragmail_query.py` to emit `body_plain` directly by adding it to `--fields`.

## Schema + indexing reference

See `references/db.md` for the current schema, FTS columns, workspace layout, and ingestion notes.

## Raw data + pipeline locations

When you need deeper context or to verify a record, dig in:
- Raw mbox: `private/gmail-*.mbox`
- Workspace outputs: `workspaces/<name>/clean`, `workspaces/<name>/spam`, `workspaces/<name>/split`, `workspaces/<name>/reports`, `workspaces/<name>/logs`

## When to go deeper

If search results are thin or you need more body context:
- Try `--table email_chunks` with the same query.
- Increase `--limit` or `--max-scan`.
- Use narrower date ranges.

## Attachments (opt-in only, slow)

Only fetch or scan attachments if the user explicitly asks. This is rare and expensive because it requires reading large raw MBOX files. You can suggest looking there when it’s likely helpful, but always warn that it will be slow and requires explicit confirmation.

Preferred flow:
1. Check metadata in LanceDB (`has_attachment`, `attachment_names`, `attachment_types`) to see if attachments exist.
2. If the user explicitly asks, use the attachment extractor to pull the attachment from the split MBOX.
3. Use the MBOX index file to avoid full scans. The pipeline creates it during the `clean` stage.

### Fast metadata check (no MBOX scan)
```bash
uv run python - <<'PY'
import lancedb

db = lancedb.connect("workspaces/2026/db/email_search.lancedb")
emails = db.open_table("emails")

email_id = "bf509cfe2e3ca574"
row = (
    emails.search()
    .where(f"email_id = '{email_id}'", prefilter=True)
    .limit(1)
    .to_list()
)
print(row[0]["has_attachment"], row[0]["attachment_names"], row[0]["attachment_types"])
PY
```

### Extract an attachment by Message-ID (optimized)
```bash
python .agents/skills/ragmail/scripts/ragmail_attachments.py \
  --workspace 2026 \
  --message-id "<abc123@example.com>" \
  --out-dir /tmp/attachments
```

### Extract by `email_id`
```bash
python .agents/skills/ragmail/scripts/ragmail_attachments.py \
  --workspace 2026 \
  --email-id bf509cfe2e3ca574 \
  --out-dir /tmp/attachments
```

### Index location (required)
The pipeline creates `workspaces/<name>/split/mbox_index.jsonl` (during the `clean` stage). Attachment extraction requires this index.
If it’s missing, ask the user to run `ragmail pipeline --stages index --workspace <name>`.

## Keep updated

If schemas, FTS columns, or workspace layouts change, update:
- `references/db.md`
- `scripts/ragmail_query.py`
