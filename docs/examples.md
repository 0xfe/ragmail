# Usage Examples

This document shows practical, current examples for searching and analyzing mail with ragmail.

## Quick Start

```bash
# 1) Build a workspace DB first (if you have not already)
ragmail pipeline private/gmail-2025.mbox --workspace my-mail

# 2) Run search against that workspace
ragmail query --workspace my-mail "meeting" --limit 5
```

Notes:
- `ragmail --help` only shows Rust-native commands.
- Commands like `query`, `stats`, `serve`, and `message` are Python passthrough commands and still work.

## Search Basics

```bash
# Keyword / semantic retrieval
ragmail query --workspace my-mail "project deadline" --limit 10

# Narrow by sender/date in natural language
ragmail query --workspace my-mail "emails from sarah in 2024 about budget" --limit 20

# Ask for a count
ragmail query --workspace my-mail "how many emails in 2025"

# Ask for top contacts
ragmail query --workspace my-mail "who did I email most in 2025"
```

If you prefer explicit DB path instead of workspace resolution:

```bash
ragmail query --db workspaces/my-mail/db/email_search.lancedb "meeting" --limit 5
```

## RAG Answers and Query Planning

```bash
# Query uses RAG by default (requires OpenAI-compatible settings)
export EMAIL_SEARCH_OPENAI_API_KEY="sk-..."
ragmail query --workspace my-mail "what did John say about the deadline?"

# Disable RAG/planner for retrieval-only output
ragmail query --workspace my-mail --no-rag "what did John say about the deadline?"

# Show planner behavior without RAG answer
ragmail query --workspace my-mail --no-rag --plan "emails from legal in January 2026 with attachments"
```

Planner notes:
- `--rag` is enabled by default for `ragmail query`.
- `--no-rag` disables RAG answer generation and planner unless `--plan` is set explicitly.
- `--plan` enables the LLM query planner.
- RAG and planner both require OpenAI-compatible backend settings.

## Stats, Dedupe, and Message Retrieval

```bash
# Database stats
ragmail stats --workspace my-mail

# Include duplicate ID analysis
ragmail stats --workspace my-mail --dupes

# Remove duplicates (dry run first)
ragmail dedupe --workspace my-mail --dry-run
ragmail dedupe --workspace my-mail --table both

# Fetch full raw MIME message by email_id
ragmail message --workspace my-mail --email-id <email_id> --output /tmp/email.eml
```

## API Examples

```bash
# Start API server
ragmail serve --workspace my-mail --host 127.0.0.1 --port 8000
```

```bash
# POST search
curl -X POST http://127.0.0.1:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query":"meeting tomorrow", "limit": 5}'

# GET search
curl "http://127.0.0.1:8000/search?q=meeting%20tomorrow&limit=5"

# Stats
curl http://127.0.0.1:8000/stats

# Full email
curl http://127.0.0.1:8000/emails/<email_id>

# Similar emails
curl "http://127.0.0.1:8000/emails/<email_id>/similar?limit=5"
```

## Advanced Python Querying

For deeper analysis (custom filters, joins, timelines), query LanceDB directly:

```bash
UV_PROJECT_ENVIRONMENT=$PWD/.venv uv run --project python python - <<'PY'
import lancedb

db = lancedb.connect("workspaces/my-mail/db/email_search.lancedb")
emails = db.open_table("emails")

rows = (
    emails.search("invoice", query_type="fts")
    .where("year = 2025", prefilter=True)
    .limit(20)
    .to_list()
)

for r in rows:
    print(r["date"], r["from_address"], r["subject"])
PY
```

## Using ragmail With a Coding Agent

This is the workflow for questions like the ones you asked earlier (counts, timelines, communication-style analysis).

### 1) Set workspace context clearly

Good prompt examples:

```text
Use workspace mo5. Find all emails mentioning unsubscribe and list counts by year.
```

```text
Use workspace mo5 and analyze how my communication style changed from 2004 to 2025, with examples.
```

### 2) Ask for concrete output format

```text
Use workspace mo5. Give me:
1) total count
2) year-by-year table
3) 10 example emails with date, sender, subject
```

### 3) Prefer explicit dates over relative dates

```text
Use workspace mo5. Analyze messages between 2024-01-01 and 2024-12-31 about school registration.
```

### 4) Ask for evidence-backed conclusions

```text
Use workspace mo5. Summarize how tone changed over time and include direct snippets for each phase.
```

### 5) Ask for reproducibility when needed

```text
Use workspace mo5. Show the exact query/filters you used so I can rerun it.
```

## Common Patterns

```bash
# Person + topic + year
ragmail query --workspace my-mail "emails from alex in 2023 about taxes"

# Conversation summary via RAG
ragmail query --workspace my-mail "summarize my discussion with legal about contract renewal"

# Decision extraction
ragmail query --workspace my-mail "what was decided about remote work policy?"
```

## Troubleshooting

```bash
# No DB found
ragmail query --workspace my-mail "meeting"
```

If this fails, check:
- workspace exists: `ragmail workspace info my-mail`
- DB exists: `workspaces/my-mail/db/email_search.lancedb`
- ingest completed: `cat workspaces/my-mail/state.json`

```bash
# RAG enabled but no key
ragmail query --workspace my-mail "what did John say?"
```

Set:

```bash
export EMAIL_SEARCH_OPENAI_API_KEY="sk-..."
export EMAIL_SEARCH_OPENAI_MODEL="gpt-4o-mini"
```
