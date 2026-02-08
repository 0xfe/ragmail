# Agent Guidelines for ragmail

This document provides guidance for AI agents working on email processing tasks in this repo.

## Project Structure

```
ragmail/
├── python/               # Python project (package + deps)
│   ├── lib/
│   │   └── ragmail/      # Python orchestration + vectorize/ingest/search interfaces
│   ├── tests/            # Python test suite
│   ├── pyproject.toml
│   └── uv.lock
├── rust/                 # Rust workspace (CLI + split/preprocess/core crates)
├── just.d/               # Build/test/release helper scripts
├── docs/                 # Documentation
├── private/              # Private email data (gitignored)
│   └── gmail-*.mbox
├── workspaces/           # Workspace runs (gitignored)
├── README.md             # Repo overview
├── ai-state.md           # Dense AI-only repo state (keep updated)
└── AGENTS.md             # This file
```

## AI State File (Required)

This repo maintains `ai-state.md` as a compact, AI-focused briefing with the current layout, commands, and design invariants.
Update it whenever you change pipeline stages, data layout, indexing, embeddings, or any CLI/workspace behavior.

## Engineering Best Practices

- **Code style**: Match existing patterns; avoid mass reformatting. Prefer small, testable functions, explicit types, and `Path` for filesystem paths. Keep streaming primitives for large files.
- **Robustness**: Preserve checkpointing, resume support, and time-based progress updates. Never load whole MBOX files into memory. Favor idempotent stages that can resume or rerun safely.
- **UX**: Keep CLI output actionable and consistent. For slow operations, surface progress, ETA, and clear remediation steps on failure. Defaults should be safe and predictable.
- **Testing**: Prefer `just test-python` (venv-first, `uv` fallback) and `cargo test --manifest-path rust/Cargo.toml --workspace`. Keep integration tests isolated behind markers and opt-in env flags.
- **Docs**: Update `README.md`, `docs/`, `.agents/skills/`, and `ai-state.md` when changing user-facing behavior, schema, or workspace layout.

## Agent Skills (Codex/Claude)

Agent skills are a first-class, supported way to query the LanceDB workspaces. This repo includes the `ragmail` skill in `.agents/skills/ragmail` with a scripted query helper. Keep the skill updated if schemas, indexing, or workspace layout change.

When using Codex or Claude to dig through email:
- Ask natural language questions; then use the skill's script to execute precise queries.
- Always translate relative dates ("last summer") into explicit ranges.
- Prefer `emails` for high-level questions; use `email_chunks` for deeper body matches.
- Provide the workspace name (e.g., `2026`) or the full db path.

More info: https://developers.openai.com/codex/skills/

## Principles for Robust Data Cleaning

### 1. Sample Across the Full Distribution

Email formats, headers, and content change significantly over time:

- **2004-2006**: Simple plain text, minimal headers, early Gmail
- **2010-2015**: More HTML, attachments, multipart messages
- **2020-present**: Heavy authentication headers (DKIM, SPF, ARC), marketing automation

**Always test with samples from multiple years:**
```bash
UV_PROJECT_ENVIRONMENT=$PWD/.venv uv run --project python python -m ragmail.sample.sampler private/gmail-*.mbox --distributed --emails-per-file 200 -o test-sample.mbox
```

### 2. Stream Processing for Large Files

The dataset is large. Never load entire files into memory:

- Use line-by-line or email-by-email streaming
- Process and write immediately
- Flush output periodically
- Track position for checkpointing

### 3. Checkpoint and Resume Support

Long-running processing jobs should be resumable:

- Save checkpoint every 30 seconds
- Store: file position, statistics, timestamp
- On resume: seek to position, sync to email boundary
- Remove checkpoint on successful completion

### 4. Progress Reporting

For large datasets, users need visibility:

- Display progress percentage and ETA
- Show current item being processed
- Track and display key statistics
- Update display every 250ms (not every item)

### 5. Graceful Error Handling

Email data is messy. Handle errors gracefully:

- Malformed headers: decode with fallback encodings
- Invalid dates: use heuristics, log errors
- Encoding issues: try charset detection, fall back to latin-1
- Parse failures: skip email, log, continue

### 6. Preserve RAG-Critical Information

For RAG (Retrieval-Augmented Generation), keep:

**Critical (always keep):**
- From, To, Cc, Date, Subject
- Message-ID, In-Reply-To, References (threading)
- X-GM-THRID (Gmail thread ID)
- X-Gmail-Labels (categorization)

**Remove (noise for RAG):**
- Authentication headers (DKIM, SPF, ARC, etc.)
- Routing headers (Received, Return-Path)
- Service-specific headers (X-Pobox-*, X-ME-*, etc.)

### 7. Attachment Handling

Attachments are not useful for RAG but metadata is:

- Remove binary content
- Preserve metadata: filename, type, size
- Preserve metadata in JSON `attachments` array

### 8. Spam/Newsletter Detection

Filter low-value content:

**Label-based:**
- Gmail Spam/Trash labels
- Category Promotions (marketing)

**Header-based:**
- `Precedence: bulk` (mass mailings)
- Marketing X-Mailer values (Mailchimp, etc.)

**Keep mailing lists** - they often have valuable technical content (identified by List-ID header).

## Running the Pipeline

```bash
# One-step bootstrap (Python env + Rust build + .venv/bin/ragmail link)
just bootstrap
source .venv/bin/activate

# Create a distributed sample
UV_PROJECT_ENVIRONMENT=$PWD/.venv uv run --project python python -m ragmail.sample.sampler private/gmail-*.mbox --distributed --emails-per-file 100 -o test-sample.mbox

# Full pipeline in a workspace
ragmail pipeline test-sample.mbox --workspace test-sample

# Search within workspace
ragmail search "meeting tomorrow" --workspace test-sample
```

Rust quality gates:
```bash
cargo fmt --manifest-path rust/Cargo.toml --all -- --check
cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets -- -D warnings
cargo test --manifest-path rust/Cargo.toml --workspace
```

Tip:
- Use `ragmail pipeline --refresh` to rerun selected stages from scratch (archives outputs and clears checkpoints).

## Output Files

For input `gmail-2015.mbox`:
- `gmail-2015.clean.jsonl` - Cleaned emails ready for RAG
- `gmail-2015.spam.jsonl` - Filtered spam/newsletters (for review)
- `gmail-2015.mbox.summary` - Processing statistics and metadata

## Shared Library Usage

```python
from ragmail.common.terminal import Colors, Glyphs, ProgressDisplay, format_bytes
from ragmail.common.checkpoint import Checkpoint
```

## Testing Guidelines

1. **Always test on samples first** - never run untested code on multi-GB files
2. **Use distributed sampling** - `--distributed` flag samples from all years
3. **Verify output format** - spot-check clean.jsonl files
4. **Check edge cases**:
   - HTML-only emails
   - Emails with many attachments
   - Non-ASCII encodings
   - Malformed headers

## Common Issues

### "From " in email body
MBOX escapes "From " at line start with ">From ". Our parser only matches "From " lines with valid date patterns.

### Encoding errors
Always decode with `errors='replace'`. Try charset from Content-Type first, then chardet, then latin-1.

### Memory issues on large files
Use streaming Rust-backed stage commands (`split`, `preprocess`); avoid any mailbox APIs that materialize entire files.

### Progress display flicker
Only render every 250ms, not every email. Use time-based throttling.
