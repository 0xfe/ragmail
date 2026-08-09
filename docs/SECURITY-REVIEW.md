# Security Review: Data Leaks & Third-Party Exposure

**Date:** 2026-08-09
**Scope:** Full repository — Python pipeline, Rust workspace, agent skill scripts,
CI/release workflows, docs, and complete git history (all commits, including
deleted files).
**Out of scope (by request):** Deliberate, user-configured use of a cloud LLM is
not treated as a leak.

## Summary

RAGmail's core promise — "no data leaves your machine by default" — mostly
holds: the Rust code has zero network dependencies, the agent skill scripts are
purely local, test fixtures are synthetic, and no secrets exist anywhere in git
history. However, there are **two significant ways real email data can leak**
without the user intending it, plus several medium/low issues.

| # | Severity | Finding | Location |
|---|----------|---------|----------|
| 1 | High | Unauthenticated REST API binds `0.0.0.0` by default | `python/lib/ragmail/search_cli.py:414` |
| 2 | High | Ambient `OPENAI_API_KEY` silently sends email content to OpenAI | `python/lib/ragmail/llm/openai.py:27` |
| 3 | Medium | Automatic `trust_remote_code=True` for HF models | `python/lib/ragmail/embedding/sentence_transformer.py:47` |
| 4 | Medium | SQL injection into LanceDB queries, reachable via the API | `python/lib/ragmail/storage/repository.py:166` |
| 5 | Medium | Hugging Face telemetry / implicit downloads not disabled | `python/lib/ragmail/workspace.py:138` |
| 6 | Low | `.env` not in `.gitignore` despite being auto-loaded | `.gitignore`, `python/lib/ragmail/config.py:15` |
| 7 | Low | GitHub token embedded in clone URL / CLI argument | `just.d/scripts/publish-homebrew-tap.sh:71` |
| 8 | Low | Email fragments can end up in workspace logs | `rust/ragmail-cli/src/python_bridge.rs:296` |
| 9 | Low | README pipes remote install scripts to the shell | `README.md:42` |

---

## High severity

### 1. The REST API binds to `0.0.0.0` with no authentication

`python/lib/ragmail/search_cli.py:414`

`ragmail-py serve` defaults to `--host 0.0.0.0` and the FastAPI app
(`python/lib/ragmail/api/app.py`) has no auth, no token, and no CORS
restrictions. Anyone on the same network (coffee-shop Wi‑Fi, office LAN, or the
internet if the port is forwarded) can read the **entire email archive**:

- `POST/GET /search` — full-text and semantic search over all mail
- `GET /emails/{id}` — full message bodies, recipients, labels
- `GET /stats` — top contacts and volume by year

**Recommendation:** default to `127.0.0.1`; require a bearer token whenever
binding to a non-loopback interface.

### 2. An ambient `OPENAI_API_KEY` silently sends email content to OpenAI

`python/lib/ragmail/llm/openai.py:27`, `python/lib/ragmail/search_cli.py:157`

Deliberate cloud LLM use is out of scope, but this path is not deliberate:

- `ragmail query` has RAG **enabled by default** (`--rag/--no-rag`, default
  `True`), and the LLM query planner follows the same default.
- When `EMAIL_SEARCH_OPENAI_API_KEY` is unset, `openai.OpenAI(api_key=None)`
  falls back to the generic `OPENAI_API_KEY` environment variable — which many
  developers have exported for unrelated tools.

The result: a plain `ragmail query "invoice"` sends the query plus the top-10
retrieved emails (subject, sender, date, and the first 500 characters of each
body — `prompts.py:75`) to `api.openai.com`, even though the user never
configured RAGmail for cloud use. The README's privacy note ("what you share
depends entirely on the tools you configure") does not hold in this case.

**Recommendation:** only honor the explicit `EMAIL_SEARCH_OPENAI_API_KEY`
(pass a sentinel instead of `None` so the OpenAI SDK cannot fall back), or
require an explicit opt-in flag before any remote LLM call. At minimum, print
the target base URL before the first remote call of a session.

---

## Medium severity

### 3. Automatic `trust_remote_code=True` for Hugging Face models

`python/lib/ragmail/embedding/sentence_transformer.py:47-48`

Any model whose name contains the substring `"nomic"` gets
`trust_remote_code=True`, which downloads and **executes arbitrary Python**
from the Hugging Face Hub. The default model is pinned to a specific commit
(`e5cf08aa…`, good), but if a user sets `EMAIL_SEARCH_EMBEDDING_MODEL` to any
other nomic variant, the revision is unpinned (lines 42-45) — a compromised or
hijacked HF repo then runs code with full access to the mailbox on disk.

**Recommendation:** require explicit opt-in for remote code and pin revisions
for all models, not just the default.

### 4. SQL injection into LanceDB queries, reachable through the unauthenticated API

`python/lib/ragmail/storage/repository.py:166, 274`

`repository.get()` interpolates the ID directly:
`.where(f"email_id = '{email_id}'")`, and `email_id` comes straight from the
URL path in `GET /emails/{email_id}` (`api/app.py:159`). The same pattern
applies to `sender` at line 274. Impact is read-only against a local database
(and finding #1 already exposes everything), but values should be escaped the
way `query_planner.py` does with its `escape()` helper.

### 5. Hugging Face Hub telemetry and implicit downloads are not disabled

`python/lib/ragmail/workspace.py:138-161`

`apply_env()` sets cache paths and disables progress bars, but not
`HF_HUB_DISABLE_TELEMETRY=1`. Model loads can send telemetry (library
versions, model ID — no email content) to Hugging Face, and the vectorize
stage silently contacts `huggingface.co` on first run.

**Recommendation:** set `HF_HUB_DISABLE_TELEMETRY=1` by default and
document/support `HF_HUB_OFFLINE=1` once the model is cached.

---

## Low severity

### 6. `.env` is not in `.gitignore`

Pydantic settings is configured to load `.env`
(`python/lib/ragmail/config.py:15`) — the natural place users will put
`EMAIL_SEARCH_OPENAI_API_KEY` — but `.gitignore` does not exclude it, so the
key is one `git add -A` away from being committed. The default `./attachments`
directory is also unignored.

### 7. GitHub token embedded in the clone URL / CLI argument

`just.d/scripts/publish-homebrew-tap.sh:71`

The Homebrew tap token is passed as a CLI argument (visible in `ps` on the
runner) and baked into `https://x-access-token:${token}@github.com/…`, which
lands in the temp clone's `.git/config`. GitHub Actions masks secrets in logs
and the `trap` removes the temp dir, so exposure is narrow — but prefer
`GIT_ASKPASS` or an `Authorization` header via `http.extraHeader`.

### 8. Email fragments can end up in workspace logs

`rust/ragmail-cli/src/python_bridge.rs:296-298`

Any non-JSON stderr/stdout line from the Python bridge (e.g., a traceback
containing a malformed message) is appended to
`workspaces/<ws>/logs/<stage>.log`, and the full query text appears in the
logged command line. Local-only, but the logs directory outlives the run and
gets copied around in the remote-GPU workflow described in the README.

### 9. README pipes remote install scripts to the shell

The rustup instruction pipes `curl` output to `sh` (standard practice, but
worth offering a checksum-verified alternative for a privacy-oriented
audience). The uv line pipes to `less` — likely a typo, which also means the
documented command never installs anything.

---

## What checked out clean

- **Rust workspace:** no `reqwest` or other network crates — the
  split/preprocess stages provably cannot phone home.
- **Agent skill scripts** (`.agents/skills/ragmail/scripts/`): pure local
  LanceDB/mbox reads; no network imports.
- **Test fixtures and docs:** all synthetic (`example.com`) — no real personal
  email data committed.
- **Git history:** scanned every commit for API-key/private-key patterns —
  nothing found; deleted files were only `.pyc` artifacts and internal docs.
- **CI workflows:** no `pull_request_target` footguns; release secrets are
  properly gated and scoped.
