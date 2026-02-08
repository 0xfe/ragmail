# RAGmail

RAGmail lets you search and analyze your email with your favourite agent (OpenCode, Claude, Codex, etc.)

It consists of a comprehensive ingestion pipeline to build a semantically-indexed local database of your email, along with an agent skill (`ragmail`) that you can use to ask questions.

Typical questions you can answer:

- "_What did we decide about the school trip budget?_"
- "_Tell me how my communication style has changed over time, with examples._"
- "_Where all did I travel to in 2006?_"
- "_Explore my relationship with Alice and write me a doc on how it progressed, start to finish._"
- "_How many times did Bob email me in February 2026?_"

As you use this more and more, you'll find that you can uncover some **really interesting insights** from your email. I have about 22 years of e-mail in Gmail, and another 10 years of it in my archives. I built this tool so I could do some local analysis on my email.

See the [privacy note](#privacy-note) below for how your data is handled.

## How it works

After downloading your mail, you call `ragmail pipeline` to start the email ingestion pipeline.

This pipeline tirelessly processes your gigantic mailboxes, cleaning them up, and building a local database indexed for both full-text and semantic search. This database can now be queried and analyzed by your favourite agent.

For more details, see the [pipeline](docs/pipeline.md) document or the [design doc](docs/DESIGN.md).

## Quickstart

### Download your mailbox

`RAGmail` works with standard `.MBOX` files. You can download your entire Gmail mailbox
with [Google Takeout](https://takeout.google.com/).

### Download and Build `RAGmail`

```bash
# Install just
brew install just  # on mac
apt install just   # on ubuntu/debain linux

# Install uv and rust
curl -LsSf https://astral.sh/uv/install.sh | less
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Make sure all your env vars (like PATH) are set, and start a new shell

# Clone this repo
git clone <this repo>
cd ragmail

# Build python and rust code
just bootstrap

# Activate the ragmail venv
source .venv/bin/activate
```

### Option 1: Run pipeline locally

This is the simplest way to get started, but it will be slow without a large GPU. For a 15GB mailbox (about 200k messages) it will take **about 25 hours** on a Macbook Air M3.

```bash
# Run full pipeline (split,preprocess,vectorize,ingest)
ragmail pipeline ~/private/all-emails.mbox --workspace my-mail
```

When this is complete, you can use your favorite agent to ask questions about your email.

### Option 2: Run pipeline locally, offload embedding to remote GPU

This approach is much faster (10 - 100x) if you have a large mailbox. For example, if you spin up an L4 instance on GCP for a 15GB mailbox (about 200k messages) it will take **about an hour and cost less than a dollar**.

```bash
# Local: run the initial stages only
ragmail pipeline ~/private/all-emails.mbox --workspace my-mail --stages split,preprocess

# Local: zip up the preprocessed mail and send to remote host
tar -czvf my-mail-clean.tar.gz workspaces/my-mail/clean
scp my-mail-clean.tar.gz user@host:/tmp

# Remote: make sure ragmail is installed and you have a venv. Then unpack the tarball,
# and run the vectorize stage to create the embeddings, and package them up again.

mkdir -p ~/tmp/ragmail && cd ~/tmp/ragmail
tar -xzf /tmp/my-mail-clean.tar.gz

# Remote: make sure the right python venv is activated before running the pipeline
ragmail pipeline --workspace my-mail --stages vectorize
tar -czvf /tmp/my-mail-embeddings.tar.gz workspaces/my-mail/embeddings

# Local: fetch the embeddings and unpack them
scp user@host:/tmp/my-mail-embeddings.tar.gz .
tar -xvzf my-mail-embeddings.tar.gz -C workspaces/my-mail

# Local: ingest the embeddings (make sure your venv is activated)
ragmail pipeline --workspace my-mail --stages ingest
```

## Analyzing your email

The simplest way to analyze your email is with a coding agent, e.g. Claude, Codex, etc. This repository comes with an agent skill called `ragmail` that can be used to search your email.

```bash
$ claude

Claude is ready. Type your questions below.

> use the ragmail skill in workspace my-mail
... skill ragmail is now available
> ragmail "tell me about the school trip budget for 2026"
... <results>


$ codex

Codex is ready. Type your questions below.

> $ragmail use the workspace my-mail
... skill ragmail is now available, looking for workspace
> ragmail "tell me about the school trip budget for 2026"
... <results>
```

## Prerequisites

Required:
- `python` 3.11+
- Rust toolchain 1.93.0+ (`rustc`, `cargo`, `rustfmt`, `clippy`)
- `uv` for Python dependency and environment management
- `just` for common build/test/release commands

Optional for release maintainers:

- `dpkg-deb` for Linux `.deb` packaging

## Privacy Note

You have complete control over your data. During the pipline stage, no data leaves your machine by default. All vector embeddings are calculated and stored locally.

When using the agent (or with `ragmail query`), what you share depends entirely on the tools and modles you use. For example, if you use OpenAI's Codex, you're sharing your queries and all relevant context with OpenAI. If you use OpenCode with a local LLM, you're not sharing anything.

## Usage Instructions

Run `ragmail --help` for Rust-native commands.
Use `ragmail-py --help` to see the full passthrough command list (`query`, `stats`, `serve`, etc.).

Common commands:
```bash
# Run full pipeline
ragmail pipeline private/gmail-2015.mbox --workspace my-mail

# Run specific stages
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --stages split,preprocess
ragmail pipeline --workspace my-mail --stages vectorize
ragmail pipeline --workspace my-mail --stages ingest

# Resume is enabled by default
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --resume

# Re-run selected stages from scratch (archives old outputs)
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --stages preprocess --refresh

# Query
ragmail query "invoice" --workspace my-mail

# Query with default RAG answer generation
ragmail query "what did we decide about the budget" --workspace my-mail

# Retrieval-only query (disable RAG/planner)
ragmail query "what did we decide about the budget" --workspace my-mail --no-rag

# Show full raw message bytes by id
ragmail message --workspace my-mail --email-id <email_id>

# Workspace utilities
ragmail workspace init my-mail
ragmail workspace info my-mail
```

Useful pipeline flags:
- `--ingest-batch-size`: write batch size for DB inserts.
- `--embedding-batch-size`: batch size sent to embedding model.
- `--chunk-size` and `--chunk-overlap`: control chunk granularity.
- `--skip-exists-check`: faster ingest when safe.
- `--checkpoint-interval`: checkpoint frequency.
- `--compact-every`: periodic DB compaction during ingest.

## About Workspaces

Each workspace is an isolated processing run. Use one workspace per email dataset or experiment.

Default layout:

```text
workspaces/<name>/
├── inputs/                 # linked input mbox files
├── split/                  # monthly mbox files + mbox_index.jsonl
├── clean/                  # cleaned jsonl
├── spam/                   # filtered bulk/spam jsonl
├── reports/                # per-file summary reports
├── embeddings/             # embedding stores (*.embed.db)
├── db/
│   └── email_search.lancedb
├── logs/                   # stage logs
├── .checkpoints/           # resume checkpoints
├── workspace.json          # workspace config/paths
└── state.json              # stage state + durations
```

You can set a different root with `--base-dir`.

## Cost / Performance / Privacy Tradeoffs

### What runs locally by default

By default, all pipeline stages run on your machine.

- `model`, `split`, `preprocess`: local processing (`split`/`preprocess` are Rust-backed).
- `vectorize`: local embedding inference.
- `ingest`: local LanceDB writes.
- `query`: local retrieval (RAG-enabled by default unless `--no-rag`).

No email data must leave your machine for these steps.

### What can call external models

LLM-assisted features (for example `ragmail query`) can call an OpenAI-compatible API.

You control this with environment variables:

```bash
# Use a trusted hosted provider (default style)
export EMAIL_SEARCH_OPENAI_BASE_URL="https://api.openai.com/v1"
export EMAIL_SEARCH_OPENAI_API_KEY="<key>"
export EMAIL_SEARCH_OPENAI_MODEL="gpt-5.2"
```

You can also point to a local OpenAI-compatible server, such as Ollama or vLLM:

```bash
# Example: local OpenAI-compatible endpoint
export EMAIL_SEARCH_OPENAI_BASE_URL="http://localhost:11434/v1"
export EMAIL_SEARCH_OPENAI_API_KEY="dummy"
export EMAIL_SEARCH_OPENAI_MODEL="llama3.1"
```

If you're using a coding agent, messages that are part of the conversation will be sent to the coding agent's LLM. Make sure you trust the coding agent's LLM with your email data.

You can keep everything local with a combination of OpenCode and vLLM/Ollama running your favourite reasoning model.

## More Docs
- Pipeline deep dive: [`docs/pipeline.md`](docs/pipeline.md)
- High-level design: [`docs/DESIGN.md`](docs/DESIGN.md)
- Developer guide: [`docs/developers.md`](docs/developers.md)
- Release process: [`docs/release.md`](docs/release.md)
- Usage examples: [`docs/examples.md`](docs/examples.md)
- Prompt details: [`docs/prompts.md`](docs/prompts.md)

## MIT License

Copyright (c) 2026 Mohit Cheppudira <shhh@mo.town>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
