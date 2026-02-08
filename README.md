# RAGmail

## About

I have about 22 years of e-mail in Gmail, and another 10 years of it my archives. I built this tool
so I could do some local analysis on my email.

RAGmail lets you search and analyze your email with your favourite agent (Claude, Codex, etc.)

Typical questions you can answer:

- "What did we decide about the school trip budget?"
- "Where all did I travel to in 2006?"
- "How many times did Bob email me in February 2026?"

## How it works

RAGmail performs all the heavy lifting around processing gigantic mail boxes, cleaning it up, and building a database indexed for both full-text and semantic search.

After downloading your mail (say, from Google Takeout), you can run `ragmail pipeline` to process it. Processing is broken down into stages, and can be resumed if it is interrupted.

The stages are:

1. `split`: Split the mbox file into smaller mbox files, one for each month.
2. `preprocess`: Clean and normalize the mbox files, e.g., remove bulk mail, attachments, signatures, etc.
3. `vectorize`: Generate embedding vectors for the subject and body of each message.
4. `ingest`: Ingest the embeddings, text, and metadata into a vector database.

## Quickstart

### Download your mailbox

`RAGmail` works with standard `.MBOX` files. You can download your entire Gmail mailbox
with [Google Takeout](https://takeout.google.com/).

### Download and Build `RAGmail`

```bash
# Clone this repo
git clone <this repo>
cd ragmail

# Build python and rust code
just bootstrap

# Activate the venv
source .venv/bin/activate
```

### Run the pipeline on your local machine

This is the simplest way to get started, but it will be slow if you have a large mailbox.

```bash
# Run full pipeline (model,split,preprocess,vectorize,ingest)
ragmail pipeline private/gmail-2015.mbox --workspace my-mail

# Search your workspace
ragmail search "meeting tomorrow" --workspace my-mail
```

### Run the pipeline on a remote GPU

This approach is much faster (10 - 100x) if you have a large mailbox. Here you run the compute-intensive parts of the pipeline on a remote GPU.

For example, you can quickly spin up an L4 instance on GCP for this purpose, which for a 15GB mailbox (about 200k messages) would take less than 30 minutes and cost about $3.

```bash
# Local: run the initial stages only
ragmail pipeline private/gmail-2015.mbox --workspace my-mail --stages split,preprocess

# Local: zip up the preprocessed mail and send to remote host
tar -czf my-mail-clean.tar.gz workspaces/my-mail/clean
rsync -av my-mail-clean.tar.gz user@host:/tmp

# On the remote host, make sure ragmail is installed and you have a venv. Then unpack the tarball,
# and run the vectorize stage to create the embeddings, and package them up again.

mkdir -p ~/tmp/ragmail && cd ~/tmp/ragmail
tar -xzf /tmp/my-mail-clean.tar.gz
ragmail pipeline --workspace my-mail --stages vectorize
tar -czf /tmp/my-mail-embeddings.tar.gz workspaces/my-mail/embeddings

# Back on the local machine, fetch the embeddings and unpack them
rsync -av user@host:/tmp/my-mail-embeddings.tar.gz .
tar -xzf my-mail-embeddings.tar.gz -C workspaces/my-mail

# Local: ingest the embeddings
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
```

## Prerequisites

Required:
- `python` 3.11+
- Rust toolchain 1.93.0+ (`rustc`, `cargo`, `rustfmt`, `clippy`)

Recommended:
- `uv` for Python dependency and environment management
- `just` for common build/test/release commands

Optional for release maintainers:

- `dpkg-deb` for Linux `.deb` packaging

## Usage Instructions

Run `ragmail --help` for the full command list.

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

# Search
ragmail search "invoice" --workspace my-mail

# Search with RAG answer generation
ragmail search "what did we decide about the budget" --workspace my-mail --rag

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
- `search`: local retrieval.

No email data must leave your machine for these steps.

### What can call external models

LLM-assisted features (for example `ragmail search --rag`) can call an OpenAI-compatible API.

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
